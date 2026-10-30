// Package client is the Go client for the KV service.
//
// It handles the two things every correct Raft client must: follow leader
// redirects, and retry with a *stable* sequence number.
//
// The second is the subtle one. A sequence number is allocated once per logical
// operation and reused across every attempt, so the server's session table can
// recognise a retry and return the original answer instead of applying the
// command twice. Allocating a fresh sequence number per attempt would make each
// retry a distinct command — which is exactly the duplicate-application bug the
// session table exists to prevent.
package client

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/MiNam8/kvstore/proto/kvstorepb"
)

// Client talks to a cluster.
type Client struct {
	addrs  []string
	conns  []*grpc.ClientConn
	stubs  []kvstorepb.KVClient
	nodeID []uint64

	clientID uint64

	mu     sync.Mutex
	seq    uint64
	leader int // index of the node last known to be leader
}

// New connects to every node in the cluster.
//
// The client id is random rather than assigned. A collision would let two
// clients share a session and see each other's cached results, so it is drawn
// from the full 64-bit space where collision is not a practical concern; a
// production system would have the cluster assign these.
func New(addrs []string) (*Client, error) {
	if len(addrs) == 0 {
		return nil, errors.New("client: no addresses")
	}
	c := &Client{addrs: addrs, clientID: rand.Uint64() | 1}
	for _, a := range addrs {
		conn, err := grpc.NewClient(a, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			c.Close()
			return nil, err
		}
		c.conns = append(c.conns, conn)
		c.stubs = append(c.stubs, kvstorepb.NewKVClient(conn))
	}
	return c, nil
}

// Close releases every connection.
func (c *Client) Close() error {
	for _, conn := range c.conns {
		_ = conn.Close()
	}
	return nil
}

// ID returns this client's session id.
func (c *Client) ID() uint64 { return c.clientID }

// nextSession allocates a sequence number for one logical operation. It is used
// for every attempt at that operation, never re-allocated on retry.
func (c *Client) nextSession() *kvstorepb.Session {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seq++
	return &kvstorepb.Session{ClientId: c.clientID, Seq: c.seq}
}

func (c *Client) startAt() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.leader
}

func (c *Client) noteLeader(i int) {
	c.mu.Lock()
	c.leader = i
	c.mu.Unlock()
}

// call issues one logical operation, retrying across nodes until ctx expires.
//
// The same request value — and therefore the same sequence number — is used for
// every attempt.
func (c *Client) call(ctx context.Context, do func(ctx context.Context, stub kvstorepb.KVClient) (*kvstorepb.Response, error)) (*kvstorepb.Response, error) {
	return c.callFrom(ctx, -1, do)
}

// callFrom is call, but beginning the search at a specific node. A negative
// index means "start wherever the leader was last seen".
func (c *Client) callFrom(ctx context.Context, from int, do func(ctx context.Context, stub kvstorepb.KVClient) (*kvstorepb.Response, error)) (*kvstorepb.Response, error) {
	var lastErr error
	start := c.startAt()
	if from >= 0 {
		start = from % len(c.stubs)
	}

	for attempt := 0; ctx.Err() == nil; attempt++ {
		i := (start + attempt) % len(c.stubs)

		reqCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		resp, err := do(reqCtx, c.stubs[i])
		cancel()

		switch {
		case err != nil:
			lastErr = err
		case resp.NotLeader:
			lastErr = fmt.Errorf("node %d is not the leader", i)
			// Follow the hint if it names a node we know, rather than walking
			// the ring one node at a time.
			if resp.LeaderHint != 0 && int(resp.LeaderHint) <= len(c.stubs) {
				start = int(resp.LeaderHint) - 1
				attempt = -1
			}
		default:
			c.noteLeader(i)
			return resp, nil
		}

		if attempt > 0 && attempt%len(c.stubs) == 0 {
			// A full lap with no leader usually means an election is in
			// progress; give it time rather than spinning.
			select {
			case <-time.After(50 * time.Millisecond):
			case <-ctx.Done():
			}
		}
	}
	if lastErr == nil {
		lastErr = ctx.Err()
	}
	return nil, lastErr
}

// Put sets a key.
func (c *Client) Put(ctx context.Context, key string, value []byte) error {
	req := &kvstorepb.PutRequest{Key: key, Value: value, Session: c.nextSession()}
	_, err := c.call(ctx, func(ctx context.Context, s kvstorepb.KVClient) (*kvstorepb.Response, error) {
		return s.Put(ctx, req)
	})
	return err
}

// Get reads a key linearizably.
func (c *Client) Get(ctx context.Context, key string) ([]byte, bool, error) {
	req := &kvstorepb.GetRequest{Key: key, Session: c.nextSession()}
	resp, err := c.call(ctx, func(ctx context.Context, s kvstorepb.KVClient) (*kvstorepb.Response, error) {
		return s.Get(ctx, req)
	})
	if err != nil {
		return nil, false, err
	}
	return resp.Value, resp.Found, nil
}

// GetFrom reads a key, contacting node i first.
//
// It models the "read from the nearest replica" pattern: the request goes to a
// specific node, and a correct server redirects it to the leader. A server that
// answers locally instead returns whatever that replica happens to hold, which
// is precisely the stale read this project's Phase 9 exists to make fast
// *without* making it wrong.
func (c *Client) GetFrom(ctx context.Context, i int, key string) ([]byte, bool, error) {
	req := &kvstorepb.GetRequest{Key: key, Session: c.nextSession()}
	resp, err := c.callFrom(ctx, i, func(ctx context.Context, s kvstorepb.KVClient) (*kvstorepb.Response, error) {
		return s.Get(ctx, req)
	})
	if err != nil {
		return nil, false, err
	}
	return resp.Value, resp.Found, nil
}

// Delete removes a key, reporting whether it existed.
func (c *Client) Delete(ctx context.Context, key string) (bool, error) {
	req := &kvstorepb.DeleteRequest{Key: key, Session: c.nextSession()}
	resp, err := c.call(ctx, func(ctx context.Context, s kvstorepb.KVClient) (*kvstorepb.Response, error) {
		return s.Delete(ctx, req)
	})
	if err != nil {
		return false, err
	}
	return resp.Found, nil
}

// CompareAndSwap sets key to value only if it currently holds expected. Pass
// hasExpected=false to require the key to be absent.
func (c *Client) CompareAndSwap(ctx context.Context, key string, expected, value []byte, hasExpected bool) (bool, error) {
	req := &kvstorepb.CASRequest{
		Key: key, Value: value, Expected: expected,
		HasExpected: hasExpected, Session: c.nextSession(),
	}
	resp, err := c.call(ctx, func(ctx context.Context, s kvstorepb.KVClient) (*kvstorepb.Response, error) {
		return s.CompareAndSwap(ctx, req)
	})
	if err != nil {
		return false, err
	}
	return resp.Ok, nil
}

// Status queries one node directly, without redirecting.
func (c *Client) Status(ctx context.Context, i int) (*kvstorepb.StatusResponse, error) {
	if i < 0 || i >= len(c.stubs) {
		return nil, errors.New("client: node index out of range")
	}
	return c.stubs[i].Status(ctx, &kvstorepb.StatusRequest{})
}

// Len returns the number of nodes configured.
func (c *Client) Len() int { return len(c.stubs) }

// Addr returns the address of node i.
func (c *Client) Addr(i int) string { return c.addrs[i] }
