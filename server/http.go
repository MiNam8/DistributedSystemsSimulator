package server

import (
	"encoding/json"
	"errors"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"

	"github.com/MiNam8/kvstore/kv"
	"github.com/MiNam8/kvstore/raft"
)

// HTTPHandler is a thin JSON/REST shim over a node, for demos and curl.
//
// # The session caveat, stated honestly
//
// At-most-once execution needs a stable (client, sequence) pair across retries,
// and a bare curl has no way to carry one. So this shim accepts ?client= and
// ?seq= when the caller can supply them, and otherwise mints a fresh random
// client id per request. That is safe for a single request — a fresh session can
// never collide with an earlier one — but it means an HTTP client that retries
// on its own gets no deduplication. Use the gRPC client for anything that
// matters; this exists so the cluster can be poked with curl.
func (n *Node) HTTPHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /kv/{key}", n.httpGet)
	mux.HandleFunc("PUT /kv/{key}", n.httpPut)
	mux.HandleFunc("DELETE /kv/{key}", n.httpDelete)
	mux.HandleFunc("GET /status", n.httpStatus)
	mux.HandleFunc("GET /metrics", n.httpMetrics)
	return mux
}

// httpSession extracts a session from the query string, or mints a single-use one.
func httpSession(r *http.Request) (clientID, seq uint64) {
	q := r.URL.Query()
	clientID, _ = strconv.ParseUint(q.Get("client"), 10, 64)
	seq, _ = strconv.ParseUint(q.Get("seq"), 10, 64)
	if clientID == 0 || seq == 0 {
		return rand.Uint64() | 1, 1
	}
	return clientID, seq
}

// writeResult renders a result, translating a not-leader rejection into a 307
// so that even curl -L follows it.
func (n *Node) writeResult(w http.ResponseWriter, r *http.Request, res kv.Result, err error) {
	if err != nil {
		var nl NotLeaderError
		if errors.As(err, &nl) {
			w.Header().Set("X-Raft-Leader", strconv.FormatUint(uint64(nl.Leader), 10))
			// 307 preserves the method and body, unlike 302 — a redirected PUT
			// must stay a PUT or the write silently becomes a GET.
			if addr, ok := n.cfg.HTTPPeers[nl.Leader]; ok && nl.Leader != raft.None {
				target := *r.URL
				target.Scheme, target.Host = "http", addr
				http.Redirect(w, r, target.String(), http.StatusTemporaryRedirect)
				return
			}
			http.Error(w, nl.Error(), http.StatusMisdirectedRequest)
			return
		}
		// A timeout is indeterminate, not failed: the write may still commit.
		// 503 tells the caller to retry rather than to give up.
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok":        res.OK,
		"found":     res.Found,
		"value":     string(res.Value),
		"duplicate": res.Duplicate,
	})
}

func (n *Node) httpGet(w http.ResponseWriter, r *http.Request) {
	id, seq := httpSession(r)
	res, err := n.Read(r.Context(), r.PathValue("key"), id, seq)
	if err == nil && !res.Found {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	n.writeResult(w, r, res, err)
}

func (n *Node) httpPut(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	id, seq := httpSession(r)
	res, err := n.Apply(r.Context(), kv.Command{
		Op: kv.OpPut, Key: r.PathValue("key"), Value: body, ClientID: id, Seq: seq,
	})
	n.writeResult(w, r, res, err)
}

func (n *Node) httpDelete(w http.ResponseWriter, r *http.Request) {
	id, seq := httpSession(r)
	res, err := n.Apply(r.Context(), kv.Command{
		Op: kv.OpDelete, Key: r.PathValue("key"), ClientID: id, Seq: seq,
	})
	n.writeResult(w, r, res, err)
}

func (n *Node) httpMetrics(w http.ResponseWriter, r *http.Request) {
	// Refresh the gauges before rendering: they are updated on the node's own
	// goroutine after every effect batch, which on an idle node could be a while
	// ago.
	n.Stats()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	if err := n.Metrics(w); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (n *Node) httpStatus(w http.ResponseWriter, r *http.Request) {
	st := n.Stats()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"id":            st.ID,
		"role":          st.Role.String(),
		"term":          st.Term,
		"leader":        st.Leader,
		"commit_index":  st.CommitIndex,
		"applied_index": st.AppliedIndex,
		"elections":     st.Elections,
		"applied":       st.Applied,
		"proposals":     st.Proposals,
	})
}

// ParsePeers parses an "id=host:port,id=host:port" specification.
func ParsePeers(spec string) (map[uint64]string, error) {
	out := make(map[uint64]string)
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, addr, ok := strings.Cut(part, "=")
		if !ok {
			return nil, errors.New("server: peer must be id=host:port, got " + part)
		}
		n, err := strconv.ParseUint(strings.TrimSpace(id), 10, 64)
		if err != nil || n == 0 {
			return nil, errors.New("server: bad peer id in " + part)
		}
		out[n] = strings.TrimSpace(addr)
	}
	if len(out) == 0 {
		return nil, errors.New("server: no peers given")
	}
	return out, nil
}
