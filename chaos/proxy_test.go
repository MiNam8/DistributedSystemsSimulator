package chaos_test

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/MiNam8/kvstore/chaos"
)

// echoServer replies with whatever it receives.
func echoServer(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { lis.Close() })
	go func() {
		for {
			c, err := lis.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, 4096)
				for {
					n, err := c.Read(buf)
					if n > 0 {
						c.Write(buf[:n])
					}
					if err != nil {
						return
					}
				}
			}(c)
		}
	}()
	return lis.Addr().String()
}

// TestProxyLatencyIsLatencyNotBandwidth validates the measurement instrument.
//
// An injector that sleeps inline before each write serialises the delay, turning
// a 2 ms "latency" into a 500-message-per-second bandwidth cap. Benchmarks run
// through such a proxy produce numbers that look like properties of the system
// under test and are entirely artefacts of the harness.
func TestProxyLatencyIsLatencyNotBandwidth(t *testing.T) {
	target := echoServer(t)
	p, err := chaos.NewProxy("test", target)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	const oneWay = 20 * time.Millisecond
	p.SetLatency(oneWay)

	conn, err := net.Dial("tcp", p.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// A single round trip should cost about twice the one-way delay.
	buf := make([]byte, 64)
	start := time.Now()
	if _, err := conn.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Read(buf); err != nil {
		t.Fatal(err)
	}
	rtt := time.Since(start)
	if rtt < oneWay || rtt > 6*oneWay {
		t.Errorf("round trip = %v, want roughly %v", rtt, 2*oneWay)
	}
	t.Logf("round trip through a %v link: %v", oneWay, rtt.Round(time.Millisecond))

	// Now the property that matters: many messages in flight must not each pay
	// the delay serially. Sending 50 pipelined messages should take roughly one
	// delay period, not fifty.
	const n = 50
	start = time.Now()
	go func() {
		for i := range n {
			fmt.Fprintf(conn, "m%03d", i)
		}
	}()
	read := 0
	for read < n*4 {
		k, err := conn.Read(buf)
		if err != nil {
			t.Fatal(err)
		}
		read += k
	}
	burst := time.Since(start)

	if burst > 10*oneWay {
		t.Errorf("%d pipelined messages took %v through a %v link — the injector is "+
			"throttling bandwidth rather than adding latency", n, burst, oneWay)
	}
	t.Logf("%d pipelined messages through a %v link: %v", n, oneWay, burst.Round(time.Millisecond))
}
