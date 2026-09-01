package gateway

import (
	"bytes"
	"compress/gzip"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/JuliusBrussee/caveman/proxy/providers"
	"github.com/JuliusBrussee/caveman/proxy/providers/anthropic"
)

// #897: a non-streaming upstream body cut mid-transfer must never reach the
// client as a 200 with a truncated (undecodable) gzip payload. The proxy reads
// the whole body first, retries once, and otherwise answers a clean 502.
func TestNonStreamingTruncatedUpstreamBodyRetriesThenFails(t *testing.T) {
	var gz bytes.Buffer
	zw := gzip.NewWriter(&gz)
	_, _ = zw.Write([]byte(`{"id":"msg_1","type":"message","content":[{"type":"text","text":"` + string(bytes.Repeat([]byte("x"), 20000)) + `"}],"usage":{"input_tokens":1,"output_tokens":2}}`))
	_ = zw.Close()
	full := gz.Bytes()

	run := func(t *testing.T, failures int32) *httptest.ResponseRecorder {
		var calls int32
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			n := atomic.AddInt32(&calls, 1)
			w.Header().Set("content-type", "application/json")
			w.Header().Set("content-encoding", "gzip")
			if n <= failures {
				// Advertise the full length, send half, then drop the connection.
				w.Header().Set("content-length", "999999")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write(full[:len(full)/2])
				w.(http.Flusher).Flush()
				panic(http.ErrAbortHandler)
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(full)
		}))
		defer upstream.Close()
		srv := New(Config{
			Adapters:   []providers.Adapter{anthropic.New(upstream.URL)},
			Auth:       stubAuth{rc: RequestContext{Label: "local", RuntimeMode: "record"}},
			Creds:      stubCreds{key: "sk-byok"},
			Sink:       &captureSink{},
			HTTPClient: &http.Client{Transport: &http.Transport{DisableCompression: true}},
		})
		req := httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader([]byte(`{"model":"claude-sonnet-5","max_tokens":8,"messages":[{"role":"user","content":"hi"}]}`)))
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, req)
		return rec
	}

	rec := run(t, 1)
	if rec.Code != http.StatusOK || !bytes.Equal(rec.Body.Bytes(), full) {
		t.Fatalf("one upstream cut must be retried transparently: status=%d bytes=%d want=%d", rec.Code, rec.Body.Len(), len(full))
	}
	if got := rec.Header().Get("Content-Length"); got != strconv.Itoa(len(full)) {
		t.Fatalf("content-length = %q, want %d", got, len(full))
	}

	rec = run(t, 2)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("two upstream cuts must surface as 502, got %d (%d bytes)", rec.Code, rec.Body.Len())
	}
	if rec.Header().Get("Content-Encoding") != "" {
		t.Fatal("502 must not carry the upstream content-encoding")
	}
}
