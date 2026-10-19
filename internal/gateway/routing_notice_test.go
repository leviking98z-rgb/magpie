package gateway

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/yetone/magpie/internal/provider"
)

func TestRoutingNoticeIsOptInAndDoesNotChangeReply(t *testing.T) {
	for _, tc := range []struct {
		name                string
		opt, group, failure bool
	}{
		{"switch", true, true, true}, {"first", true, true, false},
		{"old-client", false, true, true}, {"direct", true, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fresh(t)
			good := reply{200, "text/event-stream", sse(
				`data: {"id":"ok","choices":[{"index":0,"delta":{"content":"hello"},"finish_reason":null}]}`,
				`data: {"id":"ok","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
				`data: [DONE]`,
			)}
			a := &scripted{replies: []reply{good}}
			if tc.failure {
				a.replies = []reply{{400, "", workbuddyInputBody}}
			}
			scriptedOn(t, "a", provider.Chat, a)
			scriptedOn(t, "b", provider.Chat, &scripted{replies: []reply{good}})
			model := "a/m"
			if tc.group {
				if err := provider.SaveGroup(provider.Group{Name: "G", Members: []string{"a/m", "b/m"}, Routing: provider.Ordered, Patience: provider.PatienceOff}); err != nil {
					t.Fatal(err)
				}
				model = "group/g"
			}
			r := httptest.NewRequest("POST", "/v1/responses", strings.NewReader(`{"model":"`+model+`","input":"hi","stream":true}`))
			if tc.opt {
				r.Header.Set(routingNoticeHeader, "v1")
			}
			w := httptest.NewRecorder()
			New().Handler().ServeHTTP(w, r)
			body := w.Body.String()
			if w.Code != 200 || !strings.Contains(body, "hello") {
				t.Fatalf("%d %s", w.Code, body)
			}
			count := strings.Count(body, "event: response.magpie_routing\n")
			want := 0
			if tc.opt && tc.group {
				want = 1
			}
			if count != want {
				t.Fatalf("notices=%d want=%d: %s", count, want, body)
			}
			if want > 0 && tc.failure && !strings.Contains(body, `"routing":{"fallback":true,"from_model":"a/m","to_model":"b/m"}`) {
				t.Fatal(body)
			}
			if w.Header().Get("OpenAI-Model") != "" {
				t.Fatal("must not trigger Codex safety-routing warning")
			}
		})
	}
}

func TestRoutingNoticeHeldWithCandidateAfterKeepalive(t *testing.T) {
	w := httptest.NewRecorder()
	alive := &keptAlive{proto: provider.Responses, told: time.Now(), streams: true}
	h := newHoldWriter(w, true)
	h.alive, h.streams = alive, true
	h.routingNotice = routingNotice("group/g", "a/m", "b/m")
	h.keepAlive()
	if strings.Contains(w.Body.String(), "magpie_routing") {
		t.Fatal("announced before output")
	}
	h.Header().Set("Content-Type", "text/event-stream")
	h.WriteHeader(200)
	h.Write([]byte("event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\n"))
	h.release()
	if strings.Count(w.Body.String(), "event: response.magpie_routing\n") != 1 {
		t.Fatal(w.Body.String())
	}
	if strings.Index(w.Body.String(), "magpie_routing") > strings.Index(w.Body.String(), "response.output_text.delta") {
		t.Fatal("notice followed output")
	}
}

func TestRoutingNoticeNeverAnnouncesFailedCandidate(t *testing.T) {
	for _, code := range []int{400, 502} {
		w := httptest.NewRecorder()
		h := newHoldWriter(w, true)
		h.streams = true
		h.routingNotice = routingNotice("group/g", "a/m", "b/m")
		h.Header().Set("Content-Type", "application/json")
		h.WriteHeader(code)
		h.Write([]byte(`{"error":{"message":"unavailable"}}`))
		h.release()
		if strings.Contains(w.Body.String(), "magpie_routing") {
			t.Fatal(w.Body.String())
		}
	}
}

func TestRoutingNoticePreservesStreamBytes(t *testing.T) {
	body := "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\n"
	for _, opt := range []bool{false, true} {
		w := httptest.NewRecorder()
		h := newHoldWriter(w, true)
		h.streams = true
		notice := routingNotice("group/g", "a/m", "b/m")
		if opt {
			h.routingNotice = notice
		}
		h.Header().Set("Content-Type", "text/event-stream")
		h.WriteHeader(200)
		h.Write([]byte(body))
		h.release()
		want := body
		if opt {
			want = string(notice) + want
		}
		if w.Body.String() != want {
			t.Fatalf("opt=%v: got %q want %q", opt, w.Body.String(), want)
		}
	}
}
