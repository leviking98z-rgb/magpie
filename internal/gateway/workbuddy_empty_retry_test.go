package gateway

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/yetone/magpie/internal/provider"
)

const workbuddyEmptyBody = `{"error":{"message":"WorkBuddy: an empty reply (the model answered nothing; try again); upstream HTTP 200 contained no output","type":"upstream_model_unavailable","code":"workbuddy_empty_reply","upstream_status":200}}`

func TestWorkBuddyEmptyStandaloneRetryBudget(t *testing.T) {
	for _, client := range []struct {
		name string
		path string
		body string
	}{
		{"responses", "/v1/responses", `{"model":"a/m","input":"Reply OK","stream":true}`},
		{"chat", "/v1/chat/completions", `{"model":"a/m","messages":[{"role":"user","content":"Reply OK"}],"stream":true}`},
		{"messages", "/v1/messages", `{"model":"a/m","messages":[{"role":"user","content":"Reply OK"}],"max_tokens":32,"stream":true}`},
	} {
		t.Run(client.name, func(t *testing.T) {
			for _, tc := range []struct {
				name     string
				empty    int
				status   int
				attempts int
			}{
				{"first-retry", 1, 200, 2},
				{"fifth-retry-recovers", 5, 200, 6},
				{"exhausted", 6, 502, 6},
			} {
				t.Run(tc.name, func(t *testing.T) {
					fresh(t)
					a := &scripted{}
					for range tc.empty {
						a.replies = append(a.replies, reply{502, "", workbuddyEmptyBody})
					}
					a.replies = append(a.replies, reply{200, "text/event-stream", sse(
						`data: {"id":"ok","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"hello"},"finish_reason":null}]}`,
						`data: {"id":"ok","object":"chat.completion.chunk","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
						`data: [DONE]`,
					)})
					scriptedOn(t, "a", provider.Chat, a)
					s := New()
					w := httptest.NewRecorder()
					r := httptest.NewRequest("POST", client.path, strings.NewReader(client.body))
					s.Handler().ServeHTTP(w, r)
					if w.Code != tc.status || a.n != tc.attempts {
						t.Fatalf("status=%d attempts=%d; want %d and %d: %s", w.Code, a.n, tc.status, tc.attempts, w.Body.String())
					}
					if tc.status == 200 && !strings.Contains(w.Body.String(), "hello") {
						t.Fatalf("no answer after recovery: %s", w.Body.String())
					}
					if tc.status == 502 && (!strings.Contains(w.Body.String(), "an empty reply") || strings.Contains(w.Body.String(), "response.completed")) {
						t.Fatalf("exhaustion must be explicit, not a completed turn: %s", w.Body.String())
					}
					if tries := s.trace.routes[len(s.trace.routes)-1].Tries; len(tries) != tc.attempts {
						t.Fatalf("unrecorded retries: %+v", tries)
					}
				})
			}
		})
	}
}

func TestWorkBuddyEmptyRetryPauses(t *testing.T) {
	old := retryPause
	retryPause = time.Second
	t.Cleanup(func() { retryPause = old })
	for again, want := range []time.Duration{1, 2, 4, 5, 5} {
		wait, ok := passing(502, nil, []byte(workbuddyEmptyBody), again, false)
		if !ok || wait != want*time.Second {
			t.Errorf("retry %d: wait=%v ok=%v; want %vs", again+1, wait, ok, want)
		}
	}
	for _, tc := range []struct {
		name   string
		status int
		body   string
		group  bool
		again  int
	}{
		{"sixth-retry-denied", 502, workbuddyEmptyBody, false, 5},
		{"group-budget-unchanged", 502, workbuddyEmptyBody, true, 2},
		{"other-status", 503, workbuddyEmptyBody, false, 2},
		{"other-empty-source", 502, strings.ReplaceAll(workbuddyEmptyBody, "upstream HTTP 200 contained no output", "connection closed"), false, 2},
		{"other-vendor", 502, strings.ReplaceAll(workbuddyEmptyBody, "WorkBuddy: ", "Vendor: "), false, 2},
		{"malformed", 502, workbuddyEmptyBody + "!", false, 2},
		{"rate-budget-unchanged", 429, `{"error":{"message":"Rate limit reached for requests"}}`, false, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if wait, ok := passing(tc.status, nil, []byte(tc.body), tc.again, tc.group); ok {
				t.Fatalf("unexpected retry with wait %v", wait)
			}
		})
	}
	for _, seconds := range []string{"7", "9"} {
		wait, ok := passing(502, http.Header{"Retry-After": {seconds}}, []byte(workbuddyEmptyBody), 4, false)
		if seconds == "7" && (!ok || wait != 7*time.Second) || seconds == "9" && ok {
			t.Errorf("Retry-After=%s: wait=%v ok=%v", seconds, wait, ok)
		}
	}
}

func TestWorkBuddyEmptyGroupsAndOtherFailuresUnchanged(t *testing.T) {
	for _, tc := range []struct {
		name  string
		body  string
		group bool
		next  bool
		want  int
	}{
		{"group-next", workbuddyEmptyBody, true, true, 1},
		{"group-exhausted", workbuddyEmptyBody, true, false, 3},
		{"other-vendor-empty", `{"error":{"message":"an empty reply (the model answered nothing; try again)"}}`, false, false, 3},
		{"ordinary-bad-gateway", `{"error":{"message":"bad gateway"}}`, false, false, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fresh(t)
			a := &scripted{replies: []reply{{502, "", tc.body}}}
			scriptedOn(t, "a", provider.Chat, a)
			model := "a/m"
			var b *scripted
			if tc.group {
				members := []string{"a/m"}
				if tc.next {
					b = &scripted{replies: []reply{{200, "", chatOK}}}
					scriptedOn(t, "b", provider.Chat, b)
					members = append(members, "b/m")
				}
				if err := provider.SaveGroup(provider.Group{Name: "G", Members: members, Routing: provider.Ordered, Patience: provider.PatienceOff}); err != nil {
					t.Fatal(err)
				}
				model = "group/g"
			}
			code, body := postAs(t, New(), "", `{"model":"`+model+`","messages":[{"role":"user","content":"hi"}]}`)
			if a.n != tc.want || tc.next && (code != 200 || b.n != 1) || !tc.next && code != 502 {
				t.Fatalf("status=%d first attempts=%d; want %d: %s", code, a.n, tc.want, body)
			}
		})
	}
}
