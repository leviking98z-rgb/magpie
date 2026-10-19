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

const workbuddyInputBody = `{"error":{"message":"WorkBuddy: Model temporarily not available for this request after 1 upstream input rejections (WorkBuddy 11133: Invalid request parameters)","type":"upstream_model_unavailable","code":11133}}`

func TestWorkBuddyInputRepairDoesNotMultiply(t *testing.T) {
	for _, proto := range []provider.Protocol{provider.Chat, provider.Responses} {
		for _, group := range []bool{false, true} {
			fresh(t)
			a := &scripted{replies: []reply{{400, "application/json", workbuddyInputBody}}}
			b := &scripted{replies: []reply{{400, "application/json", workbuddyInputBody}}}
			scriptedOn(t, "a", proto, a)
			scriptedOn(t, "b", proto, b)
			model, want := "a/m", 11
			if group {
				if err := provider.SaveGroup(provider.Group{Name: "G", Members: []string{"a/m", "b/m"}, Routing: provider.Ordered, Patience: provider.PatienceOff}); err != nil {
					t.Fatal(err)
				}
				model, want = "group/g", 1
			}
			body := `{"model":"` + model + `","stream":true,"input":"hi","prompt_cache_key":"thread-1","tools":[{"type":"image_generation"}]}`
			w := httptest.NewRecorder()
			New().Handler().ServeHTTP(w, httptest.NewRequest("POST", "/v1/responses", strings.NewReader(body)))
			if w.Code != 400 || a.n != want || group && b.n != 1 || !group && b.n != 0 {
				t.Fatalf("proto=%s group=%v status=%d sends=%d/%d: %s", proto, group, w.Code, a.n, b.n, w.Body.String())
			}
		}
	}
}

func TestWorkBuddyStandaloneRetryBudget(t *testing.T) {
	for _, failure := range []struct {
		name   string
		status int
		body   string
		marker string
	}{
		{"empty", 502, workbuddyEmptyBody, "an empty reply"},
		{"input", 400, workbuddyInputBody, "WorkBuddy 11133"},
		{"renamed-input-provider", 400, strings.ReplaceAll(workbuddyInputBody, "WorkBuddy: ", "Loopback only: "), "WorkBuddy 11133"},
		{"unprefixed-input", 400, strings.ReplaceAll(workbuddyInputBody, "WorkBuddy: ", ""), "WorkBuddy 11133"},
	} {
		t.Run(failure.name, func(t *testing.T) {
			for _, client := range []struct {
				name string
				path string
				body string
			}{
				{"responses", "/v1/responses", `{"model":"a/m","input":"Reply OK","stream":true,"prompt_cache_key":"thread-1"}`},
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
						{"tenth-retry-recovers", 10, 200, 11},
						{"exhausted", 11, failure.status, 11},
					} {
						t.Run(tc.name, func(t *testing.T) {
							fresh(t)
							a := &scripted{}
							for range tc.empty {
								a.replies = append(a.replies, reply{failure.status, "", failure.body})
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
							if tc.status >= 400 && (!strings.Contains(w.Body.String(), failure.marker) || strings.Contains(w.Body.String(), "response.completed")) {
								t.Fatalf("exhaustion must be explicit, not a completed turn: %s", w.Body.String())
							}
							if tries := s.trace.routes[len(s.trace.routes)-1].Tries; len(tries) != tc.attempts {
								t.Fatalf("unrecorded retries: %+v", tries)
							}
						})
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
	for _, failure := range []struct {
		status int
		body   string
	}{{502, workbuddyEmptyBody}, {400, workbuddyInputBody}} {
		for again, want := range []time.Duration{1, 2, 4, 5, 5, 5, 5, 5, 5, 5} {
			wait, ok := passing(failure.status, nil, []byte(failure.body), again, false)
			if !ok || wait != want*time.Second {
				t.Errorf("retry %d: wait=%v ok=%v; want %vs", again+1, wait, ok, want)
			}
		}
	}
	for _, tc := range []struct {
		name   string
		status int
		body   string
		group  bool
		again  int
	}{
		{"eleventh-retry-denied", 502, workbuddyEmptyBody, false, 10},
		{"input-eleventh-retry-denied", 400, workbuddyInputBody, false, 10},
		{"input-group-no-retry", 400, workbuddyInputBody, true, 0},
		{"legacy-input-no-multiplication", 400, strings.ReplaceAll(workbuddyInputBody, "after 1", "after 2"), false, 0},
		{"schema-error", 400, `{"error":{"message":"Invalid tool schema","code":11133}}`, false, 0},
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
		name   string
		body   string
		group  bool
		next   bool
		want   int
		status int
	}{
		{"group-next", workbuddyEmptyBody, true, true, 1, 502},
		{"group-exhausted", workbuddyEmptyBody, true, false, 3, 502},
		{"input-group-next", workbuddyInputBody, true, true, 1, 400},
		{"input-group-exhausted", workbuddyInputBody, true, false, 1, 400},
		{"other-vendor-empty", `{"error":{"message":"an empty reply (the model answered nothing; try again)"}}`, false, false, 3, 502},
		{"ordinary-bad-gateway", `{"error":{"message":"bad gateway"}}`, false, false, 3, 502},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fresh(t)
			a := &scripted{replies: []reply{{tc.status, "", tc.body}}}
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
			if a.n != tc.want || tc.next && (code != 200 || b.n != 1) || !tc.next && code != tc.status {
				t.Fatalf("status=%d first attempts=%d; want %d: %s", code, a.n, tc.want, body)
			}
		})
	}
}
