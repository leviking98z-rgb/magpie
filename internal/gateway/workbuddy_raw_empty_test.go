package gateway

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yetone/magpie/internal/plugin"
	"github.com/yetone/magpie/internal/provider"
	"github.com/yetone/magpie/internal/testenv"
)

// The WorkBuddy Chat field layout used by its real streams, with synthetic
// ids and no private content. The reported route ended with this observable
// result: HTTP 200, no answer or call, and zero usage. The original upstream
// bytes were not retained. A plugin can pass such a stream through, so the
// gateway must check what its decoder actually produced, not just HTTP 200.
const workbuddyRawEmptyStream = "data: {\"id\":\"empty\",\"model\":\"gpt-5.6-sol\",\"object\":\"response\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"\",\"reasoning_content\":\"\",\"function_call\":null,\"tool_calls\":[],\"extra_fields\":null},\"logprobs\":null,\"finish_reason\":\"\"}],\"usage\":{\"prompt_tokens\":0,\"completion_tokens\":0,\"total_tokens\":0}}\n\n" +
	"data: {\"id\":\"empty\",\"model\":\"gpt-5.6-sol\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n" +
	"data: [DONE]\n\n"

const workbuddyRawAnswerStream = "data: {\"id\":\"answer\",\"model\":\"gpt-5.6-sol\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Verified result\"},\"finish_reason\":null}]}\n\n" +
	"data: {\"id\":\"answer\",\"model\":\"gpt-5.6-sol\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":12,\"completion_tokens\":3}}\n\n" +
	"data: [DONE]\n\n"

func workbuddyRawPlugin(t *testing.T, id string, up http.Handler) string {
	t.Helper()
	bun := testenv.Bun(t)
	fresh(t)
	t.Setenv("MAGPIE_BUN", bun)
	t.Setenv("FAKE_ID", id)
	t.Cleanup(plugin.Settle)
	srv := httptest.NewServer(up)
	t.Cleanup(srv.Close)
	t.Setenv("FAKE_BASE", srv.URL+"/v1")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	abs, err := filepath.Abs("testdata/workbuddy-empty-plugin.mjs")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := plugin.Add(ctx, abs); err != nil {
		t.Fatal(err)
	}
	if _, err := plugin.Providers(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := plugin.APIKey(ctx, id, 0, nil, "fixture-only", plugin.NewAccount); err != nil {
		t.Fatal(err)
	}
	pid := provider.PluginID(id)
	if p, err := provider.Find(pid); err != nil || !p.IsPlugin() || p.PluginProvider() != id {
		t.Fatalf("Find(%s) is not the fixture plugin: %v", pid, err)
	}
	return pid
}

func TestWorkBuddyRawEmptyCodexTurn(t *testing.T) {
	var mu sync.Mutex
	var bodies []string
	var emptySends int
	pid := workbuddyRawPlugin(t, "workbuddy", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/chat/completions" {
			t.Errorf("unexpected upstream %s %s", r.Method, r.URL.Path)
			http.Error(w, "unexpected upstream path", 400)
			return
		}
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, string(b))
		empty := len(bodies) <= emptySends
		mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		if empty {
			io.WriteString(w, workbuddyRawEmptyStream)
		} else {
			io.WriteString(w, workbuddyRawAnswerStream)
		}
	}))
	model := pid + "/gpt-5.6-sol"
	if err := provider.SetModelAPI(model, string(provider.Chat)); err != nil {
		t.Fatal(err)
	}
	if err := provider.SetModelEfforts(model, []string{"low", "medium", "high", "xhigh", "max"}); err != nil {
		t.Fatal(err)
	}
	for _, source := range []string{"main", "collab_spawn"} {
		for _, tc := range []struct {
			name   string
			empty  int
			sends  int
			status int
		}{
			{"first-retry", 1, 2, 200},
			{"tenth-retry", 10, 11, 200},
			{"exhausted", 11, 11, 502},
		} {
			t.Run(source+"/"+tc.name, func(t *testing.T) {
				mu.Lock()
				bodies, emptySends = nil, tc.empty
				mu.Unlock()
				// The old tool is already answered. Retrying only this model
				// request must neither rerun the tool nor change its result.
				ask := `{"model":"` + model + `","stream":true,"reasoning":{"effort":"max"},"prompt_cache_key":"` + source + "-" + tc.name + `","tools":[{"type":"function","name":"run","parameters":{"type":"object"}}],"input":[{"role":"user","content":"Check the result and report it."},{"type":"function_call","call_id":"already-ran","name":"run","arguments":"{}"},{"type":"function_call_output","call_id":"already-ran","output":"exit_code: 0; result already saved"}]}`
				r := httptest.NewRequest(http.MethodPost, "/backend-api/codex/responses", strings.NewReader(ask))
				r.Header.Set("Authorization", "Bearer magpie-codex")
				if source != "main" {
					r.Header.Set("x-openai-subagent", source)
				}
				s, w := New(), httptest.NewRecorder()
				s.Handler().ServeHTTP(w, r)
				mu.Lock()
				defer mu.Unlock()
				if len(bodies) != tc.sends {
					t.Fatalf("upstream sends=%d, want %d; status=%d reply=%s", len(bodies), tc.sends, w.Code, w.Body)
				}
				for i, b := range bodies {
					if b != bodies[0] {
						t.Fatalf("retry %d changed the completed tool's history", i)
					}
					var request struct {
						Model  string `json:"model"`
						Effort string `json:"reasoning_effort"`
					}
					if json.Unmarshal([]byte(b), &request) != nil || request.Model != "gpt-5.6-sol" || request.Effort != "max" {
						t.Fatalf("retry %d changed the requested model/effort: %s", i, b)
					}
				}
				body := w.Body.String()
				if tc.status == 200 {
					if w.Code != 200 || !strings.Contains(body, "Verified result") || !strings.Contains(body, "response.completed") || strings.Contains(body, "empty reply") {
						t.Fatalf("no final answer after recovery: %d %s", w.Code, body)
					}
				} else if !strings.Contains(body, "empty reply") || strings.Contains(body, "response.completed") || w.Code != 502 && !strings.Contains(body, "response.failed") {
					t.Fatalf("empty reply must fail explicitly, not complete a Codex turn: %d %s", w.Code, body)
				}
				if tries := s.trace.routes[len(s.trace.routes)-1].Tries; len(tries) != tc.sends {
					t.Fatalf("unrecorded model retries: %+v", tries)
				}
			})
		}
	}
}

func TestWorkBuddyRawEmptyShapesAndClients(t *testing.T) {
	var mu sync.Mutex
	var upstream string
	var sends int
	pid := workbuddyRawPlugin(t, "workbuddy", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		mu.Lock()
		sends++
		body := upstream
		mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, body)
	}))
	model := pid + "/gpt-5.6-sol"
	for _, shape := range []struct{ name, body string }{
		{"zero-usage", workbuddyRawEmptyStream},
		{"counted-no-answer", strings.ReplaceAll(workbuddyRawEmptyStream, `"prompt_tokens":0`, `"prompt_tokens":12`)},
		{"reasoning-only", strings.ReplaceAll(workbuddyRawEmptyStream, `"reasoning_content":""`, `"reasoning_content":"Pending reasoning"`)},
		{"extra-metadata", strings.ReplaceAll(workbuddyRawEmptyStream, `"extra_fields":null`, `"extra_fields":{"internal_trace":"not an answer"}`)},
		{"unknown-frame", sse(`data: {"type":"internal_trace","payload":"not an answer"}`, `data: [DONE]`)},
		{"empty-choices", sse(`data: {"id":"empty","choices":[],"usage":{"prompt_tokens":12}}`, `data: [DONE]`)},
		{"thought-tag-only", strings.Replace(workbuddyRawEmptyStream, `"content":""`, `"content":"<think>Pending reasoning</think>"`, 1)},
		{"marked-thought-only", sse(
			`data: {"id":"empty","choices":[{"index":0,"delta":{"content":"Pending reasoning","extra_content":{"google":{"thought":true}}}}]}`,
			`data: {"id":"empty","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
			`data: [DONE]`,
		)},
	} {
		for _, client := range []struct {
			name, path, ask, subagent string
		}{
			{"codex-main-stream", "/backend-api/codex/responses", `"stream":true,"input":"Report the saved result."`, ""},
			{"codex-main-whole", "/backend-api/codex/responses", `"stream":false,"input":"Report the saved result."`, ""},
			{"codex-subagent-stream", "/backend-api/codex/responses", `"stream":true,"input":"Report the saved result."`, "collab_spawn"},
			{"codex-subagent-whole", "/backend-api/codex/responses", `"stream":false,"input":"Report the saved result."`, "collab_spawn"},
			{"messages-stream", "/v1/messages", `"stream":true,"max_tokens":32,"messages":[{"role":"user","content":"Report the saved result."}]`, ""},
			{"messages-whole", "/v1/messages", `"stream":false,"max_tokens":32,"messages":[{"role":"user","content":"Report the saved result."}]`, ""},
		} {
			t.Run(shape.name+"/"+client.name, func(t *testing.T) {
				mu.Lock()
				upstream, sends = shape.body, 0
				mu.Unlock()
				ask := `{"model":"` + model + `","prompt_cache_key":"` + shape.name + "-" + client.name + `",` + client.ask + `}`
				r := httptest.NewRequest(http.MethodPost, client.path, strings.NewReader(ask))
				r.Header.Set("Authorization", "Bearer magpie-codex")
				if client.subagent != "" {
					r.Header.Set("x-openai-subagent", client.subagent)
				}
				w := httptest.NewRecorder()
				New().Handler().ServeHTTP(w, r)
				mu.Lock()
				got := sends
				mu.Unlock()
				body := w.Body.String()
				if got != 11 || !strings.Contains(body, "empty reply") || strings.Contains(body, "response.completed") || strings.Contains(body, `"type":"message_stop"`) {
					t.Fatalf("empty turn succeeded or lost its retry budget: sends=%d status=%d body=%s", got, w.Code, body)
				}
				if strings.Contains(body, "Pending reasoning") {
					t.Fatalf("reasoning escaped from a failed empty attempt: %s", body)
				}
			})
		}
	}
}

func TestWorkBuddyRawAnswerAndTerminalReasons(t *testing.T) {
	var mu sync.Mutex
	var upstream string
	var sends int
	pid := workbuddyRawPlugin(t, "workbuddy", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		mu.Lock()
		sends++
		body := upstream
		mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, body)
	}))
	thought := sse(`data: {"id":"answer","choices":[{"index":0,"delta":{"reasoning_content":"Pending reasoning"}}]}`)
	for _, tc := range []struct {
		name, body, want string
	}{
		{"answer", workbuddyRawAnswerStream, "Verified result"},
		{"reasoning-then-answer", thought + workbuddyRawAnswerStream, "Pending reasoning"},
		{"tool-call", sse(
			`data: {"id":"answer","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"new-call","type":"function","function":{"name":"verify","arguments":"{}"}}]}}]}`,
			`data: {"id":"answer","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`,
			`data: [DONE]`,
		), "new-call"},
		{"length", thought + strings.Replace(workbuddyRawEmptyStream, `"finish_reason":"stop"`, `"finish_reason":"length"`, 1), "max_output_tokens"},
		{"filter", strings.Replace(workbuddyRawEmptyStream, `"finish_reason":"stop"`, `"finish_reason":"content_filter"`, 1), "content_filter"},
	} {
		for _, stream := range []bool{true, false} {
			t.Run(tc.name+map[bool]string{true: "/stream", false: "/whole"}[stream], func(t *testing.T) {
				mu.Lock()
				upstream, sends = tc.body, 0
				mu.Unlock()
				ask, err := json.Marshal(map[string]any{
					"model": pid + "/gpt-5.6-sol", "stream": stream, "input": "Verify the saved result.",
					"tools": []any{map[string]any{"type": "function", "name": "verify", "parameters": map[string]any{"type": "object"}}},
				})
				if err != nil {
					t.Fatal(err)
				}
				r := httptest.NewRequest(http.MethodPost, "/backend-api/codex/responses", strings.NewReader(string(ask)))
				r.Header.Set("Authorization", "Bearer magpie-codex")
				w := httptest.NewRecorder()
				New().Handler().ServeHTTP(w, r)
				mu.Lock()
				got := sends
				mu.Unlock()
				body := w.Body.String()
				if got != 1 || strings.Contains(body, "empty reply") || !strings.Contains(body, tc.want) {
					t.Fatalf("actionable output or terminal reason changed: sends=%d status=%d body=%s", got, w.Code, body)
				}
				if tc.name == "reasoning-then-answer" && !strings.Contains(body, "Verified result") {
					t.Fatalf("held reasoning lost the final answer: %s", body)
				}
			})
		}
	}
}

func TestWorkBuddyRawPendingLimit(t *testing.T) {
	var mu sync.Mutex
	var sends int
	// Many readable chunks, not one overlong SSE line, reach the guard's
	// own bound before any answer or call is emitted.
	chunk := sse(`data: {"id":"pending","choices":[{"index":0,"delta":{"reasoning_content":"` + strings.Repeat("r", 16<<10) + `"}}]}`)
	upstream := strings.Repeat(chunk, 257)
	pid := workbuddyRawPlugin(t, "workbuddy", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		mu.Lock()
		sends++
		mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, upstream)
	}))
	ask := `{"model":"` + pid + `/gpt-5.6-sol","stream":true,"input":"Report the result."}`
	r := httptest.NewRequest(http.MethodPost, "/backend-api/codex/responses", strings.NewReader(ask))
	r.Header.Set("Authorization", "Bearer magpie-codex")
	w := httptest.NewRecorder()
	New().Handler().ServeHTTP(w, r)
	mu.Lock()
	got := sends
	mu.Unlock()
	if got != 3 || !strings.Contains(w.Body.String(), "buffer limit") || strings.Contains(w.Body.String(), "response.completed") || strings.Contains(w.Body.String(), strings.Repeat("r", 256)) {
		t.Fatalf("pending reasoning passed its bound or completed: sends=%d status=%d body=%s", got, w.Code, w.Body)
	}
}

func TestWorkBuddyRawNeighborEmptyTurnsUnchanged(t *testing.T) {
	for _, tc := range []struct{ id, model string }{
		{"workbuddy", "claude-opus-5.5"},
		{"another-vendor", "gpt-5.6-sol"},
	} {
		t.Run(tc.id+"/"+tc.model, func(t *testing.T) {
			var mu sync.Mutex
			var sends int
			pid := workbuddyRawPlugin(t, tc.id, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				mu.Lock()
				sends++
				mu.Unlock()
				w.Header().Set("Content-Type", "text/event-stream")
				io.WriteString(w, workbuddyRawEmptyStream)
			}))
			ask := `{"model":"` + pid + "/" + tc.model + `","stream":true,"input":"Report the result."}`
			r := httptest.NewRequest(http.MethodPost, "/backend-api/codex/responses", strings.NewReader(ask))
			r.Header.Set("Authorization", "Bearer magpie-codex")
			w := httptest.NewRecorder()
			New().Handler().ServeHTTP(w, r)
			mu.Lock()
			got := sends
			mu.Unlock()
			if got != 1 || w.Code != 200 || !strings.Contains(w.Body.String(), "response.completed") || strings.Contains(w.Body.String(), "empty reply") {
				t.Fatalf("another model/vendor's empty-turn semantics changed: sends=%d status=%d body=%s", got, w.Code, w.Body)
			}
		})
	}
}

func TestWorkBuddyRawGroupBudgetUnchanged(t *testing.T) {
	for _, next := range []bool{false, true} {
		t.Run(map[bool]string{true: "next-member", false: "last-member"}[next], func(t *testing.T) {
			var mu sync.Mutex
			var sends []string
			pid := workbuddyRawPlugin(t, "workbuddy", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var ask struct{ Model string }
				if err := json.NewDecoder(r.Body).Decode(&ask); err != nil {
					t.Error(err)
					http.Error(w, "invalid fixture request", 400)
					return
				}
				mu.Lock()
				sends = append(sends, ask.Model)
				mu.Unlock()
				w.Header().Set("Content-Type", "text/event-stream")
				if ask.Model == "gpt-5.6-sol" {
					io.WriteString(w, workbuddyRawEmptyStream)
				} else {
					io.WriteString(w, workbuddyRawAnswerStream)
				}
			}))
			members := []string{pid + "/gpt-5.6-sol"}
			if next {
				members = append(members, pid+"/claude-opus-5.5")
			}
			if err := provider.SaveGroup(provider.Group{Name: "Fixture", Members: members, Routing: provider.Ordered, Patience: provider.PatienceOff}); err != nil {
				t.Fatal(err)
			}
			r := httptest.NewRequest(http.MethodPost, "/backend-api/codex/responses", strings.NewReader(`{"model":"group/fixture","stream":true,"input":"Report the result."}`))
			r.Header.Set("Authorization", "Bearer magpie-codex")
			w := httptest.NewRecorder()
			New().Handler().ServeHTTP(w, r)
			mu.Lock()
			defer mu.Unlock()
			if next {
				if len(sends) != 2 || sends[0] != "gpt-5.6-sol" || sends[1] != "claude-opus-5.5" || !strings.Contains(w.Body.String(), "Verified result") {
					t.Fatalf("group order or budget changed: sends=%v status=%d body=%s", sends, w.Code, w.Body)
				}
			} else if len(sends) != 3 || !strings.Contains(w.Body.String(), "empty reply") || strings.Contains(w.Body.String(), "response.completed") {
				t.Fatalf("standalone budget applied to a group: sends=%v status=%d body=%s", sends, w.Code, w.Body)
			}
		})
	}
}
