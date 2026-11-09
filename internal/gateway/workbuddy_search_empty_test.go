package gateway

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yetone/magpie/internal/provider"
)

// Codex offers native web_search even when it is only checking a completed
// tool. That selects searchReply rather than streamTranslated; both paths
// must reject a raw reply that contains no answer or client tool call.
func TestWorkBuddySearchEmptyCodexTurn(t *testing.T) {
	var mu sync.Mutex
	var bodies []string
	var emptySends int
	var upstream string
	pid := workbuddyRawPlugin(t, "workbuddy", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/chat/completions" {
			t.Errorf("unexpected upstream %s %s", r.Method, r.URL.Path)
			http.Error(w, "unexpected upstream path", 400)
			return
		}
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, string(b))
		body := workbuddyRawAnswerStream
		if len(bodies) <= emptySends {
			body = upstream
		}
		mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, body)
	}))
	model := pid + "/gpt-5.6-sol"
	if err := provider.SetModelAPI(model, string(provider.Chat)); err != nil {
		t.Fatal(err)
	}
	for _, source := range []string{"main", "collab_spawn"} {
		for _, stream := range []bool{true, false} {
			for _, tc := range []struct {
				name, body string
				empty      int
				sends      int
			}{
				{"empty-first-retry", workbuddyRawEmptyStream, 1, 2},
				{"reasoning-first-retry", strings.ReplaceAll(workbuddyRawEmptyStream, `"reasoning_content":""`, `"reasoning_content":"Pending reasoning"`), 1, 2},
				{"empty-tenth-retry", workbuddyRawEmptyStream, 10, 11},
				{"empty-exhausted", workbuddyRawEmptyStream, 11, 11},
			} {
				t.Run(source+"/"+map[bool]string{true: "stream", false: "whole"}[stream]+"/"+tc.name, func(t *testing.T) {
					mu.Lock()
					bodies, emptySends, upstream = nil, tc.empty, tc.body
					mu.Unlock()
					ask, err := json.Marshal(map[string]any{
						"model": model, "stream": stream, "reasoning": map[string]any{"effort": "max"},
						"prompt_cache_key": t.Name(),
						"tools": []any{
							map[string]any{"type": "web_search"},
							map[string]any{"type": "function", "name": "run", "parameters": map[string]any{"type": "object"}},
						},
						"input": []any{
							map[string]any{"role": "user", "content": "Check the saved result and report it."},
							map[string]any{"type": "function_call", "call_id": "already-ran", "name": "run", "arguments": "{}"},
							map[string]any{"type": "function_call_output", "call_id": "already-ran", "output": "exit_code: 0; result already saved"},
						},
					})
					if err != nil {
						t.Fatal(err)
					}
					r := httptest.NewRequest(http.MethodPost, CodexPath+"/responses", strings.NewReader(string(ask)))
					r.Header.Set("Authorization", "Bearer magpie-codex")
					if source != "main" {
						r.Header.Set("x-openai-subagent", source)
					}
					s, w := New(), httptest.NewRecorder()
					s.Handler().ServeHTTP(w, r)
					mu.Lock()
					defer mu.Unlock()
					if len(bodies) != tc.sends {
						t.Fatalf("search path silently ended or lost its retry budget: sends=%d want=%d status=%d reply=%s", len(bodies), tc.sends, w.Code, w.Body)
					}
					for i, b := range bodies {
						if b != bodies[0] {
							t.Fatalf("retry %d changed the completed tool history", i)
						}
						var q struct {
							Model  string `json:"model"`
							Effort string `json:"reasoning_effort"`
							Tools  []struct {
								Function struct{ Name string } `json:"function"`
							} `json:"tools"`
						}
						if json.Unmarshal([]byte(b), &q) != nil || q.Model != "gpt-5.6-sol" || q.Effort != "max" || len(q.Tools) != 2 || q.Tools[1].Function.Name != "web_search" {
							t.Fatalf("retry %d changed model/effort or bypassed search translation: %s", i, b)
						}
					}
					body := w.Body.String()
					if strings.Contains(body, "Pending reasoning") {
						t.Fatalf("reasoning escaped from an empty attempt: %s", body)
					}
					if tc.empty < tc.sends {
						if w.Code != 200 || !strings.Contains(body, "Verified result") || stream && !strings.Contains(body, "response.completed") || !stream && !strings.Contains(body, `"status":"completed"`) {
							t.Fatalf("search path returned no final answer after recovery: %d %s", w.Code, body)
						}
					} else if !strings.Contains(body, "empty reply") || strings.Contains(body, "response.completed") || strings.Contains(body, `"status":"completed"`) {
						t.Fatalf("empty search reply must fail explicitly, not complete a Codex turn: %d %s", w.Code, body)
					}
					if tries := s.trace.routes[len(s.trace.routes)-1].Tries; len(tries) != tc.sends {
						t.Fatalf("unrecorded model retries: %+v", tries)
					}
				})
			}
		}
	}
}

func TestWorkBuddySearchRepliesAndBounds(t *testing.T) {
	var mu sync.Mutex
	var upstream string
	sends := 0
	pid := workbuddyRawPlugin(t, "workbuddy", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		mu.Lock()
		sends++
		body := upstream
		mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, body)
	}))
	thought := sse(`data: {"id":"pending","choices":[{"index":0,"delta":{"reasoning_content":"Pending reasoning"}}]}`)
	chunk := sse(`data: {"id":"pending","choices":[{"index":0,"delta":{"reasoning_content":"` + strings.Repeat("r", 64<<10) + `"}}]}`)
	for _, tc := range []struct {
		name, model, body, want string
		sends                   int
		failed                  bool
	}{
		{"answer", "gpt-5.6-sol", workbuddyRawAnswerStream, "Verified result", 1, false},
		{"thought-then-answer", "gpt-5.6-sol", thought + workbuddyRawAnswerStream, "Pending reasoning", 1, false},
		{"client-tool", "gpt-5.6-sol", sse(
			`data: {"id":"tool","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"new-call","type":"function","function":{"name":"run","arguments":"{}"}}]}}]}`,
			`data: {"id":"tool","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`,
			`data: [DONE]`,
		), "new-call", 1, false},
		{"length", "gpt-5.6-sol", thought + strings.Replace(workbuddyRawEmptyStream, `"finish_reason":"stop"`, `"finish_reason":"length"`, 1), "max_output_tokens", 1, false},
		{"filter", "gpt-5.6-sol", strings.Replace(workbuddyRawEmptyStream, `"finish_reason":"stop"`, `"finish_reason":"content_filter"`, 1), "content_filter", 1, false},
		{"unknown-prefix", "gpt-5.6-sol", sse(`data: {"type":"internal_trace","payload":"not an answer"}`) + workbuddyRawEmptyStream, "empty reply", 11, true},
		{"counted-thought-only", "gpt-5.6-sol", thought + strings.ReplaceAll(workbuddyRawEmptyStream, `"prompt_tokens":0`, `"prompt_tokens":12`), "empty reply", 11, true},
		{"tagged-thought-only", "gpt-5.6-sol", strings.Replace(workbuddyRawEmptyStream, `"content":""`, `"content":"<think>Pending reasoning</think>"`, 1), "empty reply", 11, true},
		{"pending-bound", "gpt-5.6-sol", strings.Repeat(chunk, workbuddyPendingMost/(64<<10)+2) + workbuddyRawEmptyStream, "pending reply exceeded its buffer limit", 3, true},
		{"other-model-unchanged", "claude-opus-5.5", workbuddyRawEmptyStream, `"status":"completed"`, 1, false},
	} {
		for _, stream := range []bool{true, false} {
			t.Run(tc.name+map[bool]string{true: "/stream", false: "/whole"}[stream], func(t *testing.T) {
				mu.Lock()
				upstream, sends = tc.body, 0
				mu.Unlock()
				ask, err := json.Marshal(map[string]any{
					"model": pid + "/" + tc.model, "stream": stream, "input": "Check the saved result.",
					"prompt_cache_key": t.Name(),
					"tools":            []any{map[string]any{"type": "web_search"}, map[string]any{"type": "function", "name": "run", "parameters": map[string]any{"type": "object"}}},
				})
				if err != nil {
					t.Fatal(err)
				}
				r := httptest.NewRequest(http.MethodPost, CodexPath+"/responses", strings.NewReader(string(ask)))
				r.Header.Set("Authorization", "Bearer magpie-codex")
				w := httptest.NewRecorder()
				New().Handler().ServeHTTP(w, r)
				mu.Lock()
				got := sends
				mu.Unlock()
				body := w.Body.String()
				if got != tc.sends || !strings.Contains(body, tc.want) {
					t.Fatalf("wrong search reply: sends=%d want=%d status=%d bytes=%d; missing %q", got, tc.sends, w.Code, len(body), tc.want)
				}
				if tc.failed {
					if strings.Contains(body, "response.completed") || strings.Contains(body, `"status":"completed"`) || strings.Contains(body, "Pending reasoning") || w.Code == 200 && !strings.Contains(body, "response.failed") {
						t.Fatalf("failed pending search reply succeeded or released reasoning: status=%d bytes=%d", w.Code, len(body))
					}
				} else if tc.name == "filter" {
					if w.Code != 400 || strings.Contains(body, "empty reply") {
						t.Fatalf("safety refusal changed: %d %s", w.Code, body)
					}
				} else if w.Code != 200 || strings.Contains(body, "response.failed") || strings.Contains(body, "empty reply") {
					t.Fatalf("valid reply was rejected: %d %s", w.Code, body)
				}
			})
		}
	}
}

func TestWorkBuddySearchEmptyAfterCompletedSearch(t *testing.T) {
	var mu sync.Mutex
	var bodies []string
	var searches int
	var upstream string
	pid := workbuddyRawPlugin(t, "workbuddy", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		if r.URL.Path == wbSearchPath {
			mu.Lock()
			searches++
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, `{"results":[{"title":"Saved result","url":"https://fixture.invalid/result","snippet":"verified locally"}]}`)
			return
		}
		if r.URL.Path != "/v1/chat/completions" {
			t.Errorf("unexpected upstream path %s", r.URL.Path)
			http.Error(w, "unexpected path", 400)
			return
		}
		mu.Lock()
		bodies = append(bodies, string(b))
		first := len(bodies) == 1
		body := upstream
		mu.Unlock()
		if !first && body == "" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadGateway)
			io.WriteString(w, workbuddyEmptyBody)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		if first {
			io.WriteString(w, sse(
				`data: {"id":"search","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"saved-search","type":"function","function":{"name":"web_search","arguments":"{\"query\":\"saved result\"}"}}]}}]}`,
				`data: {"id":"search","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`,
				`data: [DONE]`,
			))
		} else {
			io.WriteString(w, body)
		}
	}))
	for _, stream := range []bool{true, false} {
		for _, tc := range []struct{ name, body string }{
			{"raw-empty", strings.Replace(workbuddyRawEmptyStream, `"reasoning_content":""`, `"reasoning_content":"Pending reasoning"`, 1)},
			{"plugin-empty-error", ""},
		} {
			t.Run(tc.name+map[bool]string{true: "/stream", false: "/whole"}[stream], func(t *testing.T) {
				mu.Lock()
				bodies, searches, upstream = nil, 0, tc.body
				mu.Unlock()
				ask, err := json.Marshal(map[string]any{
					"model": pid + "/gpt-5.6-sol", "stream": stream, "input": "Check the saved result.",
					"prompt_cache_key": t.Name(), "tools": []any{map[string]any{"type": "web_search"}},
				})
				if err != nil {
					t.Fatal(err)
				}
				r := httptest.NewRequest(http.MethodPost, CodexPath+"/responses", strings.NewReader(string(ask)))
				r.Header.Set("Authorization", "Bearer magpie-codex")
				w := httptest.NewRecorder()
				New().Handler().ServeHTTP(w, r)
				mu.Lock()
				defer mu.Unlock()
				if searches != 1 || len(bodies) != 2 || !strings.Contains(bodies[1], "saved-search") || !strings.Contains(bodies[1], "verified locally") {
					t.Fatalf("completed search was lost or replayed: searches=%d model sends=%d", searches, len(bodies))
				}
				body := w.Body.String()
				if !strings.Contains(body, "empty reply") || strings.Contains(body, "response.completed") || !stream && strings.Contains(body, `"status":"completed"`) || strings.Contains(body, "Pending reasoning") || w.Code == 200 && !strings.Contains(body, "response.failed") {
					t.Fatalf("empty model round after search silently succeeded: %d %s", w.Code, body)
				}
			})
		}
	}
}

func TestWorkBuddySearchPendingCancellation(t *testing.T) {
	keepFast(t, 10*time.Millisecond, 20*time.Millisecond, time.Second)
	held, quiet, watch := keepHeldAfter, agentQuietMost, watchEvery
	keepHeldAfter, agentQuietMost, watchEvery = 20*time.Millisecond, 100*time.Millisecond, 5*time.Millisecond
	t.Cleanup(func() { keepHeldAfter, agentQuietMost, watchEvery = held, quiet, watch })
	stopped := make(chan struct{})
	var once sync.Once
	var mu sync.Mutex
	sends := 0
	pid := workbuddyRawPlugin(t, "workbuddy", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer once.Do(func() { close(stopped) })
		io.Copy(io.Discard, r.Body)
		mu.Lock()
		sends++
		mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, sse(`data: {"id":"pending","choices":[{"index":0,"delta":{"reasoning_content":"Pending reasoning"}}]}`))
		w.(http.Flusher).Flush()
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-r.Context().Done():
				return
			case <-tick.C:
				if _, err := io.WriteString(w, ": keepalive\n\n"); err != nil {
					return
				}
				w.(http.Flusher).Flush()
			}
		}
	}))
	server := httptest.NewServer(New().Handler())
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	q := `{"model":"` + pid + `/gpt-5.6-sol","stream":true,"input":"Check the saved result.","tools":[{"type":"web_search"}]}`
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+CodexPath+"/responses", strings.NewReader(q))
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("Authorization", "Bearer magpie-codex")
	res, err := http.DefaultClient.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var body strings.Builder
	pings := 0
	sc := bufio.NewScanner(res.Body)
	for sc.Scan() {
		line := sc.Text()
		body.WriteString(line + "\n")
		if strings.Contains(line, `"type":"response.in_progress"`) {
			pings++
			if pings == 3 {
				cancel()
				break
			}
		}
	}
	cancel()
	res.Body.Close()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("canceling pending reply did not close upstream")
	}
	mu.Lock()
	got := sends
	mu.Unlock()
	if got != 1 || pings != 3 || strings.Contains(body.String(), "Pending reasoning") || strings.Contains(body.String(), "response.completed") {
		t.Fatalf("pending cancellation lost keepalives, leaked reasoning, or retried: sends=%d pings=%d reply=%s", got, pings, body.String())
	}
}
