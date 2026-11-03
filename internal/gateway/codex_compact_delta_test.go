package gateway

import (
	"strings"
	"testing"

	"github.com/yetone/magpie/internal/provider"
)

// A Responses relay can stream the summary as output_text deltas and leave
// both output_item.done and response.completed's output out. The completed
// stream still wrote a summary, so Codex gets that text as its compaction.
func TestCodexCompactUsesTextOnlyStream(t *testing.T) {
	f := &fake{t: t, reply: sse(
		`data: {"type":"response.created","response":{"id":"resp_delta","status":"in_progress","output":[]}}`,
		`data: {"type":"response.reasoning_summary_text.delta","delta":"not the summary"}`,
		`data: {"type":"response.output_text.delta","delta":"SUMM"}`,
		`data: {"type":"response.output_text.delta","delta":"ARY"}`,
		`data: {"type":"response.completed","response":{"id":"resp_delta","status":"completed","output":[],"usage":{"input_tokens":9,"output_tokens":2}}}`)}
	setup(t, provider.Responses, f)

	code, body := codexPost(t, compact404Request)
	if code != 200 {
		t.Fatalf("%d %s", code, body)
	}
	if got := compactedText(t, body); got != "SUMMARY" {
		t.Fatalf("summary %q, want output_text deltas only", got)
	}
}

// Deltas are only the fallback for a completed stream that has no message
// text. A full item wins, so a normal stream is not duplicated; reasoning
// and metadata alone are still not an answer.
func TestCompactReplyTextDeltaFallback(t *testing.T) {
	for _, tc := range []struct {
		name, reply, want string
	}{
		{
			name: "full item is not duplicated",
			reply: sse(
				`data: {"type":"response.output_text.delta","delta":"SUMMARY"}`,
				`data: {"type":"response.output_item.done","output_index":0,"item":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"SUMMARY"}]}}`,
				`data: {"type":"response.completed","response":{"id":"resp_item","status":"completed","output":[]}}`),
			want: "SUMMARY",
		},
		{
			name: "completed output is not duplicated",
			reply: sse(
				`data: {"type":"response.output_text.delta","delta":"SUMMARY"}`,
				`data: {"type":"response.completed","response":{"id":"resp_whole","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"SUMMARY"}]}]}}`),
			want: "SUMMARY",
		},
		{
			name: "empty message uses deltas",
			reply: sse(
				`data: {"type":"response.output_text.delta","delta":"SUMMARY"}`,
				`data: {"type":"response.output_item.done","output_index":0,"item":{"type":"message","role":"assistant","content":[]}}`,
				`data: {"type":"response.completed","response":{"id":"resp_empty","status":"completed","output":[]}}`),
			want: "SUMMARY",
		},
		{
			name: "reasoning is not a summary",
			reply: sse(
				`data: {"type":"response.reasoning_summary_text.delta","delta":"thinking"}`,
				`data: {"type":"response.output_item.done","output_index":0,"item":{"type":"reasoning","summary":[{"type":"summary_text","text":"thinking"}]}}`,
				`data: {"type":"response.completed","response":{"id":"resp_reasoning","status":"completed","output":[]}}`),
		},
		{
			name: "metadata is not a summary",
			reply: sse(
				`data: {"type":"response.created","response":{"id":"resp_empty","status":"in_progress","output":[]}}`,
				`data: {"type":"response.completed","response":{"id":"resp_empty","status":"completed","output":[]}}`),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, err := compactReply([]byte(tc.reply))
			if err != nil {
				t.Fatal(err)
			}
			if res.Status != "completed" {
				t.Fatalf("status %q", res.Status)
			}
			if got := strings.TrimSpace(messageText(res)); got != tc.want {
				t.Fatalf("message text %q, want %q", got, tc.want)
			}
		})
	}
}
