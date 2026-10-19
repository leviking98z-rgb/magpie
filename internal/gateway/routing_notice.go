package gateway

import (
	"encoding/json"
	"strings"
)

const routingNoticeHeader = "X-Magpie-Routing-Notice"

// This is protocol metadata, never assistant text. Only a client that opts
// into v1 receives it; OpenAI-Model has unrelated safety-routing semantics
// in Codex and must not be used for ordinary gateway failover.
func routingNotice(asked, previous, next string) []byte {
	from := previous
	if from == "" {
		from = asked
	}
	b, _ := json.Marshal(map[string]any{
		"type": "response.magpie_routing",
		"routing": map[string]any{
			"from_model": from,
			"to_model":   next,
			"fallback":   previous != "" && previous != next,
		},
	})
	return append(append([]byte("event: response.magpie_routing\ndata: "), b...), '\n', '\n')
}

// Called under the hold writer's mutex, only as a successful stream is
// released. Failed candidates and keepalives never announce an answerer.
func (h *holdWriter) sendRoutingNotice() {
	if len(h.routingNotice) == 0 || h.status != 200 || h.failure != 0 || h.refused ||
		!strings.HasPrefix(h.header.Get("Content-Type"), "text/event-stream") {
		return
	}
	h.w.Write(h.routingNotice)
	h.sent(h.routingNotice)
	h.routingNotice = nil
}
