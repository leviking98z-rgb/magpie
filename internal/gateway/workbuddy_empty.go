package gateway

import "github.com/yetone/magpie/internal/provider"

// workbuddyAnswerRequired mirrors the plugin's GPT-5.6-Sol guard at the
// translation boundary. A raw Chat 200 can pass the plugin's conservative
// prefix check yet decode to no answer at all. Do not turn that into a
// successful Codex completion, regardless of usage or reasoning metadata.
// Other models retain their existing empty end-turn behavior.
func workbuddyAnswerRequired(p provider.Provider, proto provider.Protocol, model string) bool {
	return proto == provider.Chat && model == "gpt-5.6-sol" && isWorkBuddy(p)
}

// Keep pending reasoning bounded, as the WorkBuddy plugin does. Count a
// fixed allowance per event too, so empty accounting frames cannot grow the
// pending slice without limit. This limit is not a model's token budget.
const workbuddyPendingMost = 4 << 20

const workbuddyPendingReply = "WorkBuddy: the pending reply exceeded its buffer limit before an answer or tool call; retry the same model"

func emptyReplyMessage(p provider.Provider, proto provider.Protocol, model string) string {
	if workbuddyAnswerRequired(p, proto, model) {
		// The canonical vendor marker preserves the same-candidate retry
		// classification even when the user renamed their provider.
		return "WorkBuddy: " + emptyReply + "; upstream HTTP 200 contained no output (no answer or tool call)"
	}
	return p.Name + ": " + emptyReply
}
