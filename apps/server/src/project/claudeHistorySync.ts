import type { AgentSessionThreadMessage } from "./AgentSessionScanner.ts";
import type { OrchestrationV2ConversationMessage } from "@t3tools/contracts";

interface StoredMessage extends Omit<AgentSessionThreadMessage, "role"> {
  readonly role: OrchestrationV2ConversationMessage["role"];
  readonly runId: string | null;
}

interface StoredRun {
  readonly id: string;
  readonly status: string;
  readonly requestedAt: string;
  readonly completedAt: string | null;
}

export function missingClaudeMessages(
  native: ReadonlyArray<AgentSessionThreadMessage>,
  stored: ReadonlyArray<StoredMessage>,
  runs: ReadonlyArray<StoredRun>,
): ReadonlyArray<AgentSessionThreadMessage> {
  const normalize = (text: string) => text.replace(/\r\n/g, "\n").trim();
  const key = (message: { readonly role: string; readonly text: string }) =>
    JSON.stringify([message.role, normalize(message.text)]);
  const byText = new Map<string, Array<{ message: StoredMessage; index: number }>>();
  stored.forEach((message, index) => {
    const bucket = byText.get(key(message)) ?? [];
    bucket.push({ message, index });
    byText.set(key(message), bucket);
  });
  const used = new Set<number>();
  return native
    .map((message) => {
      if (
        message.role === "user" &&
        /^Context handoff \([^\n]+\):\nProvider context handoff\. Thread: /.test(message.text)
      ) {
        const original = stored.find(
          (candidate) =>
            candidate.role === "user" &&
            message.text.endsWith(`\n\nUser message:\n${candidate.text}`),
        );
        if (original) return { ...message, text: original.text };
      }
      return message;
    })
    .filter((message) => {
      const time = Date.parse(message.createdAt);
      const text = normalize(message.text);
      const match = byText
        .get(key(message))
        ?.find(
          (candidate) =>
            !used.has(candidate.index) &&
            Math.abs(Date.parse(candidate.message.createdAt) - time) <=
              (candidate.message.runId === null ? 0 : 10000),
        );
      if (match) {
        used.add(match.index);
        return false;
      }
      // T3 can coalesce several native assistant messages into one completed run.
      return !runs.some(
        (run) =>
          run.status === "completed" &&
          run.completedAt !== null &&
          time >= Date.parse(run.requestedAt) &&
          time <= Date.parse(run.completedAt) &&
          stored.some(
            (candidate) =>
              candidate.runId === run.id &&
              candidate.role === message.role &&
              normalize(candidate.text).includes(text),
          ),
      );
    });
}
