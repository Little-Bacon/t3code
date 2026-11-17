import { expect, it } from "vite-plus/test";
import * as DateTime from "effect/DateTime";
import { missingClaudeMessages } from "./claudeHistorySync.ts";
import type { AgentSessionThreadMessage } from "./AgentSessionScanner.ts";

const message = (
  role: AgentSessionThreadMessage["role"],
  text: string,
  second: number,
  runId: string | null = null,
) => ({
  role,
  text,
  createdAt: DateTime.formatIso(
    DateTime.makeUnsafe(Date.parse("2026-09-01T10:00:00Z") + second * 1000),
  ),
  runId,
});

it("keeps repeated prompts at different times and leaves stored messages intact", () => {
  const stored = [message("user", "again", 1), message("assistant", "T3 only", 5)];
  const before = structuredClone(stored);
  const repeated = message("user", "again", 60);
  expect(missingClaudeMessages([stored[0]!, repeated], stored, [])).toEqual([repeated]);
  expect(stored).toEqual(before);
});

it("does not duplicate native blocks already coalesced by a completed T3 run", () => {
  const stored = [
    message("user", "hello", 100, "run"),
    message("assistant", "first\nsecond", 110, "run"),
  ];
  const external = message("user", "external", 200);
  expect(
    missingClaudeMessages(
      [
        message("user", "hello", 101),
        message("assistant", "first", 105),
        message("assistant", "second", 109),
        external,
      ],
      stored,
      [
        {
          id: "run",
          status: "completed",
          requestedAt: message("user", "", 99).createdAt,
          completedAt: message("user", "", 112).createdAt,
        },
      ],
    ),
  ).toEqual([external]);
});

it("keeps external messages when an overlapping T3 run failed", () => {
  const native = [message("user", "external", 110), message("assistant", "answer", 120)];
  expect(
    missingClaudeMessages(
      native,
      [],
      [
        {
          id: "run",
          status: "failed",
          requestedAt: message("user", "", 100).createdAt,
          completedAt: message("user", "", 130).createdAt,
        },
      ],
    ),
  ).toEqual(native);
});

it("recognizes a T3 handoff without truncating delimiters in the original question", () => {
  const stored = message("user", "question\n\nUser message:\nexample", 100, "run");
  const wrapped = message(
    "user",
    `Context handoff (manual_context):\nProvider context handoff. Thread: test.\nHistory.\n\nUser message:\n${stored.text}`,
    101,
  );
  expect(missingClaudeMessages([wrapped], [stored], [])).toEqual([]);
});
