import { assert, it } from "@effect/vitest";
import { ProviderDriverKind } from "@t3tools/contracts";

import { shouldPrepareLegacyImportHandoff } from "./Orchestrator.ts";

it("reissues imported context until a V2 run completes", () => {
  assert.isTrue(
    shouldPrepareLegacyImportHandoff({
      historyOrigin: "v1_import",
      hasCompletedRun: false,
      legacyImportItemCount: 2,
    }),
  );
  assert.isFalse(
    shouldPrepareLegacyImportHandoff({
      historyOrigin: "v1_import",
      hasCompletedRun: true,
      legacyImportItemCount: 2,
    }),
  );
  assert.isFalse(
    shouldPrepareLegacyImportHandoff({
      historyOrigin: undefined,
      hasCompletedRun: false,
      legacyImportItemCount: 2,
    }),
  );
  assert.isFalse(
    shouldPrepareLegacyImportHandoff({
      historyOrigin: "v1_import",
      hasCompletedRun: false,
      legacyImportItemCount: 0,
    }),
  );
});

it("resumes an imported Claude conversation without replaying its transcript", () => {
  assert.isFalse(
    shouldPrepareLegacyImportHandoff({
      historyOrigin: "v1_import",
      hasCompletedRun: false,
      legacyImportItemCount: 27,
      nativeThreadRef: {
        driver: ProviderDriverKind.make("claudeAgent"),
        strength: "strong",
        nativeId: "claude:existing-session",
      },
    }),
  );
});

it("preserves imported context when Claude needs a new native conversation", () => {
  for (const nativeThreadRef of [
    null,
    { driver: ProviderDriverKind.make("claudeAgent"), strength: "none" as const, nativeId: null },
    {
      driver: ProviderDriverKind.make("codex"),
      strength: "strong" as const,
      nativeId: "codex:old",
    },
  ]) {
    assert.isTrue(
      shouldPrepareLegacyImportHandoff({
        historyOrigin: "v1_import",
        hasCompletedRun: false,
        legacyImportItemCount: 27,
        nativeThreadRef,
      }),
    );
  }
});
