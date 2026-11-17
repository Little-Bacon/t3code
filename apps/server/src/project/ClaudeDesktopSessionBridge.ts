import { ProviderInstanceId } from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import * as ProcessRunner from "../processRunner.ts";
import { ACTIVE_RUN_STATUSES } from "../orchestration-v2/ThreadManagementService.ts";
import * as AgentSessionScanner from "./AgentSessionScanner.ts";

const sessionIdPattern =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const decodeRecord = Schema.decodeUnknownOption(
  Schema.fromJsonString(
    Schema.Struct({
      cliSessionId: Schema.optional(Schema.String),
      unarchivedCliSessionId: Schema.optional(Schema.String),
      priorCliSessionIds: Schema.optional(Schema.Array(Schema.String)),
    }),
  ),
);

export const make = Effect.gen(function* () {
  const environment = yield* HostProcess.Environment;
  const platform = yield* HostProcess.Platform;
  const directory = environment.T3CODE_CLAUDE_DESKTOP_SESSIONS_DIR?.trim();
  if (platform !== "darwin" || !directory) return { sync: Effect.void };
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const sql = yield* SqlClient.SqlClient;
  const scanner = yield* AgentSessionScanner.AgentSessionScanner;
  const runner = yield* ProcessRunner.ProcessRunner;
  const home = yield* HostProcess.HomeDirectory;
  const configHome = environment.CLAUDE_CONFIG_DIR?.trim() || path.join(home, ".claude");
  const attempts = new Map<string, number>();

  const sync = Effect.gen(function* () {
    if (!path.isAbsolute(directory)) return;
    // An explicit profile directory avoids importing into a different desktop account.
    const entries = yield* fs.readDirectory(directory);
    if (entries.length > 10000) return;
    const known = new Set<string>();
    for (const name of entries) {
      if (!/^local_[a-zA-Z0-9_-]+\.json$/.test(name)) continue;
      const file = path.join(directory, name);
      const stat = yield* fs.lstat(file);
      if (stat.type !== "File" || Number(stat.size) > 1024 * 1024) return;
      const record = decodeRecord(yield* fs.readFileString(file));
      if (Option.isNone(record)) return;
      for (const id of [
        record.value.cliSessionId,
        record.value.unarchivedCliSessionId,
        ...(record.value.priorCliSessionIds ?? []),
      ]) {
        if (id) known.add(id);
      }
    }
    // Only idle local root sessions created in T3 are announced, leaving imported,
    // archived and running conversations under their current client's control.
    const sessions = yield* sql<{
      readonly native_id: string;
      readonly provider_instance_id: string;
      readonly cwd: string;
    }>`
      SELECT DISTINCT json_extract(p.payload_json, '$.nativeThreadRef.nativeId') AS native_id,
        p.provider_instance_id, COALESCE(json_extract(t.payload_json, '$.worktreePath'), j.workspace_root) AS cwd
      FROM orchestration_v2_projection_provider_threads p
      JOIN orchestration_v2_projection_threads t ON t.thread_id = p.thread_id
      JOIN projection_projects j ON j.project_id = t.project_id
      WHERE p.driver = 'claudeAgent' AND p.owner_node_id IS NULL
        AND t.active_provider_thread_id = p.provider_thread_id
        AND t.thread_id NOT LIKE 'import:%' AND t.deleted_at IS NULL AND t.archived_at IS NULL
        AND json_extract(p.payload_json, '$.nativeThreadRef.nativeId') IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM orchestration_v2_projection_runs r
          WHERE r.thread_id = t.thread_id AND r.status IN ${sql.in(ACTIVE_RUN_STATUSES)})
      ORDER BY native_id
      LIMIT 1000
    `.pipe(NodeSqliteClient.readOnly);
    for (const session of sessions) {
      const id = session.native_id;
      if (!sessionIdPattern.test(id) || known.has(id) || (attempts.get(id) ?? 0) >= 3) continue;
      const file = path.join(
        configHome,
        "projects",
        session.cwd.replace(/[^a-zA-Z0-9]/g, "-"),
        `${id}.jsonl`,
      );
      if (!(yield* fs.exists(file))) continue;
      const instances = yield* scanner.claudeInstancesForTranscript(file);
      if (!instances.includes(ProviderInstanceId.make(session.provider_instance_id))) continue;
      attempts.set(id, (attempts.get(id) ?? 0) + 1);
      const result = yield* runner.run({
        command: "/usr/bin/open",
        args: ["-g", `claude://resume?session=${id}`],
        timeout: "5 seconds",
        maxOutputBytes: 4096,
      });
      if (result.code === 0) {
        yield* Effect.logInfo("Claude desktop session import requested", { nativeSessionId: id });
      }
    }
  }).pipe(
    Effect.catch((cause) => Effect.logWarning("Claude desktop session sync deferred", { cause })),
  );
  return { sync };
});

export class ClaudeDesktopSessionBridge extends Context.Service<
  ClaudeDesktopSessionBridge,
  Effect.Success<typeof make>
>()("t3/project/ClaudeDesktopSessionBridge") {}

export const layer = Layer.effect(ClaudeDesktopSessionBridge, make).pipe(
  Layer.provide(ProcessRunner.layer),
);
