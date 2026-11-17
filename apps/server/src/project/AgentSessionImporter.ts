import {
  DEFAULT_MODEL,
  DEFAULT_MODEL_BY_PROVIDER,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  DEFAULT_RUNTIME_MODE,
  isRuntimeModeAbove,
  AgentSessionImportProjectChangedError,
  AgentSessionImportProjectNotFoundError,
  AgentSessionImportSource,
  AgentSessionScanError,
  AgentSessionSource,
  EventId,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  ProviderDriverKind,
  ThreadId,
  TurnItemId,
  type AgentSessionImportInput,
  type AgentSessionImportResult,
  type OrchestrationV2AppThread,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2TurnItem,
} from "@t3tools/contracts";
import { normalizeProjectPathForComparison } from "@t3tools/shared/path";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Schedule from "effect/Schedule";
import * as SqlClient from "effect/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- import IDs must remain stable across process restarts.
import * as NodeCrypto from "node:crypto";

import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import { isActiveRun } from "../orchestration-v2/ThreadManagementService.ts";
import * as ThreadCommandExecutor from "../orchestration-v2/ThreadCommandExecutor.ts";
import * as ProviderSessionRuntime from "../persistence/ProviderSessionRuntime.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as AgentSessionScanner from "./AgentSessionScanner.ts";
import * as ProjectService from "./ProjectService.ts";
import { missingClaudeMessages } from "./claudeHistorySync.ts";
import * as ClaudeDesktopSessionBridge from "./ClaudeDesktopSessionBridge.ts";

const IMPORT_EVENT_PREFIX = "agent-session-import:v2";
const CLAUDE_SESSION_ID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const decodeImportedTranscriptPayload = Schema.decodeUnknownOption(
  Schema.Struct({
    cwd: Schema.optional(Schema.String),
    importedTranscripts: Schema.optional(Schema.Array(AgentSessionImportSource)),
  }),
);

class AgentSessionUnresumableSessionError extends Schema.TaggedError<AgentSessionUnresumableSessionError>()(
  "AgentSessionUnresumableSessionError",
  {
    source: AgentSessionSource,
    providerSessionId: Schema.String,
  },
) {
  override get message(): string {
    return `Session '${this.providerSessionId}' from '${this.source}' cannot be resumed.`;
  }
}

class AgentSessionThreadProjectConflictError extends Schema.TaggedError<AgentSessionThreadProjectConflictError>()(
  "AgentSessionThreadProjectConflictError",
  {
    threadId: ThreadId,
    expectedProjectId: ProjectId,
    actualProjectId: ProjectId,
  },
) {
  override get message(): string {
    return `Imported thread '${this.threadId}' belongs to project '${this.actualProjectId}', not '${this.expectedProjectId}'.`;
  }
}

class AgentSessionThreadModifiedError extends Schema.TaggedError<AgentSessionThreadModifiedError>()(
  "AgentSessionThreadModifiedError",
  { threadId: ThreadId },
) {
  override get message(): string {
    return `Imported thread '${this.threadId}' already contains non-imported activity.`;
  }
}

function dateTime(value: string): DateTime.Utc {
  return DateTime.makeUnsafe(value);
}

function messageEvents(input: {
  readonly threadId: ThreadId;
  readonly index: number;
  readonly ordinal?: number;
  readonly syncKey?: string;
  readonly message: AgentSessionScanner.AgentSessionThreadMessage;
}): ReadonlyArray<OrchestrationV2DomainEvent> {
  const ordinal = input.ordinal ?? input.index + 1;
  const suffix = input.syncKey ?? String(input.index).padStart(6, "0");
  const messageId = MessageId.make(`${input.threadId}:${suffix}`);
  const turnItemId = TurnItemId.make(
    `${IMPORT_EVENT_PREFIX}:turn-item:${input.threadId}:${suffix}`,
  );
  const at = dateTime(input.message.createdAt);
  const message: OrchestrationV2ConversationMessage = {
    createdBy: input.message.role === "user" ? "user" : "agent",
    creationSource: "server",
    id: messageId,
    threadId: input.threadId,
    runId: null,
    nodeId: null,
    role: input.message.role,
    text: input.message.text,
    attachments: [],
    streaming: false,
    createdAt: at,
    updatedAt: at,
  };
  const common = {
    id: turnItemId,
    threadId: input.threadId,
    runId: null,
    nodeId: null,
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: null,
    parentItemId: null,
    ordinal,
    status: "completed" as const,
    title: null,
    startedAt: at,
    completedAt: at,
    updatedAt: at,
  };
  const turnItem: OrchestrationV2TurnItem =
    input.message.role === "user"
      ? {
          ...common,
          createdBy: "user",
          creationSource: "server",
          type: "user_message",
          messageId,
          inputIntent: "turn_start",
          text: input.message.text,
          attachments: [],
        }
      : {
          ...common,
          type: "assistant_message",
          messageId,
          text: input.message.text,
          streaming: false,
        };
  return [
    {
      id: EventId.make(`${IMPORT_EVENT_PREFIX}:message:${input.threadId}:${suffix}`),
      type: "message.updated",
      threadId: input.threadId,
      occurredAt: at,
      payload: message,
    },
    {
      id: EventId.make(`${IMPORT_EVENT_PREFIX}:turn-item:${input.threadId}:${suffix}`),
      type: "turn-item.updated",
      threadId: input.threadId,
      occurredAt: at,
      payload: turnItem,
    },
  ];
}

const make = Effect.gen(function* () {
  const scanner = yield* AgentSessionScanner.AgentSessionScanner;
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const projects = yield* ProjectService.ProjectService;
  const eventSink = yield* EventSink.EventSinkV2;
  const idAllocator = yield* IdAllocator.IdAllocatorV2;
  const runtimes = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
  const threadCommands = yield* ThreadCommandExecutor.ThreadCommandExecutor;
  const settingsService = yield* ServerSettings.ServerSettingsService;
  const sql = yield* SqlClient.SqlClient;
  const ceiling = settingsService.managedPolicy?.limits?.maxRuntimeMode;
  const importRecentAgentThreads = Effect.fn("importRecentAgentThreadsV2")(function* (
    input: AgentSessionImportInput & {
      readonly claudeHistorySyncOnly?: boolean;
      readonly forceClaudeHistoryRefresh?: boolean;
    },
  ) {
    const project = yield* projects.getById(input.projectId).pipe(
      Effect.mapError((cause) => new AgentSessionScanError({ operation: "read-projects", cause })),
      Effect.flatMap(
        Option.match({
          onNone: () =>
            Effect.fail(new AgentSessionImportProjectNotFoundError({ projectId: input.projectId })),
          onSome: Effect.succeed,
        }),
      ),
    );
    if (
      input.expectedWorkspaceRoot !== undefined &&
      normalizeProjectPathForComparison(project.workspaceRoot) !==
        normalizeProjectPathForComparison(input.expectedWorkspaceRoot)
    ) {
      return yield* new AgentSessionImportProjectChangedError({ projectId: input.projectId });
    }
    // Imports run in full access, except under a policy ceiling: then in the project's default,
    // which the policy keeps under it, so imported threads never sit above the ceiling.
    const runtimeMode =
      ceiling === undefined
        ? DEFAULT_RUNTIME_MODE
        : yield* settingsService.getSettings.pipe(
            Effect.mapError(
              (cause) => new AgentSessionScanError({ operation: "read-settings", cause }),
            ),
            Effect.map((settings) => {
              const mode = resolveProjectSettings(settings, input.projectId, project).settings
                .defaultRuntimeMode;
              return isRuntimeModeAbove(mode, ceiling) ? ceiling : mode;
            }),
          );
    const runtimeRows = yield* runtimes
      .list()
      .pipe(
        Effect.mapError(
          (cause) => new AgentSessionScanError({ operation: "read-projects", cause }),
        ),
      );
    const completedSources = runtimeRows.flatMap((runtime) => {
      const payload = decodeImportedTranscriptPayload(runtime.runtimePayload);
      if (
        Option.isNone(payload) ||
        payload.value.cwd === undefined ||
        normalizeProjectPathForComparison(payload.value.cwd) !==
          normalizeProjectPathForComparison(project.workspaceRoot)
      ) {
        return [];
      }
      return payload.value.importedTranscripts ?? [];
    });
    const outcomes = scanner.recentThreads(
      project.workspaceRoot,
      input.forceClaudeHistoryRefresh
        ? completedSources.filter((source) => source.provider !== "claudeAgent")
        : completedSources,
    );
    const importedThreadIds = new Set<ThreadId>();
    let importedCount = 0;
    let skippedCount = 0;

    yield* Stream.runForEach(outcomes, (outcome) =>
      Effect.gen(function* () {
        if (outcome._tag === "Skipped") {
          skippedCount += 1;
          return;
        }
        const source = outcome.source;
        if (input.claudeHistorySyncOnly && source.provider !== "claudeAgent") return;
        let threadId = ThreadId.make(
          `import:${source.providerInstanceId}:${source.providerSessionId}`,
        );
        if (outcome._tag === "AlreadyImported") {
          importedThreadIds.add(threadId);
          importedCount += 1;
          return;
        }
        if (outcome._tag === "Duplicate") {
          if (importedThreadIds.has(threadId)) {
            yield* runtimes.recordImportedTranscript({ threadId, source }).pipe(Effect.ignore);
          }
          return;
        }

        const imported = yield* Effect.gen(function* () {
          const thread = outcome.thread;
          const ownerInstances = [
            ...new Set([
              thread.providerInstanceId,
              ...(thread.source === "claudeAgent"
                ? yield* scanner.claudeInstancesForTranscript(source.filePath)
                : []),
            ]),
          ];
          const owners =
            thread.source === "claudeAgent" && ownerInstances.length > 0
              ? yield* sql<{ readonly thread_id: string; readonly provider_instance_id: string }>`
                SELECT DISTINCT thread_id, provider_instance_id
                FROM orchestration_v2_projection_provider_threads
                WHERE driver = 'claudeAgent'
                  AND provider_instance_id IN ${sql.in(ownerInstances)}
                  AND json_extract(payload_json, '$.nativeThreadRef.nativeId') = ${thread.providerSessionId}
                ORDER BY (thread_id LIKE 'import:%') ASC, thread_id ASC
              `.pipe(NodeSqliteClient.readOnly)
              : [];
          const owner = owners[0];
          if (owner) threadId = ThreadId.make(owner.thread_id);
          if (
            thread.source === "claudeAgent" &&
            !CLAUDE_SESSION_ID_PATTERN.test(thread.providerSessionId)
          ) {
            return yield* new AgentSessionUnresumableSessionError({
              source: thread.source,
              providerSessionId: thread.providerSessionId,
            });
          }
          const driver = ProviderDriverKind.make(thread.source);
          // Pi resumes by session file path; its session UUID only names the thread.
          const nativeThreadId =
            thread.source === "pi" ? source.filePath : thread.providerSessionId;
          const persistRuntime = runtimes.upsert(
            {
              threadId,
              providerName: driver,
              providerInstanceId: owner
                ? ProviderInstanceId.make(owner.provider_instance_id)
                : thread.providerInstanceId,
              adapterKey: driver,
              runtimeMode,
              status: "stopped",
              lastSeenAt: thread.updatedAt,
              resumeCursor:
                thread.source === "claudeAgent"
                  ? { threadId, resume: thread.providerSessionId }
                  : { threadId: nativeThreadId },
              runtimePayload: { cwd: project.workspaceRoot },
            },
            { onConflict: "ignore" },
          );
          const existing = yield* Effect.option(orchestrator.getThreadRecords(threadId, []));
          if (Option.isSome(existing)) {
            if (
              existing.value.thread.deletedAt !== null ||
              (existing.value.thread.archivedAt !== null &&
                existing.value.thread.historyOrigin !== "v1_import")
            )
              return null;
            if (existing.value.thread.projectId !== input.projectId) {
              return yield* new AgentSessionThreadProjectConflictError({
                threadId,
                expectedProjectId: input.projectId,
                actualProjectId: existing.value.thread.projectId,
              });
            }
            if (
              existing.value.thread.historyOrigin !== "v1_import" &&
              thread.source !== "claudeAgent"
            ) {
              return yield* new AgentSessionThreadModifiedError({ threadId });
            }
            if (thread.source === "claudeAgent") {
              return yield* threadCommands.withLock(
                threadId,
                Effect.gen(function* () {
                  const current = yield* orchestrator.getThreadRecords(threadId, [
                    "messages",
                    "runs",
                    "turnItems",
                    "providerThreads",
                  ]);
                  if (current.thread.deletedAt !== null || current.runs.some(isActiveRun))
                    return null;
                  const missing = missingClaudeMessages(
                    thread.messages,
                    current.messages.map((message) => ({
                      ...message,
                      createdAt: DateTime.formatIso(message.createdAt),
                    })),
                    current.runs.map((run) => ({
                      ...run,
                      requestedAt: DateTime.formatIso(run.requestedAt),
                      completedAt:
                        run.completedAt === null ? null : DateTime.formatIso(run.completedAt),
                    })),
                  );
                  if (missing.length > 0) {
                    const nextOrdinal = current.turnItems.reduce(
                      (next, item) =>
                        item.runId === null ? Math.max(next, item.ordinal + 1) : next,
                      1,
                    );
                    const events = missing.flatMap((message, index) =>
                      messageEvents({
                        threadId,
                        index,
                        ordinal: nextOrdinal + index,
                        syncKey:
                          "claude-sync:" +
                          NodeCrypto.createHash("sha256")
                            .update(JSON.stringify([message.role, message.createdAt, message.text]))
                            .digest("hex"),
                        message,
                      }),
                    );
                    const at = yield* DateTime.now;
                    const newest = missing.reduce(
                      (latest, message) => Math.max(latest, Date.parse(message.createdAt)),
                      0,
                    );
                    const lastT3Run = current.runs.reduce(
                      (latest, run) =>
                        run.completedAt === null
                          ? latest
                          : Math.max(latest, DateTime.toEpochMillis(run.completedAt)),
                      0,
                    );
                    const nativeProvider = current.providerThreads.find(
                      (provider) =>
                        provider.driver === "claudeAgent" &&
                        provider.nativeThreadRef?.nativeId === thread.providerSessionId,
                    );
                    if (nativeProvider && newest > lastT3Run) {
                      events.push({
                        id: EventId.make("claude-sync:head:" + NodeCrypto.randomUUID()),
                        type: "provider-thread.updated",
                        threadId,
                        driver: ProviderDriverKind.make("claudeAgent"),
                        providerInstanceId: nativeProvider.providerInstanceId,
                        occurredAt: at,
                        payload: {
                          ...nativeProvider,
                          nativeConversationHeadRef: null,
                          nativeMetadata: {
                            ...nativeProvider.nativeMetadata,
                            updatedAt: DateTime.formatIso(DateTime.makeUnsafe(newest)),
                          },
                          updatedAt: at,
                        },
                      });
                    }
                    events.push({
                      id: EventId.make("claude-sync:thread:" + NodeCrypto.randomUUID()),
                      type: "thread.metadata-updated",
                      threadId,
                      occurredAt: at,
                      payload: { ...current.thread, historyOrigin: "v1_import", updatedAt: at },
                    });
                    yield* eventSink.write({ events });
                    yield* Effect.logInfo("Claude external history synchronized", {
                      threadId,
                      addedMessageCount: missing.length,
                    });
                  }
                  yield* persistRuntime;
                  yield* runtimes.recordImportedTranscript({ threadId, source });
                  for (const duplicate of owners.slice(1)) {
                    const duplicateId = ThreadId.make(duplicate.thread_id);
                    if (!duplicateId.startsWith("import:")) continue;
                    yield* threadCommands.withLock(
                      duplicateId,
                      Effect.gen(function* () {
                        const snapshot = yield* orchestrator.getThreadRecords(duplicateId, [
                          "runs",
                          "messages",
                        ]);
                        if (
                          snapshot.thread.projectId !== input.projectId ||
                          snapshot.thread.deletedAt !== null ||
                          snapshot.thread.archivedAt !== null ||
                          snapshot.runs.length > 0
                        )
                          return;
                        const uncovered = missingClaudeMessages(
                          snapshot.messages.flatMap((message) =>
                            message.role === "user" || message.role === "assistant"
                              ? [
                                  {
                                    role: message.role,
                                    text: message.text,
                                    createdAt: DateTime.formatIso(message.createdAt),
                                  },
                                ]
                              : [],
                          ),
                          thread.messages.map((message) => ({ ...message, runId: null })),
                          [],
                        );
                        if (uncovered.length > 0) return;
                        const at = yield* DateTime.now;
                        yield* eventSink.write({
                          events: [
                            {
                              id: EventId.make("claude-sync:duplicate:" + NodeCrypto.randomUUID()),
                              type: "thread.metadata-updated",
                              threadId: duplicateId,
                              occurredAt: at,
                              payload: { ...snapshot.thread, archivedAt: at, updatedAt: at },
                            },
                          ],
                        });
                      }),
                    );
                  }
                  return true;
                }),
              );
            }
            yield* persistRuntime;
            yield* runtimes.recordImportedTranscript({ threadId, source });
            return true;
          }

          const model = thread.model ?? DEFAULT_MODEL_BY_PROVIDER[driver] ?? DEFAULT_MODEL;
          const providerThreadId = idAllocator.derive.providerThread({
            driver,
            providerInstanceId: thread.providerInstanceId,
            nativeThreadId,
          });
          const createdAt = dateTime(thread.createdAt);
          const updatedAt = dateTime(thread.updatedAt);
          const appThread: OrchestrationV2AppThread = {
            createdBy: "system",
            creationSource: "server",
            id: threadId,
            projectId: input.projectId,
            title: thread.title.trim() === "" ? "Untitled thread" : thread.title,
            providerInstanceId: thread.providerInstanceId,
            modelSelection: { instanceId: thread.providerInstanceId, model },
            runtimeMode,
            interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
            branch: null,
            worktreePath: null,
            linkedPullRequest: null,
            branchPullRequest: null,
            activeProviderThreadId: providerThreadId,
            historyOrigin: "v1_import",
            lineage: {
              parentThreadId: null,
              relationshipToParent: null,
              rootThreadId: threadId,
            },
            forkedFrom: null,
            createdAt,
            updatedAt,
            archivedAt: null,
            settledOverride: "settled",
            settledAt: updatedAt,
            unsettledAt: null,
            snoozedUntil: null,
            snoozedAt: null,
            pinnedAt: null,
            pinOrderKey: null,
            activeOrderKey: null,
            lastVisitedAt: null,
            deletedAt: null,
          };
          const providerThread: OrchestrationV2ProviderThread = {
            id: providerThreadId,
            driver,
            providerInstanceId: thread.providerInstanceId,
            providerSessionId: null,
            appThreadId: threadId,
            ownerNodeId: null,
            nativeThreadRef: {
              driver,
              nativeId: nativeThreadId,
              strength: "strong",
            },
            nativeConversationHeadRef: null,
            status: "idle",
            firstRunOrdinal: null,
            lastRunOrdinal: null,
            handoffIds: [],
            forkedFrom: null,
            pendingBackgroundTasks: [],
            createdAt,
            updatedAt,
          };

          const committed = yield* eventSink.write({
            unlessNativeThreadOwned: providerThread,
            nativeThreadOwnerInstanceIds: ownerInstances,
            events: [
              {
                id: EventId.make(`${IMPORT_EVENT_PREFIX}:thread:${threadId}:created`),
                type: "thread.created",
                threadId,
                providerInstanceId: thread.providerInstanceId,
                occurredAt: createdAt,
                payload: appThread,
              },
              ...(thread.omittedMessageCount > 0
                ? [
                    {
                      id: EventId.make(`${IMPORT_EVENT_PREFIX}:history-truncated:${threadId}`),
                      type: "turn-item.updated" as const,
                      threadId,
                      occurredAt: createdAt,
                      payload: {
                        id: TurnItemId.make(`${IMPORT_EVENT_PREFIX}:history-truncated:${threadId}`),
                        threadId,
                        runId: null,
                        nodeId: null,
                        providerThreadId: null,
                        providerTurnId: null,
                        nativeItemRef: null,
                        parentItemId: null,
                        ordinal: 0,
                        status: "completed" as const,
                        title: null,
                        startedAt: createdAt,
                        completedAt: createdAt,
                        updatedAt: createdAt,
                        type: "system_notice" as const,
                        message: `${thread.omittedMessageCount} earlier ${thread.omittedMessageCount === 1 ? "message was" : "messages were"} not imported. This thread keeps the first user message and the most recent history. The original session transcript is unchanged.`,
                      },
                    },
                  ]
                : []),
              ...thread.messages.flatMap((message, index) =>
                messageEvents({ threadId, index, message }),
              ),
              {
                id: EventId.make(`${IMPORT_EVENT_PREFIX}:provider-thread:${providerThreadId}`),
                type: "provider-thread.updated",
                threadId,
                driver,
                providerInstanceId: thread.providerInstanceId,
                occurredAt: updatedAt,
                payload: providerThread,
              },
            ],
          });
          if (committed.length === 0) return null;
          yield* persistRuntime;
          yield* runtimes.recordImportedTranscript({ threadId, source });
          return true;
        }).pipe(
          Effect.catch((cause) =>
            Effect.logWarning("Could not import an agent session", {
              provider: outcome.thread.source,
              sessionId: outcome.thread.providerSessionId,
              cause,
            }).pipe(Effect.as(false)),
          ),
        );
        if (imported === true) {
          importedThreadIds.add(threadId);
          importedCount += 1;
        } else if (imported === false) {
          skippedCount += 1;
        }
      }),
    );

    return { importedCount, skippedCount } satisfies AgentSessionImportResult;
  });

  let firstSweep = true;
  let sweepCount = 0;
  const syncClaudeHistory = Effect.fn("AgentSessionImporter.syncClaudeHistory")(function* () {
    if (sweepCount++ % 4 === 0) yield* scanner.scan;
    const forceClaudeHistoryRefresh = firstSweep;
    firstSweep = false;
    for (const project of yield* projects.listShells()) {
      yield* importRecentAgentThreads({
        projectId: project.id,
        claudeHistorySyncOnly: true,
        forceClaudeHistoryRefresh,
      }).pipe(
        Effect.catch((cause) =>
          Effect.logWarning("Claude history sync deferred", { projectId: project.id, cause }),
        ),
      );
    }
  });
  return { importRecentAgentThreads, syncClaudeHistory };
});

type AgentSessionImporterShape = Effect.Success<typeof make>;

export class AgentSessionImporter extends Context.Service<
  AgentSessionImporter,
  AgentSessionImporterShape
>()("t3/project/AgentSessionImporter") {}

export const layer = Layer.effect(AgentSessionImporter, make);

export const layerWithClaudeHistorySync = Layer.effectDiscard(
  Effect.gen(function* () {
    const importer = yield* AgentSessionImporter;
    const desktop = yield* ClaudeDesktopSessionBridge.ClaudeDesktopSessionBridge;
    yield* importer.syncClaudeHistory().pipe(
      Effect.andThen(desktop.sync),
      Effect.catch((cause) => Effect.logWarning("Claude history sync failed", { cause })),
      Effect.repeat(Schedule.spaced("20 seconds")),
      Effect.delay("5 seconds"),
      Effect.forkScoped,
    );
  }),
).pipe(Layer.provideMerge(layer));
