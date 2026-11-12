#!/usr/bin/env node
// @effect-diagnostics nodeBuiltinImport:off - a standalone benchmark reads the WAL with synchronous Node I/O.
// @effect-diagnostics globalConsole:off - standalone benchmark output.
// @effect-diagnostics globalDate:off globalFetch:off globalTimers:off - a driver script outside the server's runtime.
//
// SQLite write cost per orchestration event, through the server's own write path.
//
//   record  --live <statev2.sqlite> --base <snapshot.sqlite> --window <window.sqlite> [--limit N]
//   prepare --base <snapshot.sqlite>
//   run     --base <snapshot.sqlite> --window <window.sqlite> [--runs 3] [--events N] [--out <dir>]
//           [--reallocate-event-ids]
//   live    --home <seeded T3 home> --project <git repo> [--model claude-haiku-5-5] [--out <dir>]
//
// `base` is a `VACUUM INTO` copy of a server database; `record` reads, read-only, the events the
// live database appended after it. `prepare` migrates the base once with the server's setup.
// Each `run` replays the window, one EventSink write per commit, against the base in WAL mode
// with checkpoints off, so the base file never changes and a run costs only its WAL. `live` runs
// one real provider turn on this checkout's server, counts its WAL, and replays the same events.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import * as NodeURL from "node:url";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { CommandId, ProviderSessionId, ThreadId } from "@t3tools/contracts";
import type { OrchestrationV2StoredEvent } from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";

import * as EventSink from "../src/orchestration-v2/EventSink.ts";
import { layerEventSink } from "../src/orchestration-v2/runtimeLayer.ts";
import * as OrchestrationEventStore from "../src/persistence/OrchestrationEventStore.ts";
import * as Sqlite from "../src/persistence/Sqlite.ts";

const PAGE_SIZE = 4096;
const WAL_HEADER_BYTES = 32;
const FRAME_HEADER_BYTES = 24;
const FRAME_BYTES = FRAME_HEADER_BYTES + PAGE_SIZE;
const FREE_PAGE = "(free at end)";

const args = process.argv.slice(2);
const command = args[0];
const flag = (name: string, fallback?: string): string => {
  const index = args.indexOf(`--${name}`);
  const value = index === -1 ? fallback : args[index + 1];
  if (value === undefined) throw new Error(`--${name} is required`);
  return value;
};
const optionalNumber = (name: string): number | undefined => {
  const index = args.indexOf(`--${name}`);
  return index === -1 ? undefined : Number(args[index + 1]);
};

interface Stamp {
  readonly size: number;
  readonly mtimeMs: number;
  readonly baseSequence: number;
  readonly pageCount: number;
  readonly names: ReadonlyArray<string>;
}
const stampPath = (base: string) => `${base}.bench.json`;
const pageMapPath = (base: string) => `${base}.pages.bin`;

const readStamp = (base: string): Stamp => {
  const stamp = JSON.parse(NodeFS.readFileSync(stampPath(base), "utf8")) as Stamp;
  const stat = NodeFS.statSync(base);
  if (stat.size !== stamp.size || stat.mtimeMs !== stamp.mtimeMs) {
    throw new Error(
      `${base} changed after prepare (a run checkpointed into it, or it was opened by a server). Take a new VACUUM INTO snapshot and prepare it again.`,
    );
  }
  return stamp;
};

// Thread projections record the last thread event they applied; project events do not move it.
const projectedSequence = (db: NodeSqlite.DatabaseSync): number => {
  const row = db
    .prepare(
      "SELECT last_sequence AS s FROM orchestration_v2_projection_metadata WHERE projection_name = 'thread-projections'",
    )
    .get() as { s: number } | undefined;
  const thread = db
    .prepare(
      "SELECT MAX(sequence) AS s FROM orchestration_events WHERE aggregate_kind = 'thread' AND application_event_version = 2",
    )
    .get() as { s: number | null };
  if (row === undefined || row.s !== thread.s) {
    throw new Error(
      `projections stop at ${row?.s}, thread events at ${thread.s}; the copy is torn`,
    );
  }
  return (db.prepare("SELECT MAX(sequence) AS s FROM orchestration_events").get() as { s: number })
    .s;
};

// dbstat lists every b-tree page; pages it omits are on the freelist.
const pageOwners = (file: string) => {
  const db = new NodeSqlite.DatabaseSync(file, { readOnly: true });
  try {
    const pageSize = (db.prepare("PRAGMA page_size").get() as { page_size: number }).page_size;
    if (pageSize !== PAGE_SIZE) throw new Error(`${file} has ${pageSize}-byte pages`);
    const pageCount = (db.prepare("PRAGMA page_count").get() as { page_count: number }).page_count;
    const names: Array<string> = [FREE_PAGE];
    const index = new Map(names.map((name, i) => [name, i]));
    const owners = new Uint16Array(pageCount + 1);
    for (const row of db.prepare("SELECT pageno, name FROM dbstat").iterate() as Iterable<{
      pageno: number;
      name: string;
    }>) {
      let id = index.get(row.name);
      if (id === undefined) {
        id = names.push(row.name) - 1;
        index.set(row.name, id);
      }
      owners[row.pageno] = id;
    }
    return { pageCount, names, owners };
  } finally {
    db.close();
  }
};

const record = (live: string, base: string, window: string, limit = Number.MAX_SAFE_INTEGER) => {
  if (NodeFS.existsSync(window)) throw new Error(`${window} exists`);
  const baseDb = new NodeSqlite.DatabaseSync(base, { readOnly: true });
  const baseSequence = projectedSequence(baseDb);
  const ddl = (
    baseDb
      .prepare(
        "SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = 'orchestration_events'",
      )
      .get() as { sql: string }
  ).sql;
  baseDb.close();

  const liveDb = new NodeSqlite.DatabaseSync(live, { readOnly: true });
  const out = new NodeSqlite.DatabaseSync(window);
  out.exec(ddl);
  out.exec("CREATE TABLE bench_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)");
  const rows = liveDb
    .prepare("SELECT * FROM orchestration_events WHERE sequence > ? ORDER BY sequence LIMIT ?")
    .all(baseSequence, limit) as Array<Record<string, NodeSqlite.SQLInputValue>>;
  if (rows.length === 0) throw new Error(`no events after sequence ${baseSequence}`);
  // Deduplicated subagent prompts point at anchor rows; the store's read restores them from those.
  const anchors = liveDb
    .prepare(
      `SELECT * FROM orchestration_events WHERE sequence IN (
         SELECT DISTINCT json_extract(payload_json, '$.promptSequence') FROM orchestration_events
         WHERE sequence > ? AND sequence <= ? AND json_type(payload_json, '$.promptSequence') = 'integer'
       ) AND sequence <= ?`,
    )
    .all(baseSequence, rows.at(-1)!.sequence as number, baseSequence) as Array<
    Record<string, NodeSqlite.SQLInputValue>
  >;
  liveDb.close();
  const columns = Object.keys(rows[0]!);
  const insert = out.prepare(
    `INSERT INTO orchestration_events (${columns.join(", ")}) VALUES (${columns.map(() => "?").join(", ")})`,
  );
  out.exec("BEGIN");
  for (const row of [...anchors, ...rows]) insert.run(...columns.map((column) => row[column]!));
  const meta = out.prepare("INSERT INTO bench_meta VALUES (?, ?)");
  meta.run("baseSequence", String(baseSequence));
  meta.run("recordedAt", new Date().toISOString());
  out.exec("COMMIT");
  out.close();
  console.log(
    `recorded ${rows.length} events (${rows[0]!.sequence}..${rows.at(-1)!.sequence}) and ${anchors.length} prompt anchors into ${window}`,
  );
};

const prepare = async (base: string) => {
  // The server's own layer: setup PRAGMAs, migrations to this checkout's head, checkpoint worker.
  await Effect.runPromise(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`PRAGMA wal_checkpoint(TRUNCATE)`;
    }).pipe(
      Effect.provide(Sqlite.layerFromPath(base).pipe(Layer.provide(NodeServices.layer))),
      Effect.scoped,
    ),
  );
  if (NodeFS.existsSync(`${base}-wal`)) throw new Error(`${base}-wal remains after close`);
  const db = new NodeSqlite.DatabaseSync(base, { readOnly: true });
  const baseSequence = projectedSequence(db);
  db.close();
  const { pageCount, names, owners } = pageOwners(base);
  NodeFS.writeFileSync(pageMapPath(base), owners);
  const stat = NodeFS.statSync(base);
  const stamp: Stamp = { size: stat.size, mtimeMs: stat.mtimeMs, baseSequence, pageCount, names };
  NodeFS.writeFileSync(stampPath(base), JSON.stringify(stamp));
  console.log(`prepared ${base}: ${pageCount} pages, events through ${baseSequence}`);
};

interface Frame {
  readonly page: number;
  readonly kind: string;
  /** Rowids on a table leaf page, so a live commit's appended event sequences can be read. */
  readonly rowids?: ReadonlyArray<number>;
}

const readVarint = (page: Buffer, offset: number) => {
  let value = 0;
  for (let i = 0; i < 8; i++) {
    const byte = page[offset + i]!;
    value = value * 128 + (byte & 0x7f);
    if (byte < 0x80) return { value, next: offset + i + 1 };
  }
  return { value: value * 256 + page[offset + 8]!, next: offset + 9 };
};

const tableLeafRowids = (page: Buffer, pageNumber: number) => {
  const header = pageNumber === 1 ? 100 : 0;
  const cells = page.readUInt16BE(header + 3);
  const rowids: Array<number> = [];
  for (let i = 0; i < cells; i++) {
    const cell = page.readUInt16BE(header + 8 + 2 * i);
    rowids.push(readVarint(page, readVarint(page, cell).next).value);
  }
  return rowids;
};
const pageKind = (page: Buffer, pageNumber: number) => {
  switch (page[pageNumber === 1 ? 100 : 0]) {
    case 0x02:
      return "index interior";
    case 0x05:
      return "table interior";
    case 0x0a:
      return "index leaf";
    case 0x0d:
      return "table leaf";
    default:
      return "overflow or freelist";
  }
};

// Reads the commits appended since the last call. Callers keep checkpoints from restarting the
// WAL; frames past the last valid one carry an older salt, and a partial commit waits.
const makeWalReader = (walPath: string) => {
  let offset = WAL_HEADER_BYTES;
  let salt: Buffer | undefined;
  return (): Array<Array<Frame>> => {
    if (!NodeFS.existsSync(walPath)) return [];
    const handle = NodeFS.openSync(walPath, "r");
    try {
      const size = NodeFS.fstatSync(handle).size;
      if (size < WAL_HEADER_BYTES) return [];
      const header = Buffer.alloc(WAL_HEADER_BYTES);
      NodeFS.readSync(handle, header, 0, WAL_HEADER_BYTES, 0);
      if (salt === undefined) salt = Buffer.from(header.subarray(16, 24));
      else if (!header.subarray(16, 24).equals(salt)) {
        throw new Error("the WAL restarted: a checkpoint ran while frames were being counted");
      }
      const commits: Array<Array<Frame>> = [];
      let current: Array<Frame> = [];
      let position = offset;
      const frame = Buffer.alloc(FRAME_BYTES);
      while (position + FRAME_BYTES <= size) {
        NodeFS.readSync(handle, frame, 0, FRAME_BYTES, position);
        if (!frame.subarray(8, 16).equals(salt)) break;
        const page = frame.readUInt32BE(0);
        const image = frame.subarray(FRAME_HEADER_BYTES);
        const kind = pageKind(image, page);
        current.push(
          kind === "table leaf"
            ? { page, kind, rowids: tableLeafRowids(image, page) }
            : { page, kind },
        );
        position += FRAME_BYTES;
        if (frame.readUInt32BE(4) !== 0) {
          commits.push(current);
          current = [];
          offset = position;
        }
      }
      return commits;
    } finally {
      NodeFS.closeSync(handle);
    }
  };
};

interface CommitRecord {
  readonly eventTypes: ReadonlyArray<string>;
  readonly ms: number;
  readonly frames: ReadonlyArray<Frame>;
}

const loadWindow = Effect.fn("benchEventWrites.loadWindow")(
  function* (_window: string, baseSequence: number, events: number | undefined) {
    const store = yield* OrchestrationEventStore.OrchestrationEventStore;
    return yield* Stream.runCollect(
      store.readAgentEvents({
        afterSequence: baseSequence,
        ...(events === undefined ? {} : { limit: events }),
      }),
    );
  },
  (effect, window) =>
    effect.pipe(
      Effect.provide(
        OrchestrationEventStore.layer.pipe(
          Layer.provide(NodeSqliteClient.layer({ filename: window, readonly: true })),
        ),
      ),
      Effect.scoped,
    ),
);

// One write stamps all its events with one clock read, and a provider event's write holds only
// its own events, so consecutive events of a thread and command sharing occurredAt are one commit.
// Checked against live commit boundaries: `live` prints both groupings.
const sameWrite = (a: OrchestrationV2StoredEvent, b: OrchestrationV2StoredEvent) =>
  a.commandId === b.commandId &&
  a.event.threadId === b.event.threadId &&
  a.event.occurredAt.epochMilliseconds === b.event.occurredAt.epochMilliseconds;

// `lastSequences`, when a live run recorded them, ends each group where a live commit ended.
const groupCommits = (
  events: ReadonlyArray<OrchestrationV2StoredEvent>,
  lastSequences: ReadonlyArray<number> | undefined,
) => {
  const groups: Array<Array<OrchestrationV2StoredEvent>> = [];
  const ends = lastSequences === undefined ? undefined : new Set(lastSequences);
  let open = false;
  for (const stored of events) {
    const last = groups.at(-1);
    const joins = ends === undefined ? last !== undefined && sameWrite(last.at(-1)!, stored) : open;
    if (joins && last !== undefined) last.push(stored);
    else groups.push([stored]);
    open = ends !== undefined && !ends.has(stored.sequence);
  }
  return groups;
};

// The window's ids were allocated by the recording build. `--reallocate-event-ids` gives each
// event a fresh id from this checkout's allocator, with the inputs the recorded id encodes
// (`event:thread:<id>[:command:<id>][:provider-session:<id>]:<uuid>`), as production would at
// write time; ids of any other shape stay as recorded.
const allocatorInput = (id: string) => {
  const parts = id.split(":");
  if (parts[0] !== "event" || parts.length % 2 !== 0) return undefined;
  const input: {
    threadId?: ThreadId;
    commandId?: CommandId;
    providerSessionId?: ProviderSessionId;
  } = {};
  for (let i = 1; i < parts.length - 1; i += 2) {
    const value = decodeURIComponent(parts[i + 1]!);
    switch (parts[i]) {
      case "thread":
        input.threadId = ThreadId.make(value);
        break;
      case "command":
        input.commandId = CommandId.make(value);
        break;
      case "provider-session":
        input.providerSessionId = ProviderSessionId.make(value);
        break;
      default:
        return undefined;
    }
  }
  return input;
};

const replayMain = async () => {
  const base = flag("base");
  const window = flag("window");
  const outPath = flag("out");
  const stamp = readStamp(base);
  const windowDb = new NodeSqlite.DatabaseSync(window, { readOnly: true });
  const recordedBase = Number(
    (
      windowDb.prepare("SELECT value FROM bench_meta WHERE key = 'baseSequence'").get() as {
        value: string;
      }
    ).value,
  );
  const projectEvents = (
    windowDb
      .prepare(
        "SELECT COUNT(*) AS n FROM orchestration_events WHERE sequence > ? AND aggregate_kind <> 'thread'",
      )
      .get(recordedBase) as { n: number }
  ).n;
  windowDb.close();
  if (recordedBase !== stamp.baseSequence) {
    throw new Error(
      `the window starts after ${recordedBase}, the base ends at ${stamp.baseSequence}`,
    );
  }
  const events = [
    ...(await Effect.runPromise(loadWindow(window, recordedBase, optionalNumber("events")))),
  ];
  const reallocate = args.includes("--reallocate-event-ids");
  const groupingFile = args.includes("--grouping") ? flag("grouping") : undefined;
  const groups = groupCommits(
    events,
    groupingFile === undefined
      ? undefined
      : (JSON.parse(NodeFS.readFileSync(groupingFile, "utf8")) as Array<number>),
  );
  const readFrames = makeWalReader(`${base}-wal`);
  // A read transaction opened before the first frame stops every checkpoint, including the one
  // a closing connection runs, from copying frames into the base.
  const pin = new NodeSqlite.DatabaseSync(base, { readOnly: true });
  pin.exec("BEGIN");
  pin.prepare("SELECT 1 FROM sqlite_schema LIMIT 1").get();

  type Replayed = {
    readonly commits: ReadonlyArray<CommitRecord>;
    readonly sequenceMismatches: number;
    readonly emptyWrites: number;
    readonly reallocatedIds: number;
  };
  let hand: { resolve: (value: Replayed) => void; reject: (cause: unknown) => void };
  const handed = new Promise<Replayed>((resolve, reject) => {
    hand = { resolve, reject };
  });
  const run = Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    // Checkpoints would write the base; their page cost is computed from the frames instead.
    yield* sql`PRAGMA wal_autocheckpoint = 0`;
    const sink = yield* EventSink.EventSinkV2;
    const ids = yield* IdAllocator.IdAllocatorV2;
    readFrames();
    const commits: Array<CommitRecord> = [];
    let sequenceMismatches = 0;
    let emptyWrites = 0;
    let reallocatedIds = 0;
    for (const group of groups) {
      const toWrite = yield* Effect.forEach(group, (recorded) => {
        const input = reallocate ? allocatorInput(recorded.event.id) : undefined;
        if (input === undefined) return Effect.succeed(recorded.event);
        reallocatedIds++;
        return ids.allocate.event(input).pipe(Effect.map((id) => ({ ...recorded.event, id })));
      });
      const started = performance.now();
      const stored = yield* sink.write({
        ...(group[0]!.commandId === null ? {} : { commandId: group[0]!.commandId }),
        events: toWrite,
      });
      const ms = performance.now() - started;
      stored.forEach((written, i) => {
        if (written.sequence !== group[i]!.sequence) sequenceMismatches++;
      });
      const written = readFrames();
      if (written.length === 0) emptyWrites++;
      if (written.length > 1) throw new Error("one write produced several commits");
      commits.push({
        eventTypes: group.map((recorded) => recorded.event.type),
        ms,
        frames: written[0] ?? [],
      });
    }
    hand.resolve({ commits, sequenceMismatches, emptyWrites, reallocatedIds });
    // The layer's scope ends with this effect, and closing the writer would checkpoint.
    return yield* Effect.never;
  }).pipe(
    Effect.provide(
      layerEventSink.pipe(
        Layer.provideMerge(
          Layer.provideMerge(
            Sqlite.layerSetup,
            NodeSqliteClient.layer({ filename: base, readerWorker: true, checkpointWorker: false }),
          ),
        ),
        Layer.provideMerge(IdAllocator.layer),
        Layer.provide(NodeServices.layer),
      ),
    ),
  );

  Effect.runFork(
    run.pipe(Effect.tapCause((cause) => Effect.sync(() => hand.reject(Cause.pretty(cause))))),
  );
  const { commits, sequenceMismatches, emptyWrites, reallocatedIds } = await handed.catch(
    (cause: unknown) => {
      console.error(cause);
      // A normal exit closes the connections in no fixed order; the pin must outlive the writer.
      return process.kill(process.pid, "SIGKILL") as never;
    },
  );

  const endOwners = pageOwners(base);
  const startOwners = new Uint16Array(NodeFS.readFileSync(pageMapPath(base)).buffer.slice(0));
  const walBytes = NodeFS.statSync(`${base}-wal`).size;
  NodeFS.writeFileSync(
    outPath,
    JSON.stringify(
      summarize({
        commits,
        sequenceMismatches,
        emptyWrites,
        reallocatedIds,
        events: events.length,
        projectEvents,
        stamp,
        startOwners,
        endOwners,
        walBytes,
      }),
    ),
  );
  process.kill(process.pid, "SIGKILL");
};

const percentile = (sorted: ReadonlyArray<number>, p: number) =>
  sorted.length === 0 ? 0 : sorted[Math.min(sorted.length - 1, Math.floor(p * sorted.length))]!;

const summarize = (input: {
  readonly commits: ReadonlyArray<CommitRecord>;
  readonly sequenceMismatches: number;
  readonly emptyWrites: number;
  readonly reallocatedIds: number;
  readonly events: number;
  readonly stamp: Stamp;
  readonly startOwners: Uint16Array;
  readonly endOwners: ReturnType<typeof pageOwners>;
  readonly walBytes: number;
  readonly projectEvents: number;
}) => {
  const { commits, endOwners, startOwners, stamp } = input;
  const written = commits.filter((commit) => commit.frames.length > 0);
  const frameCounts = written.map((commit) => commit.frames.length).sort((a, b) => a - b);
  const frames = frameCounts.reduce((sum, n) => sum + n, 0);
  const byOwner = new Map<string, number>();
  const byKind = new Map<string, number>();
  let ownerChanged = 0;
  let checkpointPages = 0;
  let checkpoints = 0;
  let pending = new Set<number>();
  let pendingFrames = 0;
  for (const commit of written) {
    for (const { page, kind } of commit.frames) {
      const owner = endOwners.names[endOwners.owners[page] ?? 0]!;
      byOwner.set(owner, (byOwner.get(owner) ?? 0) + 1);
      byKind.set(kind, (byKind.get(kind) ?? 0) + 1);
      if (page <= stamp.pageCount && stamp.names[startOwners[page]!] !== owner) ownerChanged++;
      pending.add(page);
    }
    pendingFrames += commit.frames.length;
    // The checkpoint worker backfills once a commit leaves this many frames unchecked.
    if (pendingFrames >= NodeSqliteClient.CHECKPOINT_FRAMES) {
      checkpoints++;
      checkpointPages += pending.size;
      pending = new Set();
      pendingFrames = 0;
    }
  }
  const byEventType = new Map<string, { commits: number; frames: number }>();
  for (const commit of written) {
    const key = commit.eventTypes.length === 1 ? commit.eventTypes[0]! : "(command group)";
    const entry = byEventType.get(key) ?? { commits: 0, frames: 0 };
    entry.commits++;
    entry.frames += commit.frames.length;
    byEventType.set(key, entry);
  }
  const ms = commits.map((commit) => commit.ms).sort((a, b) => a - b);
  const totalMs = ms.reduce((sum, n) => sum + n, 0);
  return {
    events: input.events,
    writes: commits.length,
    commits: written.length,
    emptyWrites: input.emptyWrites,
    reallocatedIds: input.reallocatedIds,
    sequenceMismatches: input.sequenceMismatches,
    frames,
    walBytes: input.walBytes,
    projectEventsNotReplayed: input.projectEvents,
    framesPerEvent: frames / input.events,
    walBytesPerEvent: (frames * FRAME_BYTES) / input.events,
    framesPerCommit: {
      p50: percentile(frameCounts, 0.5),
      p90: percentile(frameCounts, 0.9),
      max: frameCounts.at(-1) ?? 0,
      mean: frames / written.length,
    },
    checkpoint: {
      checkpoints,
      pages: checkpointPages,
      // The next checkpoint writes the tail too; without it, a run whose last checkpoint lands at
      // its end reads costlier per event than one that stops just short of it.
      pagesPerEvent: (checkpointPages + pending.size) / input.events,
      uncheckpointedTailPages: pending.size,
    },
    ownerChangedFrames: ownerChanged,
    byOwner: Object.fromEntries([...byOwner].sort((a, b) => b[1] - a[1])),
    byKind: Object.fromEntries([...byKind].sort((a, b) => b[1] - a[1])),
    byEventType: Object.fromEntries([...byEventType].sort((a, b) => b[1].frames - a[1].frames)),
    writeMs: {
      total: totalMs,
      perEvent: totalMs / input.events,
      p50: percentile(ms, 0.5),
      p90: percentile(ms, 0.9),
      max: ms.at(-1) ?? 0,
    },
  };
};
type Summary = ReturnType<typeof summarize>;

const hostLoad = () => {
  const run = (file: string, argv: ReadonlyArray<string>) =>
    NodeChildProcess.spawnSync(file, argv, { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] })
      .stdout ?? "";
  return {
    uptime: run("uptime", []).trim(),
    cgtop: run("systemd-cgtop", ["-b", "-n", "2", "-d", "1", "--depth=3", "-c"])
      .split("\n")
      .slice(-25)
      .join("\n"),
  };
};

const spread = (values: ReadonlyArray<number>) => {
  const min = Math.min(...values);
  const max = Math.max(...values);
  return { min, max, spread: max - min };
};

const runMain = (input: {
  readonly base: string;
  readonly window: string;
  readonly runs: number;
  readonly out: string;
  readonly events: number | undefined;
  readonly grouping?: string | undefined;
  readonly reallocate?: boolean;
}) => {
  const { base, window, runs, out, events, grouping, reallocate } = input;
  readStamp(base);
  NodeFS.mkdirSync(out, { recursive: true });
  const script = NodeURL.fileURLToPath(import.meta.url);
  const loadBefore = hostLoad();
  const results: Array<Summary> = [];
  for (let i = 1; i <= runs; i++) {
    for (const suffix of ["-wal", "-shm"]) NodeFS.rmSync(`${base}${suffix}`, { force: true });
    readStamp(base);
    const outPath = NodePath.join(out, `run-${i}.json`);
    NodeFS.rmSync(outPath, { force: true });
    const child = NodeChildProcess.spawnSync(
      process.execPath,
      [
        script,
        "replay",
        "--base",
        base,
        "--window",
        window,
        "--out",
        outPath,
        ...(events === undefined ? [] : ["--events", String(events)]),
        ...(grouping === undefined ? [] : ["--grouping", grouping]),
        ...(reallocate === true ? ["--reallocate-event-ids"] : []),
      ],
      { stdio: "inherit" },
    );
    if (!NodeFS.existsSync(outPath)) {
      throw new Error(`run ${i} failed (status ${child.status}, signal ${child.signal})`);
    }
    results.push(JSON.parse(NodeFS.readFileSync(outPath, "utf8")) as Summary);
  }
  for (const suffix of ["-wal", "-shm"]) NodeFS.rmSync(`${base}${suffix}`, { force: true });
  readStamp(base);
  const loadAfter = hostLoad();
  const first = results[0]!;
  const counts = {
    commits: spread(results.map((r) => r.commits)),
    frames: spread(results.map((r) => r.frames)),
    checkpointPages: spread(results.map((r) => r.checkpoint.pages)),
  };
  const report = { runs: results.length, counts, first, results, loadBefore, loadAfter };
  NodeFS.writeFileSync(NodePath.join(out, "summary.json"), JSON.stringify(report, null, 2));
  printReport(report);
  return report;
};

const printReport = (report: {
  readonly runs: number;
  readonly counts: Record<string, ReturnType<typeof spread>>;
  readonly first: Summary;
  readonly results: ReadonlyArray<Summary>;
  readonly loadBefore: ReturnType<typeof hostLoad>;
  readonly loadAfter: ReturnType<typeof hostLoad>;
}) => {
  const r = report.first;
  const pct = (n: number) => `${((100 * n) / r.frames).toFixed(1)}%`;
  console.log(
    `events ${r.events}, writes ${r.writes}, commits ${r.commits} (empty writes ${r.emptyWrites}, sequence mismatches ${r.sequenceMismatches}, project events not replayed ${r.projectEventsNotReplayed}, event ids reallocated ${r.reallocatedIds})`,
  );
  console.log(
    `WAL frames ${r.frames}: ${r.framesPerEvent.toFixed(2)} per event, ${(r.walBytesPerEvent / 1024).toFixed(1)} KiB per event`,
  );
  console.log(
    `frames per commit: p50 ${r.framesPerCommit.p50}, p90 ${r.framesPerCommit.p90}, max ${r.framesPerCommit.max}, mean ${r.framesPerCommit.mean.toFixed(2)}`,
  );
  console.log(
    `checkpoints (every ${NodeSqliteClient.CHECKPOINT_FRAMES} frames, simulated): ${r.checkpoint.checkpoints}, ${r.checkpoint.pages} pages + ${r.checkpoint.uncheckpointedTailPages} in the tail, ${r.checkpoint.pagesPerEvent.toFixed(2)} per event`,
  );
  console.log(
    `repeatability over ${report.runs} runs: ${Object.entries(report.counts)
      .map(([k, v]) => `${k} ${v.min}..${v.max}`)
      .join(", ")}`,
  );
  console.log(
    `frames whose page changed owner during the replay: ${r.ownerChangedFrames} (${pct(r.ownerChangedFrames)})`,
  );
  console.log("\nframes by b-tree (owner at the end of the run):");
  for (const [name, n] of Object.entries(r.byOwner))
    console.log(`  ${String(n).padStart(8)}  ${pct(n).padStart(6)}  ${name}`);
  console.log("\nframes by page kind:");
  for (const [name, n] of Object.entries(r.byKind))
    console.log(`  ${String(n).padStart(8)}  ${pct(n).padStart(6)}  ${name}`);
  console.log("\nframes per commit by event type:");
  for (const [type, { commits, frames }] of Object.entries(r.byEventType))
    console.log(
      `  ${String(commits).padStart(6)} commits  ${(frames / commits).toFixed(1).padStart(6)} frames each  ${type}`,
    );
  console.log("\nwrite time (ms, inside EventSink.write):");
  for (const [i, result] of report.results.entries())
    console.log(
      `  run ${i + 1}: ${result.writeMs.perEvent.toFixed(3)} per event, p50 ${result.writeMs.p50.toFixed(3)}, p90 ${result.writeMs.p90.toFixed(3)}, max ${result.writeMs.max.toFixed(1)}`,
    );
  console.log(
    `\nhost load before: ${report.loadBefore.uptime}\nhost load after:  ${report.loadAfter.uptime}`,
  );
};

const DEFAULT_MESSAGE =
  "Run `ls`, read README.md, then write notes.txt with a one-line summary of it. Reply in one sentence.";

const maxSequence = (file: string) => {
  const db = new NodeSqlite.DatabaseSync(file, { readOnly: true });
  try {
    return (
      db.prepare("SELECT MAX(sequence) AS s FROM orchestration_events").get() as { s: number }
    ).s;
  } finally {
    db.close();
  }
};

const waitForServer = async (port: number) => {
  for (let attempt = 0; attempt < 240; attempt++) {
    const up = await fetch(`http://127.0.0.1:${port}/`).then(
      () => true,
      () => false,
    );
    if (up) return;
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  throw new Error(`no server on port ${port} after 120 s`);
};

// One real provider turn on a server started from this checkout against a seeded scratch home,
// counted from its WAL, then the same events replayed on a copy taken just before the turn.
const live = async () => {
  const home = NodePath.resolve(flag("home"));
  const project = NodePath.resolve(flag("project"));
  const provider = flag("provider", "claudeAgent");
  const model = flag("model", "claude-haiku-5-5");
  const message = flag("message", DEFAULT_MESSAGE);
  const port = Number(flag("port", "38431"));
  const settleMs = Number(flag("settle-ms", "15000"));
  const out = NodePath.resolve(flag("out", NodePath.join(home, "..", "live-results")));
  const db = NodePath.join(home, "userdata", "statev2.sqlite");
  if (!NodeFS.existsSync(db)) {
    throw new Error(
      `${db} is missing; seed it with migrate-dev-db --source <snapshot> --base-dir ${home}`,
    );
  }
  NodeFS.rmSync(out, { recursive: true, force: true });
  NodeFS.mkdirSync(out, { recursive: true });
  const pre = NodePath.join(out, "pre-turn.sqlite");
  const window = NodePath.join(out, "turn-window.sqlite");
  const grouping = NodePath.join(out, "live-commits.json");
  const bin = NodeURL.fileURLToPath(new URL("../src/bin.ts", import.meta.url));
  const log = NodeFS.openSync(NodePath.join(out, "server.log"), "w");
  const server = NodeChildProcess.spawn(
    process.execPath,
    [bin, "serve", "--base-dir", home, "--port", String(port), "--no-browser", project],
    { cwd: project, stdio: ["ignore", log, log] },
  );
  const exited = new Promise((resolve) => server.once("exit", resolve));
  let liveReport;
  try {
    await waitForServer(port);
    const added = NodeChildProcess.spawnSync(
      process.execPath,
      [bin, "project", "add", "--base-dir", home, project],
      { cwd: project, encoding: "utf8" },
    );
    const addOutput = added.stdout + added.stderr;
    if (added.status !== 0 && !addOutput.includes("already exists")) {
      throw new Error(`project add failed: ${addOutput}`);
    }
    // Startup work (imports, sweeps) commits for a while after the port opens.
    await new Promise((resolve) => setTimeout(resolve, settleMs));
    new NodeSqlite.DatabaseSync(db, { readOnly: true }).exec(
      `VACUUM INTO '${pre.replaceAll("'", "''")}'`,
    );
    const pin = new NodeSqlite.DatabaseSync(db, { readOnly: true });
    pin.exec("BEGIN");
    const startSequence = (
      pin.prepare("SELECT MAX(sequence) AS s FROM orchestration_events").get() as { s: number }
    ).s;
    const preDb = new NodeSqlite.DatabaseSync(pre, { readOnly: true });
    const preSequence = projectedSequence(preDb);
    preDb.close();
    if (preSequence !== startSequence) {
      throw new Error("the server appended events between the copy and the pin; raise --settle-ms");
    }
    const readFrames = makeWalReader(`${db}-wal`);
    readFrames();
    try {
      const turn = NodeChildProcess.spawnSync(
        process.execPath,
        [
          bin,
          "thread",
          "start",
          "--base-dir",
          home,
          project,
          message,
          "--provider",
          provider,
          "--model",
          model,
          "--wait",
          "--json",
        ],
        { cwd: project, encoding: "utf8", timeout: 600_000 },
      );
      NodeFS.writeFileSync(NodePath.join(out, "turn.json"), turn.stdout + turn.stderr);
      if (turn.status !== 0)
        throw new Error(`the turn failed (${turn.status}); see ${out}/turn.json`);
      await new Promise((resolve) => setTimeout(resolve, settleMs));
      const commits = readFrames();
      const endSequence = maxSequence(db);
      const owners = pageOwners(db);
      const ownerOf = (page: number) => owners.names[owners.owners[page] ?? 0]!;
      const eventCommits = commits.filter((frames) =>
        frames.some(({ page }) => ownerOf(page) === "orchestration_events"),
      );
      const events = endSequence - startSequence;
      // A commit rewrites the events table's last leaf; the highest rowid on it ends that commit.
      const lastSequences = eventCommits.flatMap((frames) => {
        const rowids = frames.flatMap((frame) =>
          ownerOf(frame.page) === "orchestration_events" ? (frame.rowids ?? []) : [],
        );
        const last = Math.max(...rowids);
        return last > startSequence ? [last] : [];
      });
      NodeFS.writeFileSync(grouping, JSON.stringify(lastSequences));
      const count = (list: ReadonlyArray<ReadonlyArray<Frame>>) =>
        list.reduce((sum, frames) => sum + frames.length, 0);
      const sizes = eventCommits.map((frames) => frames.length).sort((a, b) => a - b);
      const byOwner = new Map<string, number>();
      for (const frames of commits)
        for (const { page } of frames)
          byOwner.set(ownerOf(page), (byOwner.get(ownerOf(page)) ?? 0) + 1);
      liveReport = {
        events,
        commits: commits.length,
        eventCommits: eventCommits.length,
        frames: count(commits),
        eventCommitFrames: count(eventCommits),
        framesPerEvent: count(commits) / events,
        eventCommitFramesPerEvent: count(eventCommits) / events,
        eventCommitFrames_p50: percentile(sizes, 0.5),
        eventCommitFrames_p90: percentile(sizes, 0.9),
        byOwner: Object.fromEntries([...byOwner].sort((a, b) => b[1] - a[1])),
      };
      NodeFS.writeFileSync(NodePath.join(out, "live.json"), JSON.stringify(liveReport, null, 2));
    } finally {
      pin.close();
    }
  } finally {
    server.kill("SIGTERM");
    await exited;
    NodeFS.closeSync(log);
  }
  record(db, pre, window);
  await prepare(pre);
  const regrouped = runMain({
    base: pre,
    window,
    runs: 1,
    out: NodePath.join(out, "replay-live-commits"),
    events: undefined,
    grouping,
  }).first;
  const replayed = runMain({
    base: pre,
    window,
    runs: 1,
    out: NodePath.join(out, "replay"),
    events: undefined,
  }).first;
  const r = liveReport;
  const pctOf = (map: Record<string, number>, total: number, name: string) =>
    `${(((map[name] ?? 0) * 100) / total).toFixed(1)}%`;
  console.log(
    `\nlive turn: ${r.events} events, ${r.commits} commits (${r.eventCommits} wrote events), ${r.frames} frames`,
  );
  console.log(
    `  frames per event: ${r.framesPerEvent.toFixed(2)} over all commits, ${r.eventCommitFramesPerEvent.toFixed(2)} over event commits; event commit p50 ${r.eventCommitFrames_p50}, p90 ${r.eventCommitFrames_p90}`,
  );
  console.log(
    `replay of the same events: ${replayed.commits} commits, ${replayed.framesPerEvent.toFixed(2)} frames per event, p50 ${replayed.framesPerCommit.p50}, p90 ${replayed.framesPerCommit.p90}`,
  );
  console.log(
    `replay with the live commit boundaries: ${regrouped.commits} commits, ${regrouped.framesPerEvent.toFixed(2)} frames per event, p50 ${regrouped.framesPerCommit.p50}, p90 ${regrouped.framesPerCommit.p90}`,
  );
  console.log("\nshare of frames, live vs replay:");
  for (const name of Object.keys(r.byOwner).slice(0, 30))
    console.log(
      `  ${pctOf(r.byOwner, r.frames, name).padStart(6)}  ${pctOf(replayed.byOwner, replayed.frames, name).padStart(6)}  ${name}`,
    );
};

switch (command) {
  case "record":
    record(flag("live"), flag("base"), flag("window"), optionalNumber("limit"));
    break;
  case "prepare":
    await prepare(flag("base"));
    break;
  case "replay":
    await replayMain();
    break;
  case "run":
    runMain({
      base: flag("base"),
      window: flag("window"),
      runs: optionalNumber("runs") ?? 3,
      out: flag("out", NodePath.join(NodePath.dirname(flag("base")), "results")),
      events: optionalNumber("events"),
      grouping: args.includes("--grouping") ? flag("grouping") : undefined,
      reallocate: args.includes("--reallocate-event-ids"),
    });
    break;
  case "live":
    await live();
    break;
  default:
    throw new Error("usage: bench-event-writes.ts record|prepare|run (see the header)");
}
