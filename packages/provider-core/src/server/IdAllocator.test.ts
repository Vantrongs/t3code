import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { TestClock } from "effect/testing";

import * as IdAllocator from "./IdAllocator.ts";

it.effect("event ids are a short UUIDv7 that sorts in allocation order", () =>
  Effect.gen(function* () {
    const ids = yield* IdAllocator.IdAllocatorV2;
    const start = Date.parse("2026-11-12T08:00:00.000Z");
    yield* TestClock.setTime(start);
    const allocated: Array<string> = [];
    for (let ms = 0; ms < 50; ms++) {
      allocated.push(yield* ids.allocate.event());
      yield* TestClock.adjust("1 millis");
    }

    assert.deepStrictEqual(allocated.toSorted(), allocated);
    const [first] = allocated;
    assert.match(
      first!,
      /^event:[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
    );
    assert.equal(
      Number.parseInt(first!.slice("event:".length).replace("-", "").slice(0, 12), 16),
      start,
    );

    const sameMillisecond = yield* Effect.replicateEffect(ids.allocate.event(), 1000);
    assert.equal(new Set(sameMillisecond).size, 1000);
  }).pipe(Effect.provide(IdAllocator.layer)),
);
