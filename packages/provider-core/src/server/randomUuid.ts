import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Random from "effect/Random";

const randomBytes = Effect.all(
  Array.from({ length: 16 }, () => Random.nextIntBetween(0, 256, { halfOpen: true })),
);

const format = (bytes: ReadonlyArray<number>) => {
  const hex = bytes.map((byte) => byte.toString(16).padStart(2, "0")).join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
};

export const randomUuidV4 = randomBytes.pipe(
  Effect.map((bytes) => {
    bytes[6] = (bytes[6]! & 0x0f) | 0x40;
    bytes[8] = (bytes[8]! & 0x3f) | 0x80;
    return format(bytes);
  }),
);

/**
 * RFC 9562 UUIDv7: a 48-bit Unix millisecond timestamp, then random bits. UUIDs made later sort
 * after earlier ones (within a millisecond their order is random), so keys ending in one land
 * next to each other in an index instead of in a random leaf.
 */
export const randomUuidV7 = Effect.zipWith(Clock.currentTimeMillis, randomBytes, (now, bytes) => {
  const timestamp = Math.min(Math.max(0, Math.trunc(now)), 2 ** 48 - 1);
  for (let i = 0; i < 6; i++) bytes[i] = Math.floor(timestamp / 2 ** (8 * (5 - i))) & 0xff;
  bytes[6] = (bytes[6]! & 0x0f) | 0x70;
  bytes[8] = (bytes[8]! & 0x3f) | 0x80;
  return format(bytes);
});
