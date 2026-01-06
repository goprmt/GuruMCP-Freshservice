import { kv } from "@vercel/kv";

const QUEUE_KEY = "fs:guru:queue";
const LOCK_PREFIX = "fs:guru:lock:";   // lock:<jobId>
const DONE_PREFIX = "fs:guru:done:";   // done:<jobId>

/**
 * jobId should be deterministic to prevent duplicates.
 * Example: sha256(ticketId + subject + description + company + vip)
 */
export async function enqueueJob(job) {
  await kv.rpush(QUEUE_KEY, JSON.stringify(job));
}

export async function dequeueJob() {
  const raw = await kv.lpop(QUEUE_KEY);
  if (!raw) return null;
  return JSON.parse(raw);
}

export async function markDone(jobId, ttlSeconds = 3600) {
  await kv.set(`${DONE_PREFIX}${jobId}`, "1", { ex: ttlSeconds });
}

export async function isDone(jobId) {
  return Boolean(await kv.get(`${DONE_PREFIX}${jobId}`));
}

// Simple lock to avoid double-processing
export async function acquireLock(jobId, ttlSeconds = 120) {
  const key = `${LOCK_PREFIX}${jobId}`;
  // SET key NX EX ttl
  const ok = await kv.set(key, "1", { nx: true, ex: ttlSeconds });
  return ok === "OK";
}

export async function releaseLock(jobId) {
  await kv.del(`${LOCK_PREFIX}${jobId}`);
}

