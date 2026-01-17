import { kv } from "@vercel/kv";

const QUEUE_KEY = "fs:guru:queue";
const LOCK_PREFIX = "fs:guru:lock:";   // lock:<jobId>
const DONE_PREFIX = "fs:guru:done:";   // done:<jobId>

/**
 * jobId should be deterministic to prevent duplicates.
 * Example: sha256(ticketId + subject + description + company + vip)
 */
export async function enqueueJob(job) {
  // Always enqueue as a string to avoid KV returning objects like "[object Object]".
  const payload = typeof job === "string" ? job : JSON.stringify(job);
  await kv.rpush(QUEUE_KEY, payload);
}

export async function dequeueJob() {
  // Try a few pops in case the queue contains one or more bad entries.
  for (let i = 0; i < 10; i++) {
    const raw = await kv.lpop(QUEUE_KEY);
    if (!raw) return null;

    // KV usually returns a string, but be defensive.
    // Some clients/paths may return already-deserialized objects.
    if (typeof raw === "object") {
      if (raw && typeof raw === "object") return raw;
      console.warn("Queue item was object but invalid; dropping.");
      continue;
    }

    if (typeof raw !== "string") {
      console.warn("Queue item was not a string; dropping. type=", typeof raw);
      continue;
    }

    try {
      const job = JSON.parse(raw);
      // Basic shape check
      if (!job || typeof job !== "object") {
        console.warn("Queue item parsed to non-object; dropping.");
        continue;
      }
      return job;
    } catch (e) {
      console.warn("Invalid queue JSON; dropping item:", raw.slice(0, 200));
      continue;
    }
  }

  // If we got here, we likely had a run of bad entries.
  return null;
}

export async function purgeQueue() {
  await kv.del(QUEUE_KEY);
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
