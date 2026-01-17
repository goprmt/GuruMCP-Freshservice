import crypto from "crypto";
import { enqueueJob, isDone } from "../lib/queue.js";

export const config = { runtime: "nodejs" };

function mustEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env var: ${name}`);
  return v;
}

const BRIDGE_KEY = mustEnv("BRIDGE_KEY");
const WORKER_KEY = mustEnv("WORKER_KEY"); // internal secret used to call worker

function jobIdFor(payload) {
  const s = JSON.stringify({
    ticketId: payload.ticketId,
    company: payload.company,
    subject: payload.subject,
    description: payload.description,
    vip: payload.vip,
  });
  return crypto.createHash("sha256").update(s).digest("hex");
}

export default async function handler(req, res) {
  try {
    if (req.method !== "POST") return res.status(405).json({ error: "Method not allowed" });

    const key = req.headers["x-bridge-key"];
    if (key !== BRIDGE_KEY) return res.status(401).json({ error: "Unauthorized" });

    const { description = "", subject = "", company = "", ticketId, vip = false } = req.body || {};
    if (!ticketId || !company) return res.status(400).json({ error: "Missing ticketId/company" });

    const jobId = jobIdFor({ description, subject, company, ticketId, vip });

    // idempotency: if already done recently, return OK
    if (await isDone(jobId)) return res.status(200).json({ ok: true, deduped: true });

    await enqueueJob({
      jobId,
      ticketId,
      company,
      subject,
      description,
      vip,
      createdAt: Date.now(),
    });

    // Kick the worker (best effort). Don't wait for completion; cron will drain if this fails.
    // Build a reliable origin across Vercel prod/preview and proxies.
    const proto = (req.headers["x-forwarded-proto"] || "https").toString().split(",")[0].trim();
    const forwardedHost = (req.headers["x-forwarded-host"] || "").toString().split(",")[0].trim();
    const host = (req.headers.host || "").toString().trim();
    const vercelHost = (process.env.VERCEL_URL || "").toString().trim();

    const originHost = vercelHost || forwardedHost || host;
    if (!originHost) {
      console.warn("Worker kick skipped: no host available (VERCEL_URL/x-forwarded-host/host all empty)");
    } else {
      const origin = `${proto}://${originHost}`;

      void fetch(`${origin}/api/worker`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-WORKER-KEY": WORKER_KEY,
        },
        body: JSON.stringify({ maxJobs: 1 }),
      })
        .then(async (r) => {
          if (!r.ok) {
            const text = await r.text().catch(() => "");
            console.warn("Worker kick failed:", r.status, text);
          }
        })
        .catch((err) => {
          // TypeError: fetch failed usually means DNS/TLS/origin issues.
          console.warn(
            "Worker kick error:",
            err?.name || err,
            err?.message || "",
            "originHost=",
            originHost,
            "proto=",
            proto
          );
        });
    }

    return res.status(200).json({ ok: true, enqueued: true, jobId });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: String(e?.message || e) });
  }
}
