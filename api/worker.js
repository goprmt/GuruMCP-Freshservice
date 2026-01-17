import OpenAI from "openai";
import { dequeueJob, acquireLock, releaseLock, markDone } from "../lib/queue.js";

export const config = { runtime: "nodejs" };

function mustEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env var: ${name}`);
  return v;
}

const FS_DOMAIN = mustEnv("FRESHSERVICE_DOMAIN");
const FS_KEY = mustEnv("FRESHSERVICE_API_KEY");

const GURU_EMAIL = mustEnv("GURU_EMAIL");
const GURU_TOKEN = mustEnv("GURU_API_TOKEN");
const GURU_AGENT_ID = mustEnv("GURU_AGENT_ID");

const OPENAI_API_KEY = mustEnv("OPENAI_API_KEY");
const OPENAI_MODEL = process.env.OPENAI_MODEL || "gpt-5";

const WORKER_KEY = mustEnv("WORKER_KEY");

const CLIENTS_COLLECTION_ID = "f46a8a25-78ae-4214-b181-185d8d5d455d";
const INTERNAL_COLLECTION_ID = "a45f67d4-19fe-47e1-a86a-34b8cd438a76";

const openai = new OpenAI({ apiKey: OPENAI_API_KEY });

/** -----------------------------
 * Freshservice helpers
 * ----------------------------- */
async function freshserviceAddPrivateNote(ticketId, bodyHtml) {
  const url = `https://${FS_DOMAIN}/api/v2/tickets/${ticketId}/notes`;
  const res = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: "Basic " + Buffer.from(`${FS_KEY}:X`).toString("base64"),
    },
    body: JSON.stringify({ body: bodyHtml, private: true }),
  });
  if (!res.ok) throw new Error(`Freshservice note failed ${res.status}: ${await res.text()}`);
}

/** -----------------------------
 * HTML rendering helpers
 * ----------------------------- */
function escapeHtml(s) {
  return String(s || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function renderSourcesAsHtmlLinks(sources = []) {
  if (!sources.length) {
    return `<ul><li><em>No applicable Guru sources found within allowed scope.</em></li></ul>`;
  }

  const items = sources.map((s) => {
    const title = escapeHtml(s.title || "Untitled");
    const url = s.url ? escapeHtml(s.url) : "";
    const ver = s.verificationState
      ? ` <span style="color:#777">(${escapeHtml(s.verificationState)})</span>`
      : "";
    return `<li>${url ? `<a href="${url}" target="_blank" rel="noreferrer">${title}</a>` : title}${ver}</li>`;
  });

  return `<ul>${items.join("")}</ul>`;
}

function renderRunbookHtml({ company, runbook, sourcesHtml, followUps, signals }) {
  const stepsHtml = (runbook.steps || [])
    .map(
      (s) => `
<li>
  <strong>${escapeHtml(s.action)}</strong><br/>
  <span>${escapeHtml(s.details)}</span><br/>
  <em style="color:#555">Verify:</em> ${escapeHtml(s.verification)}
</li>`.trim()
    )
    .join("");

  const prereqHtml = (runbook.prerequisites || []).map((p) => `<li>${escapeHtml(p)}</li>`).join("");
  const escalationHtml = (runbook.escalation || []).map((e) => `<li>${escapeHtml(e)}</li>`).join("");
  const notesHtml = (runbook.notes || []).map((n) => `<li>${escapeHtml(n)}</li>`).join("");
  const followHtml = (followUps || []).slice(0, 3).map((q) => `<li>${escapeHtml(q)}</li>`).join("");

  return `
<h3>✅ Guru Recommendation (${escapeHtml(company)})</h3>

<h4>🧭 Runbook: ${escapeHtml(runbook.title)}</h4>
<p><strong>Summary:</strong> ${escapeHtml(runbook.summary)}</p>

${runbook.prerequisites?.length ? `
<p><strong>Prerequisites / Confirm:</strong></p>
<ul>${prereqHtml}</ul>
` : ""}

<p><strong>Steps:</strong></p>
<ol>${stepsHtml}</ol>

<p><strong>Approvals:</strong> ${runbook.approvals?.required ? "✅ Required" : "❌ Not explicitly required"}</p>
<p style="color:#555; margin-top:-8px;">${escapeHtml(runbook.approvals?.rationale || "")}</p>

${runbook.escalation?.length ? `
<p><strong>Escalation:</strong></p>
<ul>${escalationHtml}</ul>
` : ""}

${runbook.notes?.length ? `
<p><strong>Notes:</strong></p>
<ul>${notesHtml}</ul>
` : ""}

<hr/>

<h4>📚 Sources</h4>
${sourcesHtml}

${(followUps || []).length ? `
<h4>❓ Suggested Follow-ups</h4>
<ul>${followHtml}</ul>
` : ""}

<hr/>
<p style="color:#777; font-size:12px;">
VIP=${signals.vip} • Privileged=${signals.privileged} • ApprovalGate=${signals.approvalGate} • ExemptionsCard=${signals.exemptionsFound} • CompanyBoard=${escapeHtml(signals.companyBoardId || "not-detected")}
</p>
`.trim();
}

/** -----------------------------
 * Guru MCP JSON-RPC client
 * ----------------------------- */
let rpcId = 1;
async function guruToolCall({ name, args }) {
  const basic = Buffer.from(`${GURU_EMAIL}:${GURU_TOKEN}`).toString("base64");
  const payload = {
    jsonrpc: "2.0",
    id: rpcId++,
    method: "tools/call",
    params: { name, arguments: args },
  };

  const res = await fetch("https://mcp.api.getguru.com/mcp", {
    method: "POST",
    headers: {
      Authorization: `Basic ${basic}`,
      "Content-Type": "application/json",
      Accept: "application/json, text/event-stream",
    },
    body: JSON.stringify(payload),
  });

  const text = await res.text();
  if (!res.ok) throw new Error(`Guru MCP HTTP ${res.status}: ${text}`);

  // handle normal JSON response (most common)
  let json;
  try {
    json = JSON.parse(text);
  } catch {
    // handle SSE: pick last data: line
    const dataLines = text.split("\n").map(l => l.trim()).filter(l => l.startsWith("data:"));
    const last = dataLines[dataLines.length - 1] || "";
    json = JSON.parse(last.replace(/^data:\s*/, ""));
  }

  if (json.error) throw new Error(`Guru MCP error: ${json.error.message}`);
  return json.result;
}

/** -----------------------------
 * Normalization + scoping helpers
 * ----------------------------- */
function normalize(s) {
  return String(s || "").trim().toLowerCase();
}

function isInternalOrClientCompanyCard(card, companyBoardId, companyName) {
  const colId = card?.collection?.id;
  if (!colId) return false;

  if (colId === INTERNAL_COLLECTION_ID) return true;

  if (colId === CLIENTS_COLLECTION_ID) {
    const boards = Array.isArray(card?.boards) ? card.boards : [];

    // Preferred: strict board-id match when detected.
    if (companyBoardId) {
      return boards.some((b) => b?.id === companyBoardId);
    }

    // Fallback: if board-id detection failed, allow cards that clearly live in the
    // company folder by matching company name to board titles.
    const company = normalize(companyName);
    if (!company) return false;
    return boards.some((b) => normalize(b?.title).includes(company));
  }

  return false;
}

function findCompanyBoardIdFromResults(results, companyName) {
  const company = normalize(companyName);
  for (const card of results) {
    const boards = Array.isArray(card?.boards) ? card.boards : [];
    for (const b of boards) {
      if (normalize(b?.title).includes(company)) return b.id;
    }
  }
  return null;
}

function scoreExemptionsCard(card) {
  const title = normalize(card?.preferredPhrase || card?.title || "");
  const slug = normalize(card?.slug || "");
  let score = 0;

  if (title.includes("exempt")) score += 6;
  if (title.includes("exception")) score += 6;
  if (slug.includes("exempt")) score += 3;
  if (slug.includes("exception")) score += 3;

  if (card?.collection?.id === CLIENTS_COLLECTION_ID) score += 1;
  return score;
}

/** -----------------------------
 * Tool result helpers
 * ----------------------------- */
function coerceSearchResultsToArray(searchResult) {
  // In your examples, guru_search_documents returns an array of card objects.
  if (!searchResult) return [];
  if (Array.isArray(searchResult)) return searchResult;

  // Some servers might wrap it (rare)
  if (Array.isArray(searchResult.results)) return searchResult.results;
  if (Array.isArray(searchResult.documents)) return searchResult.documents;
  if (Array.isArray(searchResult.items)) return searchResult.items;

  // If it's a single card-ish object
  if (searchResult?.id && searchResult?.collection?.id) return [searchResult];

  return [];
}

function formatAnswerResult(answerResult) {
  if (!answerResult) return { answerText: "", sources: [] };

  if (typeof answerResult.answer === "string") {
    return {
      answerText: answerResult.answer.trim(),
      sources: Array.isArray(answerResult.sources) ? answerResult.sources : [],
    };
  }

  // Fallback
  return {
    answerText: typeof answerResult === "string" ? answerResult : JSON.stringify(answerResult, null, 2),
    sources: [],
  };
}

function cardToSource(card) {
  if (!card?.id) return null;
  return {
    id: card.id,
    title: card.preferredPhrase || card.title || "Untitled",
    url: card.slug ? `https://app.getguru.com/card/${card.slug}` : undefined,
    verificationState: card.verificationState,
  };
}

function dedupeSourcesById(list) {
  const seen = new Set();
  return (list || []).filter((s) => {
    if (!s?.id) return false;
    if (seen.has(s.id)) return false;
    seen.add(s.id);
    return true;
  });
}

/** -----------------------------
 * Exemptions relevance heuristic (conservative)
 * ----------------------------- */
function exemptionsLikelyRelevant(exemptionsText, subject, description) {
  if (!exemptionsText) return false;

  const hay = normalize(exemptionsText);
  const needles = normalize(`${subject} ${description}`);

  const keyTerms = [
    "calendar",
    "delegate",
    "delegation",
    "mailbox",
    "drive",
    "file",
    "access",
    "approval",
    "exception",
    "exempt",
    "security",
    "report",
    "admin",
    "privileged",
    "password",
    "mfa",
    "sso",
    "entra",
    "egnyte",
  ];

  const requestHits = keyTerms.filter((t) => needles.includes(t));
  const exemptionsHits = requestHits.filter((t) => hay.includes(t));
  return exemptionsHits.length > 0;
}

/** -----------------------------
 * Enforce scope on answer sources (post-check)
 * ----------------------------- */
async function filterAnswerSourcesToAllowedScope(sources, companyBoardId, companyName) {
  const allowed = [];
  const rejected = [];

  for (const s of sources || []) {
    const cardId = s?.id;
    if (!cardId) {
      rejected.push({ source: s, reason: "missing id" });
      continue;
    }

    try {
      const card = await guruToolCall({
        name: "guru_get_card_by_id",
        args: { id: cardId },
      });

      if (isInternalOrClientCompanyCard(card, companyBoardId, companyName)) {
        allowed.push(s);
      } else {
        rejected.push({ source: s, reason: "outside allowed collection/board scope" });
      }
    } catch (e) {
      rejected.push({ source: s, reason: `lookup failed: ${String(e?.message || e)}` });
    }
  }

  return { allowed, rejected };
}

/** -----------------------------
 * OpenAI Question Builder (Structured Outputs)
 * ----------------------------- */
async function buildQueryPackage({ subject, description, company, vip }) {
  const schema = {
    type: "object",
    additionalProperties: false,
    properties: {
      company: { type: "string" },
      searchQuery: { type: "string" },
      exemptionsQuery: { type: "string" },
      question: { type: "string" },
      isPrivileged: { type: "boolean" },
      checkApprovals: { type: "boolean" },
      followUps: { type: "array", items: { type: "string" }, minItems: 0, maxItems: 5 },
    },
    required: [
      "company",
      "searchQuery",
      "exemptionsQuery",
      "question",
      "isPrivileged",
      "checkApprovals",
      "followUps",
    ],
  };

  const system = [
    "You convert Freshservice ticket text into an optimal Guru query package.",
    "Company MUST always be included in searchQuery, exemptionsQuery, and question.",
    "Classify isPrivileged=true if the request involves protected/privileged actions or data, including:",
    "- access delegation (calendar/mailbox/files), admin permissions, security reports, credentials, MFA, SSO, provisioning, PII.",
    "Approvals should only be considered when BOTH: vip=false AND isPrivileged=true.",
    "Set checkApprovals accordingly.",
    "exemptionsQuery MUST target the company's Exemptions/Exceptions List card and include key terms from the ticket.",
    "Make searchQuery concise and high-signal (include product/system + action + company).",
    "Return ONLY JSON matching the schema (no markdown).",
  ].join("\n");

  const payload = { subject, description, company, vip };

  const resp = await openai.responses.create({
    model: OPENAI_MODEL,
    input: [
      { role: "system", content: system },
      { role: "user", content: JSON.stringify(payload) },
    ],
    text: {
      format: {
        type: "json_schema",
        name: "guru_query_package",
        strict: true,
        schema: schema,
      },
    },
  });

  return JSON.parse(resp.output_text);
}

/** -----------------------------
 * Runbook builder
 * ----------------------------- */
async function answerToRunbook({ company, subject, description, answerText, vip, isPrivileged, approvalGate }) {
  const schema = {
    type: "object",
    additionalProperties: false,
    properties: {
      title: { type: "string" },
      summary: { type: "string" },
      prerequisites: { type: "array", items: { type: "string" }, minItems: 0, maxItems: 5 },
      steps: {
        type: "array",
        minItems: 3,
        maxItems: 7,
        items: {
          type: "object",
          additionalProperties: false,
          properties: {
            step: { type: "integer" },
            action: { type: "string" },
            details: { type: "string" },
            verification: { type: "string" },
          },
          required: ["step", "action", "details", "verification"],
        },
      },
      approvals: {
        type: "object",
        additionalProperties: false,
        properties: {
          required: { type: "boolean" },
          rationale: { type: "string" },
        },
        required: ["required", "rationale"],
      },
      escalation: { type: "array", items: { type: "string" }, minItems: 0, maxItems: 3 },
      notes: { type: "array", items: { type: "string" }, minItems: 0, maxItems: 4 },
    },
    required: ["title", "summary", "prerequisites", "steps", "approvals", "escalation", "notes"],
  };

  const system = [
    "You convert a support answer into a short step-by-step runbook for an IT technician.",
    "Do NOT invent tools, policies, approvers, or steps not supported by the provided answerText or ticket details.",
    "If the platform is unknown (Google vs Microsoft 365), add a prerequisite/step to confirm it.",
    "Keep details + verification to ONE sentence each.",
    "",
    "Approval rules:",
    "- approvals.required MUST be true ONLY if answerText explicitly says approvals are required for THIS request, OR if a provided policy excerpt explicitly requires it.",
    "- If approvalGate=true but answerText does NOT explicitly mention approvals, set approvals.required=false and rationale='No explicit approval requirement found in scoped sources; confirm if needed.'",
    "- NEVER reference Egnyte/file storage approvers unless answerText explicitly mentions them.",
    "",
    "Return ONLY JSON matching the schema.",
  ].join("\n");

  const userPayload = { company, subject, description, vip, isPrivileged, approvalGate, answerText };

  const resp = await openai.responses.create({
    model: OPENAI_MODEL,
    input: [
      { role: "system", content: system },
      { role: "user", content: JSON.stringify(userPayload) },
    ],
    text: {
      format: {
        type: "json_schema",
        name: "runbook",
        strict: true,
        schema,
      },
    },
  });

  return JSON.parse(resp.output_text);
}

/** -----------------------------
 * Pipeline: run the full Guru/OpenAI workflow and post to Freshservice
 * ----------------------------- */
async function runPipelineAndPostNote({ ticketId, company, subject, description, vip }) {
  // 1) Build query package via OpenAI
  const qp = await buildQueryPackage({ subject, description, company, vip });

  // 2) Exemptions/Exceptions List lookup first
  const exSearchRaw = await guruToolCall({
    name: "guru_search_documents",
    args: {
      query: qp.exemptionsQuery,
      agentId: GURU_AGENT_ID,
    },
  });
  const exResults = coerceSearchResultsToArray(exSearchRaw);

  // Determine company board/folder id
  let companyBoardId = findCompanyBoardIdFromResults(exResults, company);

  // If not found, discover company board by searching company name broadly
  if (!companyBoardId) {
    const companySearchRaw = await guruToolCall({
      name: "guru_search_documents",
      args: {
        query: `${company}`,
        agentId: GURU_AGENT_ID,
      },
    });
    const companyResults = coerceSearchResultsToArray(companySearchRaw);
    companyBoardId = findCompanyBoardIdFromResults(companyResults, company);
  }

  // Pick best exemptions card candidate
  let exCard = null;
  let bestScore = 0;
  for (const c of exResults) {
    const s = scoreExemptionsCard(c);
    if (s > bestScore) {
      bestScore = s;
      exCard = c;
    }
  }

  const exCardId =
    exCard && isInternalOrClientCompanyCard(exCard, companyBoardId, company) ? exCard.id : null;

  let exemptionsText = "";
  let exCardFull = null;
  if (exCardId) {
    exCardFull = await guruToolCall({
      name: "guru_get_card_by_id",
      args: { id: exCardId },
    });

    // Prefer content if present; else stringify
    exemptionsText =
      typeof exCardFull?.content === "string"
        ? exCardFull.content
        : JSON.stringify(exCardFull, null, 2);
  }

  const exemptionRelevant = exemptionsLikelyRelevant(exemptionsText, subject, description);

  // 3) Approval gating: ONLY if non-vip AND privileged/protected
  const shouldConsiderApprovals = vip === false && qp.isPrivileged === true;

  // 4) Search policy docs; hard-filter to allowed scope
  const approvalTail = shouldConsiderApprovals ? " calendar delegate access approval" : "";
  const policySearchRaw = await guruToolCall({
    name: "guru_search_documents",
    args: {
      query: `${qp.searchQuery}${approvalTail}`,
      agentId: GURU_AGENT_ID,
    },
  });
  const policyResults = coerceSearchResultsToArray(policySearchRaw);

  // If company board still unknown, infer from policy results
  if (!companyBoardId) companyBoardId = findCompanyBoardIdFromResults(policyResults, company);

  const scopedPolicyResults = policyResults.filter((c) =>
    isInternalOrClientCompanyCard(c, companyBoardId, company)
  );

  const scopedContextSummary = scopedPolicyResults
    .slice(0, 10)
    .map((c) => {
      const col = c?.collection?.name || c?.collection?.id || "UnknownCollection";
      const board = c?.boards?.[0]?.title ? ` / ${c.boards[0].title}` : "";
      const label = c?.preferredPhrase || c?.title || c?.slug || c?.id;
      return `- ${label} (${col}${board}) [id=${c?.id}]`;
    })
    .join("\n");

  // 5) Generate final answer (extra guardrail: collectionIds)
  const instructions = [
    `You are an internal IT support assistant answering a Freshservice ticket.`,
    `Company: ${company}. VIP requester: ${vip ? "YES" : "NO"}.`,
    `STRICT SOURCE RULE: Only use knowledge from (a) Internal collection, or (b) Clients collection within the ${company} folder/board. If you cannot find an answer within those sources, say so.`,
    exCardId
      ? `Company Exemptions/Exceptions List card was found (id=${exCardId}). Apply relevant exemptions if they match this request.`
      : `No company Exemptions/Exceptions List card was found within allowed scope.`,
    shouldConsiderApprovals
      ? `Because this is a non-VIP privileged/protected request, verify whether approvals are required BEFORE giving execution steps. If exemptions explicitly waive approvals, state that and cite it.`
      : `Do NOT introduce approval steps unless an explicit Guru policy within allowed sources requires it.`,
    `Provide: (1) recommended steps, (2) approvals if applicable, (3) up to 2 follow-up questions if needed, (4) cite the Guru card titles/ids you relied on.`,
    `Scoped candidate sources:\n${scopedContextSummary || "- (none found; broaden search terms slightly but remain within allowed sources)"}`
  ].join("\n");

  const finalQuestion = [
    qp.question,
    "",
    `Ticket Subject: ${subject}`,
    `Ticket Description: ${description}`,
    "",
    `System instruction:\n${instructions}`,
    exemptionRelevant && exemptionsText ? `\nRelevant Exemptions/Exceptions List content:\n${exemptionsText}` : "",
  ].join("\n");

  const answerRaw = await guruToolCall({
    name: "guru_answer_generation",
    args: {
      question: finalQuestion,
      agentId: GURU_AGENT_ID,
      collectionIds: [CLIENTS_COLLECTION_ID, INTERNAL_COLLECTION_ID],
    },
  });

  const { answerText, sources } = formatAnswerResult(answerRaw);

  // 6) Enforce scope on returned sources
  const { allowed: allowedSources, rejected: rejectedSources } =
    await filterAnswerSourcesToAllowedScope(sources, companyBoardId, company);

  const contextSources = [
    ...(scopedPolicyResults || []).slice(0, 8).map(cardToSource).filter(Boolean),
  ];

  if (exCardId) {
    // if you already fetched exCardFull you can cardToSource(exCardFull)
    // otherwise, add a minimal entry and let the URL be omitted
    if (exCardFull) {
      const exSource = cardToSource(exCardFull);
      if (exSource) contextSources.unshift(exSource);
    } else {
      contextSources.unshift({ id: exCardId, title: "Exemptions/Exceptions List" });
    }
  }

  const sourcesUsed = dedupeSourcesById([
    ...(allowedSources || []),
    ...contextSources,
  ]);

  const sourcesHtml = renderSourcesAsHtmlLinks(sourcesUsed);

  let runbook;
  if (answerText) {
    runbook = await answerToRunbook({
      company,
      subject,
      description,
      answerText,
      vip,
      isPrivileged: qp.isPrivileged,
      approvalGate: shouldConsiderApprovals,
    });
  } else {
    runbook = {
      title: `No scoped answer found`,
      summary: `No scoped Guru answer was returned. Try adjusting the query terms while staying within allowed sources.`,
      prerequisites: [],
      steps: [
        { step: 1, action: "Confirm request details", details: "Clarify platform, delegate identity, and permission level.", verification: "Ticket contains all required details." },
        { step: 2, action: "Search scoped Guru sources", details: "Search Internal + the company folder for platform-specific steps or policy.", verification: "At least 1 relevant scoped source identified." },
        { step: 3, action: "Proceed or escalate", details: "If no policy exists, follow standard platform procedure or escalate to the client's IT owner.", verification: "Next action is documented." },
      ],
      approvals: { required: false, rationale: "No explicit approval requirement found in scoped sources; confirm if needed." },
      escalation: [],
      notes: [],
    };
  }

  const note = renderRunbookHtml({
    company,
    runbook,
    sourcesHtml,
    followUps: qp.followUps,
    signals: {
      vip: vip ? "true" : "false",
      privileged: qp.isPrivileged ? "true" : "false",
      approvalGate: shouldConsiderApprovals ? "true" : "false",
      exemptionsFound: exCardId ? "true" : "false",
      companyBoardId,
    },
  });

  // 7) Post back to Freshservice
  await freshserviceAddPrivateNote(ticketId, note);
}

async function processOneJob(job) {
  // This should be the body of what your old webhook handler did:
  // 1) buildQueryPackage
  // 2) exemptions search + companyBoardId detect
  // 3) policy search (scoped)
  // 4) guru_answer_generation
  // 5) runbook builder
  // 6) add Freshservice private note
  //
  // You already have that code — we've moved it into a function.
  await runPipelineAndPostNote(job);
}

export default async function handler(req, res) {
  try {
    // Allow Vercel Cron (GET) and manual trigger (POST)
    if (req.method !== "GET" && req.method !== "POST") {
      return res.status(405).json({ error: "Method not allowed" });
    }

    const ua = req.headers["user-agent"] || "";
    const isCron =
  req.headers["x-vercel-cron"] != null ||
  ua.includes("vercel-cron/1.0");

    // If it's not cron, require WORKER_KEY
    if (!isCron) {
      const key = req.headers["x-worker-key"];
      if (key !== WORKER_KEY) return res.status(401).json({ error: "Unauthorized" });
    }

    // maxJobs from POST body or GET query (?maxJobs=3)
    const maxJobsRaw =
      req.method === "POST"
        ? req.body?.maxJobs
        : (req.query?.maxJobs || req.url?.split("maxJobs=")[1]);

    const maxJobs = Math.max(1, Math.min(5, Number(maxJobsRaw || 1)));

    const processed = [];
    for (let i = 0; i < maxJobs; i++) {
      const job = await dequeueJob();
      if (!job) break;

      const locked = await acquireLock(job.jobId, 180);
      if (!locked) {
        // Someone else is processing; skip
        continue;
      }

      try {
        await processOneJob(job);
        await markDone(job.jobId, 6 * 3600);
        processed.push(job.jobId);
      } catch (e) {
        console.error("Worker job failed", job.jobId, e);
        // Optional: push to a dead-letter queue key here
      } finally {
        await releaseLock(job.jobId);
      }
    }

    return res.status(200).json({ ok: true, processed });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: String(e?.message || e) });
  }
}

