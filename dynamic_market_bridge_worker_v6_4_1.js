// dynamic_market_bridge_worker.js
// V6.4.1 candidate anomaly fast-lane edition
// Cloudflare Worker: OKX + Binance -> GitHub dynamic crypto bridge
//
// Model architecture:
// 1) Fixed market benchmark: BTC + ETH
// 2) Core execution coin: SOL
// 3) Dynamic opportunity pool: auto-select from OKX USDT perpetual swaps
//
// Technical framework:
// - 15m + 1h
// - MA7 / MA25 / MA60
// - market structure
// - support / resistance
// - volume anomaly ratio
//
// Data quality:
// - retry up to 3 times
// - request budget prevents Cloudflare subrequest-limit crashes
// - OKX + Binance dual-exchange discovery and confirmation
// - Binance resilient sidecar: 403/429/5xx/timeout never aborts runBridge
// - Binance is retried every Cron run and automatically rejoins dual-exchange discovery when healthy
// - layered Stage-B scheduler: core fresh first, only top dynamic candidates spend candle requests
// - recent cached dynamic snapshots can compete without consuming new subrequests
// - OKX request pacing smooths Stage-B traffic to reduce HTTP 429 rate limits
// - 429 retries use stronger exponential backoff while preserving request budget
// - V6.3.1 dual mode: Cron Lite for Cloudflare Free CPU limit; manual /run remains full pipeline
// - V6.3.1 Cron Lite round-robins BTC/ETH/SOL: one core symbol per 5m run, each refreshed every 15m
// - V6.4.0 Bitget always covers BTC/ETH/SOL plus additional dynamic candidates
// - Cron Lite carries the last Bitget evidence forward as an explicitly stale snapshot
// - Bitget contributes a bounded opportunity score only when at least two independent fields agree
// - No single Bitget field can change ranking, direction, trade triggers, or failure state
// - OKX instrument-category purification (crypto only)
// - stale fallback to prior GitHub data
// - partial / failedSymbols / staleSymbols / lastSuccessfulAt
//
// Required Worker secrets:
//   GITHUB_TOKEN
//
// Optional Worker vars:
//   GITHUB_OWNER=zhaorui0521-pixel
//   GITHUB_REPO=hype-market-data
//   GITHUB_BRANCH=main
//   FIXED_SYMBOLS=BTC-USDT-SWAP,ETH-USDT-SWAP,SOL-USDT-SWAP
//   WATCH_SYMBOLS=HYPE-USDT-SWAP,XRP-USDT-SWAP,ENA-USDT-SWAP,DOGE-USDT-SWAP
//   DYNAMIC_COUNT=10
//   PRESELECT_COUNT=14
//   MAX_STAGE_B_SYMBOLS=12
//   SUBREQUEST_BUDGET=46
//   DYNAMIC_FRESH_ANALYSIS_COUNT=5
//   CORE_TIMEFRAME_ATTEMPTS=2
//   DYNAMIC_TIMEFRAME_ATTEMPTS=1
//   ANOMALY_RECHECK_MAX_SYMBOLS=4
//   OKX_STAGE_B_START_DELAY_MS=550
//   OKX_INTER_TIMEFRAME_DELAY_MS=320
//   OKX_INTER_SYMBOL_DELAY_MS=380
//   OKX_429_BASE_BACKOFF_MS=1200
//   OKX_SIDECAR_URL=https://your-sidecar.example.com
//   BINANCE_SIDECAR_URL=https://your-sidecar.example.com
//   BITGET_SIDECAR_URL=https://your-sidecar.example.com  // falls back to Binance/OKX sidecar URL
//   BITGET_EVIDENCE_MAX_SYMBOLS=6
//   SIDECAR_TOKEN=<shared-secret>  // optional but recommended
//   REQUIRE_DUAL_EXCHANGE=false
//   # If Binance is unavailable, REQUIRE_DUAL_EXCHANGE is automatically relaxed for that run.
//   SINGLE_EXCHANGE_MIN_TURNOVER_USD=100000000
//   EXCLUDED_BASES=SOXL
//   MIN_TURNOVER_USD=20000000
//   MIN_OI_USD=5000000
//   MIN_LIQUIDITY_PERCENTILE=0.35
//   MIN_OI_PERCENTILE=0.25
//   RUN_SECRET=xxxx
//
// Recommended Cron:
//   */5 * * * *

const OKX_BASE = "https://www.okx.com";

const DEFAULT_FIXED = [
  "BTC-USDT-SWAP",
  "ETH-USDT-SWAP",
  "SOL-USDT-SWAP"
];

const DEFAULT_BITGET_CORE = [
  "BTC-USDT-SWAP",
  "ETH-USDT-SWAP",
  "SOL-USDT-SWAP"
];

const DEFAULT_BITGET_EVIDENCE_MAX_SYMBOLS = 6;
const BITGET_SCORE_WEIGHT = 0.10;

const DEFAULT_WATCH = [
  "HYPE-USDT-SWAP",
  "XRP-USDT-SWAP",
  "ENA-USDT-SWAP",
  "DOGE-USDT-SWAP"
];

// Dynamic-pool quality floors. These can be overridden with Worker vars.
// Purpose: prevent tiny, illiquid coins from winning merely because their 24h range is extreme.
const DEFAULT_MIN_TURNOVER_USD = 20_000_000;
const DEFAULT_MIN_OI_USD = 5_000_000;
const DEFAULT_MIN_LIQUIDITY_PERCENTILE = 0.35;
const DEFAULT_MIN_OI_PERCENTILE = 0.25;

// Source-freshness thresholds.
const MAX_TICKER_AGE_MS = 120_000;
const MAX_OI_AGE_MS = 180_000;

// Cloudflare subrequest protection.
// Keep a safety margin below the platform ceiling so retries and GitHub writes
// cannot make the whole invocation fail with "Too many subrequests".
const DEFAULT_SUBREQUEST_BUDGET = 46;
const DEFAULT_STAGE_B_SYMBOL_LIMIT = 13;
const GITHUB_WRITE_RESERVE = 6;

// V6.2.4 layered scheduler.
// Only the strongest few dynamic candidates receive fresh 15m/1h requests
// on each run. The rest may compete using recent <=10m cached snapshots.
const DEFAULT_DYNAMIC_FRESH_ANALYSIS_COUNT = 5;
const DEFAULT_CORE_TIMEFRAME_ATTEMPTS = 2;
const DEFAULT_DYNAMIC_TIMEFRAME_ATTEMPTS = 1;
const DYNAMIC_FRESH_REQUEST_ESTIMATE = 3; // ticker refresh + 15m + 1h worst normal case

// V6.4.1 candidate anomaly fast lane. Each lightweight recheck uses only two
// Binance requests: three 5m klines (price + taker buy/sell) and three 5m OI
// samples (5m/10m deltas). A candidate must have at least two mutually
// consistent signals; no single field can promote it into full analysis.
const DEFAULT_ANOMALY_RECHECK_MAX_SYMBOLS = 4;
const ANOMALY_RECHECK_REQUEST_ESTIMATE = 2;
const ANOMALY_PRICE_5M_PCT = 0.65;
const ANOMALY_PRICE_10M_PCT = 1.0;
const ANOMALY_OI_5M_PCT = 0.6;
const ANOMALY_OI_10M_PCT = 1.0;
const ANOMALY_TAKER_BULL = 1.18;
const ANOMALY_TAKER_BEAR = 0.85;

// V6.2.5 OKX pacing.
// Stage A produces several quick public requests; pause briefly before Stage B,
// then space timeframe and symbol requests so BTC/ETH/SOL do not collide with
// OKX's short-window rate limits.
const DEFAULT_OKX_STAGE_B_START_DELAY_MS = 550;
const DEFAULT_OKX_INTER_TIMEFRAME_DELAY_MS = 320;
const DEFAULT_OKX_INTER_SYMBOL_DELAY_MS = 380;

const DEFAULT_OKX_429_BASE_BACKOFF_MS = 1200;

// V6.3 transport layer:
// Prefer sidecars when configured, otherwise fall back to direct exchange access.
// Sidecar protocol:
//   GET <BASE_URL>/proxy?exchange=okx&path=<encoded path>
//   GET <BASE_URL>/proxy?exchange=binance&path=<encoded path>
// The sidecar must return the upstream JSON body and preserve an HTTP-like status.
let ACTIVE_OKX_SIDECAR_URL = "";
let ACTIVE_BINANCE_SIDECAR_URL = "";
let ACTIVE_BITGET_SIDECAR_URL = "";
let ACTIVE_SIDECAR_TOKEN = "";

function normalizeBaseUrl(value) {
  return String(value || "")
    .trim()
    .replace(/\/+$/, "");
}

function applyTransportEnv(env) {
  ACTIVE_OKX_SIDECAR_URL =
    normalizeBaseUrl(
      env.OKX_SIDECAR_URL
    );

  ACTIVE_BINANCE_SIDECAR_URL =
    normalizeBaseUrl(
      env.BINANCE_SIDECAR_URL
    );

  ACTIVE_BITGET_SIDECAR_URL =
    normalizeBaseUrl(
      env.BITGET_SIDECAR_URL ||
      env.BINANCE_SIDECAR_URL ||
      env.OKX_SIDECAR_URL
    );

  ACTIVE_SIDECAR_TOKEN =
    String(
      env.SIDECAR_TOKEN ||
      ""
    ).trim();
}

function sidecarHeaders() {
  const headers = {
    "Accept": "application/json",
    "User-Agent":
      "dynamic-market-bridge-worker/6.3.1"
  };

  if (ACTIVE_SIDECAR_TOKEN) {
    headers["Authorization"] =
      `Bearer ${ACTIVE_SIDECAR_TOKEN}`;
  }

  return headers;
}

async function sidecarFetchJson(
  baseUrl,
  exchange,
  path,
  subrequestBudget = null,
  timeoutMs = 6000
) {
  if (!baseUrl) {
    throw new Error(
      `${exchange} sidecar not configured`
    );
  }

  consumeSubrequest(
    subrequestBudget,
    `${exchange} sidecar ${path}`
  );

  const controller =
    new AbortController();

  const timer =
    setTimeout(
      () => controller.abort(),
      timeoutMs
    );

  try {
    const url =
      `${baseUrl}/proxy?exchange=${encodeURIComponent(exchange)}&path=${encodeURIComponent(path)}`;

    const resp =
      await fetch(
        url,
        {
          signal:
            controller.signal,
          headers:
            sidecarHeaders()
        }
      );

    const text =
      await resp.text();

    let body;

    try {
      body =
        text
          ? JSON.parse(text)
          : {};
    } catch {
      body = {
        raw: text
      };
    }

    if (!resp.ok) {
      const err =
        new Error(
          `${exchange} sidecar HTTP ${resp.status}`
        );

      err.status =
        resp.status;

      err.body =
        body;

      throw err;
    }

    if (
      body &&
      typeof body === "object" &&
      body.ok === false &&
      body.upstreamStatus
    ) {
      const err =
        new Error(
          `${exchange} upstream HTTP ${body.upstreamStatus}`
        );

      err.status =
        body.upstreamStatus;

      err.body =
        body;

      throw err;
    }

    // Sidecar may wrap the upstream payload in `data`,
    // or may return upstream JSON directly.
    return (
      body?.data !== undefined &&
      body?.sidecar === true
    )
      ? body.data
      : body;

  } finally {
    clearTimeout(
      timer
    );
  }
}

function toBitgetSymbol(symbol) {
  const base =
    baseFromOkxSymbol(symbol);

  return base
    ? `${base}USDT`
    : null;
}

function unavailableBitgetEvidence(
  status,
  requestedSymbols,
  error = null
) {
  return {
    ok: false,
    status,
    provider:
      "bitget-public-market-data",
    mode:
      "formal_multi_field_scoring",
    observedOnly: false,
    affectsScore: false,
    scorePolicy: {
      minimumIndependentDirectionalFields: 2,
      maxWeight: BITGET_SCORE_WEIGHT,
      singleFieldCanAffectScore: false,
      canTriggerTrade: false
    },
    requestedSymbols,
    healthyCount: 0,
    generatedAt:
      new Date().toISOString(),
    error,
    symbols: {}
  };
}

async function fetchBitgetEvidence(
  okxSymbols,
  subrequestBudget = null
) {
  const symbols =
    unique(
      okxSymbols
        .map(toBitgetSymbol)
        .filter(Boolean)
    );

  if (!symbols.length) {
    return unavailableBitgetEvidence(
      "no_symbols",
      []
    );
  }

  if (!ACTIVE_BITGET_SIDECAR_URL) {
    return unavailableBitgetEvidence(
      "not_configured",
      symbols,
      "Bitget sidecar URL is not configured"
    );
  }

  if (
    !canSpendSubrequest(
      subrequestBudget,
      1
    )
  ) {
    return unavailableBitgetEvidence(
      "skipped_budget",
      symbols,
      "Non-reserved subrequest budget exhausted"
    );
  }

  consumeSubrequest(
    subrequestBudget,
    "Bitget shadow evidence"
  );

  const controller =
    new AbortController();

  const timer =
    setTimeout(
      () => controller.abort(),
      12000
    );

  try {
    const url =
      `${ACTIVE_BITGET_SIDECAR_URL}/bitget/evidence?symbols=${encodeURIComponent(symbols.join(","))}&period=1h&funding=true`;

    const response =
      await fetch(
        url,
        {
          signal:
            controller.signal,
          headers:
            sidecarHeaders()
        }
      );

    const text =
      await response.text();

    let body;

    try {
      body = text
        ? JSON.parse(text)
        : {};
    } catch {
      body = {
        raw: text
      };
    }

    if (!response.ok) {
      return unavailableBitgetEvidence(
        `sidecar_http_${response.status}`,
        symbols,
        body?.error ||
        body?.raw ||
        "Bitget sidecar request failed"
      );
    }

    return {
      ...body,
      provider:
        "bitget-public-market-data",
      mode:
        "formal_multi_field_scoring",
      observedOnly: false,
      affectsScore: true,
      scorePolicy: {
        minimumIndependentDirectionalFields: 2,
        maxWeight: BITGET_SCORE_WEIGHT,
        singleFieldCanAffectScore: false,
        canTriggerTrade: false
      }
    };

  } catch (error) {
    return unavailableBitgetEvidence(
      error?.name === "AbortError"
        ? "timeout"
        : "unavailable",
      symbols,
      String(
        error?.message ||
        error
      )
    );

  } finally {
    clearTimeout(timer);
  }
}

function bitgetDirectionalSignal(value, lower, upper) {
  const number = Number(value);

  if (!Number.isFinite(number)) return null;
  if (number > upper) return 1;
  if (number < lower) return -1;
  return 0;
}

function bitgetModelScoreForSymbol(bitgetEvidence, okxSymbol) {
  const bitgetSymbol = toBitgetSymbol(okxSymbol);
  const evidence = bitgetEvidence?.symbols?.[bitgetSymbol];

  if (!evidence || evidence.status === "failed") {
    return {
      eligible: false,
      opportunityScore: null,
      directionalConsensus: "unavailable",
      independentDirectionalFields: 0,
      agreeingDirectionalFields: 0,
      effectiveWeight: 0,
      reason: "Bitget evidence unavailable"
    };
  }

  const signals = [
    {
      field: "activeBuySell",
      signal: bitgetDirectionalSignal(
        evidence.activeBuySell?.buySellRatio,
        0.95,
        1.05
      )
    },
    {
      field: "topPositionRatio",
      signal: bitgetDirectionalSignal(
        evidence.topPositionRatio?.longShortRatio,
        0.95,
        1.05
      )
    },
    {
      field: "spotWhaleFlow",
      signal: bitgetDirectionalSignal(
        evidence.spotWhaleFlow?.netVolume,
        0,
        0
      )
    }
  ].filter(item => item.signal !== null && item.signal !== 0);

  const bullish = signals.filter(item => item.signal > 0).length;
  const bearish = signals.filter(item => item.signal < 0).length;
  const agreeingDirectionalFields = Math.max(bullish, bearish);
  const eligible = signals.length >= 2 && agreeingDirectionalFields >= 2;
  const agreement = signals.length
    ? agreeingDirectionalFields / signals.length
    : 0;

  return {
    eligible,
    opportunityScore: eligible ? round6(agreement) : null,
    directionalConsensus: !eligible
      ? "insufficient_or_conflicting"
      : bullish > bearish
        ? "bullish"
        : "bearish",
    independentDirectionalFields: signals.length,
    agreeingDirectionalFields,
    effectiveWeight: eligible ? BITGET_SCORE_WEIGHT : 0,
    reason: eligible
      ? `multi-field ${bullish > bearish ? "bullish" : "bearish"} agreement ${agreeingDirectionalFields}/${signals.length}`
      : `requires >=2 agreeing independent fields; observed=${signals.length}; agreeing=${agreeingDirectionalFields}`,
    fields: signals
  };
}



let ACTIVE_OKX_429_BASE_BACKOFF_MS =
  DEFAULT_OKX_429_BASE_BACKOFF_MS;

function okxRateLimitBackoffBase() {
  return ACTIVE_OKX_429_BASE_BACKOFF_MS;
}


const BINANCE_FAPI_BASE = "https://fapi.binance.com";
const BINANCE_TIMEOUT_MS = 4500;
const DEFAULT_SINGLE_EXCHANGE_MIN_TURNOVER_USD = 100_000_000;

// Hard safety exclusions are only a last-resort guardrail.
// Primary purification uses OKX instCategory === "1" and Binance contract metadata.
const DEFAULT_EXCLUDED_BASES = [
  "SOXL"
];

export default {
  async scheduled(event, env, ctx) {
    const scheduledTime =
      Number(
        event?.scheduledTime ||
        Date.now()
      );
    const isHourlyFullRun =
      new Date(
        scheduledTime
      ).getUTCMinutes() === 0;

    // Every UTC hour boundary runs the same full dynamic discovery + scoring
    // pipeline as /run. The other 5-minute slots retain the lightweight core
    // refresh, but no longer overwrite latest.json / summary.json.
    ctx.waitUntil(
      isHourlyFullRun
        ? runBridge(env)
        : runCronLite(
            env,
            scheduledTime
          )
    );
  },

  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === "/run") {
      if (env.RUN_SECRET) {
        const auth = request.headers.get("Authorization") || "";
        if (auth !== `Bearer ${env.RUN_SECRET}`) {
          return jsonResponse({ ok: false, error: "unauthorized" }, 401);
        }
      }

      try {
        const result = await runBridge(env);
        return jsonResponse(result);
      } catch (err) {
        return jsonResponse({
          ok: false,
          error: String(err?.stack || err)
        }, 500);
      }
    }

    return jsonResponse({
      ok: true,
      service: "dynamic-market-bridge",
      architecture: {
        benchmark: ["BTC", "ETH"],
        coreExecution: ["SOL"],
        dynamic: "purified OKX crypto USDT perpetual discovery with Binance USDⓈ-M cross-confirmation when available; automatic OKX-only degradation otherwise"
      },
      version: "6.4.1",
  maSystem: ["MA7", "MA25", "MA60"],
      note: "V6.4.1 adds a bounded multi-field anomaly fast lane before full dynamic analysis. Cron Lite behavior is unchanged. No single field can promote a candidate, affect a trade trigger, or define a failure state.",
        transport: "sidecar-first for OKX + Binance; Bitget formal evidence is aggregated by the sidecar"
    });
  }
};


// ============================================================
// V6.3.1 FREE-PLAN CRON LITE MODE
// ============================================================

const CRON_LITE_START_JITTER_MIN_MS = 700;
const CRON_LITE_START_JITTER_MAX_MS = 1200;
const CRON_LITE_TICKER_TO_15M_GAP_MS = 500;
const CRON_LITE_15M_TO_1H_GAP_MS = 700;
const CRON_LITE_INTER_SYMBOL_GAP_MS = 0;
const CRON_LITE_429_BACKOFF_MIN_MS = 1800;
const CRON_LITE_429_BACKOFF_MAX_MS = 3000;

function randomIntBetween(min, max) {
  return Math.floor(
    min +
    Math.random() *
      (max - min + 1)
  );
}

async function sleepRandom(min, max) {
  await sleep(
    randomIntBetween(
      min,
      max
    )
  );
}

async function okxGetCronLite(
  path,
  subrequestBudget = null
) {
  let lastErr = null;

  for (
    let attempt = 0;
    attempt < 2;
    attempt++
  ) {
    try {
      return await okxGet(
        path,
        1,
        subrequestBudget
      );
    } catch (err) {
      lastErr = err;

      const msg =
        String(
          err?.message ||
          err
        );

      const retryable429 =
        /HTTP 429|50011|rate limit/i
          .test(msg);

      if (
        !retryable429 ||
        attempt === 1
      ) {
        throw err;
      }

      await sleepRandom(
        CRON_LITE_429_BACKOFF_MIN_MS,
        CRON_LITE_429_BACKOFF_MAX_MS
      );
    }
  }

  throw lastErr ||
    new Error(
      "Cron Lite OKX request failed"
    );
}

//
// Why:
// - Cloudflare Free scheduled events can terminate at a very small CPU budget.
// - Full Stage-A universe parsing + ranking + deep structure analysis is kept
//   for manual /run.
// - Cron does only BTC/ETH/SOL with 70 closed candles per timeframe and a
//   lightweight MA/support/resistance/volume calculation.
// - Binance is still probed once per Cron so the line is not abandoned.
// - latest.json / summary.json are intentionally compact in Cron mode.
//   A manual /run will replace them with the full dynamic-pool payload again.
//

function buildCronBitgetEvidence(
  storedBitgetState,
  carriedAt
) {
  const storedBitgetEvidence =
    storedBitgetState?.bitgetEvidence &&
    typeof storedBitgetState.bitgetEvidence ===
      "object"
      ? storedBitgetState.bitgetEvidence
      : null;

  if (!storedBitgetEvidence) {
    return {
      ok: false,
      status: "unavailable",
      provider:
        "bitget-public-market-data",
      mode:
        "formal_multi_field_scoring",
      observedOnly: true,
      affectsScore: false,
      requestedSymbols: [],
      healthyCount: 0,
      freshness: {
        state: "missing",
        sourceRunId: null,
        sourceUpdatedAt: null,
        carriedAt,
        ageMinutes: null
      }
    };
  }

  const sourceUpdatedAt =
    storedBitgetState.updatedAt ||
    storedBitgetEvidence.generatedAt ||
    null;

  const sourceUpdatedAtMs =
    Date.parse(
      sourceUpdatedAt ||
      ""
    );

  const carriedAtMs =
    Date.parse(
      carriedAt ||
      ""
    );

  const ageMinutes =
    Number.isFinite(
      sourceUpdatedAtMs
    ) &&
    Number.isFinite(
      carriedAtMs
    )
      ? Number(
          Math.max(
            0,
            (
              carriedAtMs -
              sourceUpdatedAtMs
            ) /
            60_000
          ).toFixed(3)
        )
      : null;

  return {
    ...storedBitgetEvidence,
    status:
      "carried_forward",
    sourceStatus:
      storedBitgetEvidence.status ||
      null,
    observedOnly: true,
    affectsScore: false,
    freshness: {
      state:
        "carried_forward",
      sourceRunId:
        storedBitgetState.runId ||
        null,
      sourceUpdatedAt,
      carriedAt,
      ageMinutes
    }
  };
}

async function runCronLite(
  env,
  scheduledTime = Date.now()
) {
  applyTransportEnv(env);
  await sleepRandom(
    CRON_LITE_START_JITTER_MIN_MS,
    CRON_LITE_START_JITTER_MAX_MS
  );

  const startedAt =
    new Date().toISOString();

  const runId =
    `${Date.now()}-${crypto.randomUUID()}`;

  const gh = {
    owner:
      env.GITHUB_OWNER ||
      "zhaorui0521-pixel",
    repo:
      env.GITHUB_REPO ||
      "hype-market-data",
    branch:
      env.GITHUB_BRANCH ||
      "main",
    token:
      env.GITHUB_TOKEN
  };

  if (!gh.token) {
    throw new Error(
      "Missing GITHUB_TOKEN"
    );
  }

  const fixedSymbols =
    parseCsv(
      env.FIXED_SYMBOLS,
      DEFAULT_FIXED
    ).slice(0, 3);

  const cronBudget =
    createSubrequestBudget(
      24,
      5
    );

  // Dedicated tiny cache: avoids decoding the much larger full /run payload.
  const [
    coreStateMeta,
    bitgetEvidenceStateMeta,
    binanceProbe
  ] =
    await Promise.all([
      githubGetJson(
        gh,
        "cron_core_state.json",
        cronBudget
      ).catch(() => null),

      githubGetJson(
        gh,
        "bitget_evidence_state.json",
        cronBudget
      ).catch(() => null),

      probeBinanceCronLite(
        cronBudget
      )
    ]);

  const previousState =
    coreStateMeta?.json &&
    typeof coreStateMeta.json ===
      "object"
      ? coreStateMeta.json
      : {};

  const previousMarkets =
    previousState.markets &&
    typeof previousState.markets ===
      "object"
      ? previousState.markets
      : {};

  // Deterministic 5-minute round-robin:
  // slot 0 BTC, slot 1 ETH, slot 2 SOL, then repeat.
  const slotNumber =
    Math.floor(
      Number(scheduledTime) /
      (5 * 60 * 1000)
    );

  const refreshIndex =
    fixedSymbols.length
      ? (
          (
            slotNumber %
            fixedSymbols.length
          ) +
          fixedSymbols.length
        ) %
        fixedSymbols.length
      : 0;

  const refreshSymbol =
    fixedSymbols[
      refreshIndex
    ];

  const markets = {
    ...previousMarkets
  };

  let refreshError = null;

  if (refreshSymbol) {
    try {
      markets[
        refreshSymbol
      ] =
        await analyzeSymbolCronLite(
          refreshSymbol,
          cronBudget
        );
    } catch (err) {
      refreshError =
        String(
          err?.message ||
          err
        );

      const prior =
        previousMarkets[
          refreshSymbol
        ];

      if (prior) {
        markets[
          refreshSymbol
        ] = {
          ...prior,
          status:
            "cached_after_refresh_failure",
          stale:
            true,
          staleReason:
            `refresh_failed: ${refreshError}`,
          refreshFailedAt:
            new Date().toISOString()
        };
      } else {
        markets[
          refreshSymbol
        ] = {
          symbol:
            refreshSymbol,
          status: "failed",
          stale: true,
          error:
            refreshError,
          updatedAt:
            new Date().toISOString()
        };
      }
    }
  }

  const nowMs =
    Date.now();

  const failedSymbols = [];
  const staleSymbols = [];

  // Re-evaluate carried-forward market age without re-fetching it.
  for (
    const symbol of fixedSymbols
  ) {
    const market =
      markets[symbol];

    if (!market) {
      failedSymbols.push(
        symbol
      );
      continue;
    }

    if (
      market.status ===
        "failed"
    ) {
      failedSymbols.push(
        symbol
      );
      continue;
    }

    const sourceTs =
      Date.parse(
        market.updatedAt ||
        ""
      );

    const ageMinutes =
      Number.isFinite(
        sourceTs
      )
        ? Math.max(
            0,
            (
              nowMs -
              sourceTs
            ) /
            60_000
          )
        : Infinity;

    market.cacheAgeMinutes =
      Number.isFinite(
        ageMinutes
      )
        ? Number(
            ageMinutes.toFixed(
              3
            )
          )
        : null;

    market.sourceState =
      symbol ===
      refreshSymbol
        ? (
            refreshError
              ? "cached_after_refresh_failure"
              : "fresh_round_robin"
          )
        : "carried_forward";

    // Each symbol is targeted every 15 minutes.
    // <=10m is full weight; 10-30m is down-weighted by the model.
    if (
      ageMinutes >
      10
    ) {
      market.stale = true;

      if (
        !market.staleReason
      ) {
        market.staleReason =
          `cache_age_${ageMinutes.toFixed(1)}m`;
      }

      staleSymbols.push(
        symbol
      );
    } else if (
      market.stale
    ) {
      staleSymbols.push(
        symbol
      );
    }
  }

  const updatedAt =
    new Date().toISOString();

  const storedBitgetState =
    bitgetEvidenceStateMeta?.json &&
    typeof bitgetEvidenceStateMeta.json ===
      "object"
      ? bitgetEvidenceStateMeta.json
      : null;

  // Cron Lite intentionally makes no Bitget request. It carries the last
  // manual /run evidence forward so latest.json and summary.json retain the
  // evidence layer without spending another exchange subrequest.
  const bitgetEvidence =
    buildCronBitgetEvidence(
      storedBitgetState,
      updatedAt
    );

  const partial =
    failedSymbols.length > 0 ||
    staleSymbols.length > 0 ||
    !binanceProbe.available;

  const pool = {
    benchmarkSymbols:
      fixedSymbols.filter(
        x =>
          x.startsWith("BTC-") ||
          x.startsWith("ETH-")
      ),
    coreExecutionSymbols:
      fixedSymbols.filter(
        x =>
          x.startsWith("SOL-")
      ),
    fixedSymbols,
    dynamicSymbols: [],
    publishedSymbols:
      fixedSymbols.filter(
        x =>
          markets[x] &&
          markets[x].status !==
            "failed"
      ),
    evidenceLayer: {
      bitget: {
        mode:
          "formal_multi_field_scoring",
        observedOnly: false,
        affectsScore: true,
        minimumIndependentDirectionalFields: 2,
        maxScoreWeight: BITGET_SCORE_WEIGHT,
        singleFieldCanAffectScore: false,
        canTriggerTrade: false,
        cronLiteEnabled: false,
        cronBehavior:
          "carry_forward_only",
        status:
          bitgetEvidence.status,
        sourceStatus:
          bitgetEvidence.sourceStatus ||
          null,
        freshness:
          bitgetEvidence.freshness
      }
    },
    scheduler: {
      mode:
        "cron_lite_round_robin",
      fullMode:
        "manual_/run",
      scheduledTime:
        new Date(
          scheduledTime
        ).toISOString(),
      refreshIndex,
      refreshSymbol,
      refreshEveryMinutes:
        fixedSymbols.length *
        5,
      carriedForwardSymbols:
        fixedSymbols.filter(
          x =>
            x !==
            refreshSymbol
        ),
      candleLimit: 70,
      deepDynamicDiscovery:
        false,
      binanceProbeOnly:
        true,
      coreStateFile:
        "cron_core_state.json",
      okxPacing: {
        startJitterMs: [
          CRON_LITE_START_JITTER_MIN_MS,
          CRON_LITE_START_JITTER_MAX_MS
        ],
        tickerTo15mGapMs:
          CRON_LITE_TICKER_TO_15M_GAP_MS,
        fifteenTo1hGapMs:
          CRON_LITE_15M_TO_1H_GAP_MS,
        rateLimitBackoffMs: [
          CRON_LITE_429_BACKOFF_MIN_MS,
          CRON_LITE_429_BACKOFF_MAX_MS
        ],
        maxAttemptsPerRequest:
          2
      }
    }
  };

  const dataQuality = {
    failedSymbols,
    staleSymbols,
    closedCandlesOnly: true,
    cronCpuStrategy:
      "V6.3.1 round-robin: refresh only one of BTC/ETH/SOL per 5m Cron; carry the other two from tiny cron_core_state.json; each core target refreshes every 15m",
    refreshSymbol,
    refreshError,
    subrequestBudget: {
      max:
        cronBudget.max,
      usedBeforePublish:
        cronBudget.used,
      githubWriteReserve: 5
    }
  };

  const common = {
    ok: true,
    partial,
    mode:
      "cron_lite_round_robin",
    source:
      "sidecar-first exchange transport with direct fallback",
    transport: {
      okx:
        ACTIVE_OKX_SIDECAR_URL
          ? "sidecar_preferred"
          : "direct_only",
      binance:
        ACTIVE_BINANCE_SIDECAR_URL
          ? "sidecar_preferred"
          : "direct_only"
    },
    discoveryMode:
      binanceProbe.available
        ? "cron_round_robin_okx_plus_binance_probe"
        : "cron_round_robin_okx_only_degraded",
    binanceAvailable:
      binanceProbe.available,
    binanceStatus:
      binanceProbe.status,
    binanceLastError:
      binanceProbe.error,
    binanceAttemptedAt:
      binanceProbe.attemptedAt,
    runId,
    startedAt,
    updatedAt,
    pool,
    dataQuality,
    bitgetEvidence,
    markets
  };

  const coreState = {
    version:
      "6.4.1",
    mode:
      "cron_lite_round_robin",
    runId,
    partial,
    scheduledTime:
      new Date(
        scheduledTime
      ).toISOString(),
    updatedAt,
    lastRefreshSymbol:
      refreshSymbol,
    refreshError,
    failedSymbols,
    staleSymbols,
    binanceStatus:
      binanceProbe.status,
    bitgetEvidence,
    markets
  };

  const coreStateWrite =
    await githubPutJson(
      gh,
      "cron_core_state.json",
      coreState,
      coreStateMeta?.sha,
      `bridge: V6.4.1 core-state ${refreshSymbol || "none"} ${updatedAt}`,
      cronBudget
    );

  return {
    ok: true,
    partial,
    mode:
      "cron_lite_round_robin",
    runId,
    updatedAt,
    refreshSymbol,
    refreshError,
    fixedSymbols,
    failedSymbols,
    staleSymbols,
    binanceStatus:
      binanceProbe.status,
    subrequests: {
      used:
        cronBudget.used,
      max:
        cronBudget.max,
      remaining:
        Math.max(
          0,
          cronBudget.max -
          cronBudget.used
        )
    },
    github: {
      coreStateCommit:
        coreStateWrite.commitSha,
      latestCommit: null,
      summaryCommit: null,
      fullResultPreserved: true
    }
  };
}

async function analyzeSymbolCronLite(
  symbol,
  subrequestBudget
) {
  const tickerRaw =
    await okxGetCronLite(
      `/api/v5/market/ticker?instId=${encodeURIComponent(symbol)}`,
      subrequestBudget
    );

  const ticker =
    normalizeTicker(
      tickerRaw.data?.[0] ||
      {}
    );

  if (
    !ticker.symbol ||
    !ticker.last
  ) {
    throw new Error(
      `${symbol} ticker unavailable`
    );
  }

  // 70 is enough for MA60 while materially reducing JSON parse + loop work.
  await sleep(
    CRON_LITE_TICKER_TO_15M_GAP_MS
  );

  const raw15 =
    await okxGetCronLite(
      `/api/v5/market/candles?instId=${encodeURIComponent(symbol)}&bar=15m&limit=70`,
      subrequestBudget
    );

  await sleep(
    CRON_LITE_15M_TO_1H_GAP_MS
  );

  const raw1h =
    await okxGetCronLite(
      `/api/v5/market/candles?instId=${encodeURIComponent(symbol)}&bar=1H&limit=70`,
      subrequestBudget
    );

  const candles15 =
    normalizeCandles(
      raw15.data || []
    );

  const candles1h =
    normalizeCandles(
      raw1h.data || []
    );

  if (
    candles15.length < 60 ||
    candles1h.length < 60
  ) {
    throw new Error(
      `${symbol} insufficient Cron Lite candles`
    );
  }

  const timeframe15m =
    analyzeTimeframeCronLite(
      candles15
    );

  const timeframe1h =
    analyzeTimeframeCronLite(
      candles1h
    );

  const now = Date.now();

  const last15 =
    candles15.at(-1)?.ts ||
    0;

  const last1h =
    candles1h.at(-1)?.ts ||
    0;

  const stale15 =
    !isLatestClosedCandlePresent(
      last15,
      15 * 60_000,
      now
    );

  const stale1h =
    !isLatestClosedCandlePresent(
      last1h,
      60 * 60_000,
      now
    );

  const stale =
    stale15 ||
    stale1h;

  return {
    symbol,
    status:
      stale
        ? "stale"
        : "fresh",
    stale,
    staleReason:
      [
        stale15
          ? "15m_not_latest"
          : null,
        stale1h
          ? "1h_not_latest"
          : null
      ]
        .filter(Boolean)
        .join("; ") ||
      null,
    price:
      ticker.last,
    high24h:
      ticker.high24h,
    low24h:
      ticker.low24h,
    open24h:
      ticker.open24h,
    tickerTs:
      ticker.ts,
    timeframe15m,
    timeframe1h,
    updatedAt:
      new Date().toISOString()
  };
}

function analyzeTimeframeCronLite(
  candles
) {
  const closes =
    candles.map(
      x => x.close
    );

  const volumes =
    candles.map(
      x => x.volume
    );

  const latest =
    candles.at(-1);

  const ma7 =
    averageLast(
      closes,
      7
    );

  const ma25 =
    averageLast(
      closes,
      25
    );

  const ma60 =
    averageLast(
      closes,
      60
    );

  const recent20 =
    candles.slice(-20);

  let support =
    Infinity;

  let resistance =
    -Infinity;

  for (
    const x of recent20
  ) {
    if (
      x.low < support
    ) {
      support =
        x.low;
    }

    if (
      x.high >
      resistance
    ) {
      resistance =
        x.high;
    }
  }

  const previousVolumes =
    volumes.slice(
      -21,
      -1
    );

  const baseVolume =
    previousVolumes.length
      ? (
          previousVolumes.reduce(
            (a, b) =>
              a + b,
            0
          ) /
          previousVolumes.length
        )
      : 0;

  const volumeRatio =
    baseVolume > 0
      ? latest.volume /
        baseVolume
      : 0;

  const maState =
    ma7 > ma25 &&
    ma25 > ma60
      ? "bullish"
      : ma7 < ma25 &&
        ma25 < ma60
        ? "bearish"
        : "mixed";

  const structureSlice =
    candles.slice(-8);

  const firstHalf =
    structureSlice.slice(
      0,
      4
    );

  const secondHalf =
    structureSlice.slice(
      4
    );

  const firstHigh =
    Math.max(
      ...firstHalf.map(
        x => x.high
      )
    );

  const secondHigh =
    Math.max(
      ...secondHalf.map(
        x => x.high
      )
    );

  const firstLow =
    Math.min(
      ...firstHalf.map(
        x => x.low
      )
    );

  const secondLow =
    Math.min(
      ...secondHalf.map(
        x => x.low
      )
    );

  const structure =
    secondHigh >
      firstHigh &&
    secondLow >
      firstLow
      ? "HH_HL"
      : secondHigh <
          firstHigh &&
        secondLow <
          firstLow
        ? "LH_LL"
        : "range";

  return {
    lastClosedTs:
      latest.ts,
    lastClose:
      latest.close,
    ma7:
      round6(ma7),
    ma25:
      round6(ma25),
    ma60:
      round6(ma60),
    maState,
    structure,
    support:
      round6(
        support
      ),
    resistance:
      round6(
        resistance
      ),
    volumeRatio:
      round6(
        volumeRatio
      )
  };
}

function averageLast(
  values,
  count
) {
  const slice =
    values.slice(-count);

  if (!slice.length) {
    return 0;
  }

  return (
    slice.reduce(
      (a, b) =>
        a + b,
      0
    ) /
    slice.length
  );
}


async function binanceFetchJsonV63(
  path,
  subrequestBudget = null,
  timeoutMs = 5000
) {
  if (
    ACTIVE_BINANCE_SIDECAR_URL
  ) {
    try {
      return await sidecarFetchJson(
        ACTIVE_BINANCE_SIDECAR_URL,
        "binance",
        path,
        subrequestBudget,
        timeoutMs
      );
    } catch {
      // Fall through to direct Binance.
    }
  }

  consumeSubrequest(
    subrequestBudget,
    `Binance direct ${path}`
  );

  const controller =
    new AbortController();

  const timer =
    setTimeout(
      () =>
        controller.abort(),
      timeoutMs
    );

  try {
    const resp =
      await fetch(
        `https://fapi.binance.com${path}`,
        {
          signal:
            controller.signal,
          headers: {
            "Accept":
              "application/json",
            "User-Agent":
              "dynamic-market-bridge-worker/6.3.1"
          }
        }
      );

    if (!resp.ok) {
      const err =
        new Error(
          `Binance HTTP ${resp.status}`
        );

      err.status =
        resp.status;

      throw err;
    }

    return await resp.json();

  } finally {
    clearTimeout(
      timer
    );
  }
}

async function probeBinanceCronLite(
  subrequestBudget = null
) {
  const attemptedAt =
    new Date().toISOString();

  try {
    await binanceFetchJsonV63(
      "/fapi/v1/ping",
      subrequestBudget,
      3000
    );

    return {
      available: true,
      status:
        ACTIVE_BINANCE_SIDECAR_URL
          ? "available_via_sidecar"
          : "available_direct",
      error: null,
      attemptedAt
    };

  } catch (err) {
    const msg =
      String(
        err?.message ||
        err
      );

    const status =
      Number(
        err?.status ||
        0
      );

    return {
      available: false,
      status:
        status === 403 ||
        /HTTP 403/.test(msg)
          ? "blocked_403"
          : /abort/i.test(msg)
            ? "timeout"
            : "unavailable",
      error: msg,
      attemptedAt
    };
  }
}

async function githubGetShaOnly(
  gh,
  path,
  subrequestBudget = null
) {
  const url =
    `https://api.github.com/repos/${gh.owner}/${gh.repo}/contents/${encodePath(path)}?ref=${encodeURIComponent(gh.branch)}`;

  consumeSubrequest(
    subrequestBudget,
    `GitHub SHA GET ${path}`
  );

  const resp =
    await fetch(
      url,
      {
        headers:
          githubHeaders(gh)
      }
    );

  if (
    resp.status === 404
  ) {
    return null;
  }

  if (!resp.ok) {
    throw new Error(
      `GitHub SHA GET ${path} HTTP ${resp.status}`
    );
  }

  const body =
    await resp.json();

  return {
    sha:
      body.sha ||
      null
  };
}


async function runBridge(env) {
  applyTransportEnv(env);
  const startedAt = new Date().toISOString();
  const runId = `${Date.now()}-${crypto.randomUUID()}`;

  const subrequestBudget = createSubrequestBudget(
    clampInt(
      env.SUBREQUEST_BUDGET,
      DEFAULT_SUBREQUEST_BUDGET,
      30,
      48
    ),
    GITHUB_WRITE_RESERVE
  );

  const stageBSymbolLimit = clampInt(
    env.MAX_STAGE_B_SYMBOLS,
    DEFAULT_STAGE_B_SYMBOL_LIMIT,
    7,
    16
  );

  const dynamicFreshAnalysisCount = clampInt(
    env.DYNAMIC_FRESH_ANALYSIS_COUNT,
    DEFAULT_DYNAMIC_FRESH_ANALYSIS_COUNT,
    2,
    8
  );

  const anomalyRecheckMaxSymbols = clampInt(
    env.ANOMALY_RECHECK_MAX_SYMBOLS,
    DEFAULT_ANOMALY_RECHECK_MAX_SYMBOLS,
    1,
    6
  );

  const coreTimeframeAttempts = clampInt(
    env.CORE_TIMEFRAME_ATTEMPTS,
    DEFAULT_CORE_TIMEFRAME_ATTEMPTS,
    1,
    3
  );

  const dynamicTimeframeAttempts = clampInt(
    env.DYNAMIC_TIMEFRAME_ATTEMPTS,
    DEFAULT_DYNAMIC_TIMEFRAME_ATTEMPTS,
    1,
    2
  );

  const bitgetEvidenceMaxSymbols = clampInt(
    env.BITGET_EVIDENCE_MAX_SYMBOLS,
    DEFAULT_BITGET_EVIDENCE_MAX_SYMBOLS,
    4,
    10
  );

  const okxStageBStartDelayMs = clampInt(
    env.OKX_STAGE_B_START_DELAY_MS,
    DEFAULT_OKX_STAGE_B_START_DELAY_MS,
    0,
    2000
  );

  const okxInterTimeframeDelayMs = clampInt(
    env.OKX_INTER_TIMEFRAME_DELAY_MS,
    DEFAULT_OKX_INTER_TIMEFRAME_DELAY_MS,
    0,
    1500
  );

  const okxInterSymbolDelayMs = clampInt(
    env.OKX_INTER_SYMBOL_DELAY_MS,
    DEFAULT_OKX_INTER_SYMBOL_DELAY_MS,
    0,
    2000
  );

  const okx429BaseBackoffMs = clampInt(
    env.OKX_429_BASE_BACKOFF_MS,
    DEFAULT_OKX_429_BASE_BACKOFF_MS,
    800,
    5000
  );

  ACTIVE_OKX_429_BASE_BACKOFF_MS =
    okx429BaseBackoffMs;

  const requireDualExchange =
    String(env.REQUIRE_DUAL_EXCHANGE || "")
      .toLowerCase() === "true";

  const singleExchangeMinTurnoverUsd =
    positiveNumber(
      env.SINGLE_EXCHANGE_MIN_TURNOVER_USD,
      DEFAULT_SINGLE_EXCHANGE_MIN_TURNOVER_USD
    );

  const excludedBases = new Set(
    parseCsv(
      env.EXCLUDED_BASES,
      DEFAULT_EXCLUDED_BASES
    ).map(x => x.toUpperCase())
  );

  const fixedSymbols = parseCsv(env.FIXED_SYMBOLS, DEFAULT_FIXED);
  const watchSymbols = parseCsv(env.WATCH_SYMBOLS, DEFAULT_WATCH);

  const dynamicCount = clampInt(env.DYNAMIC_COUNT, 10, 3, 20);
  const preselectCount = clampInt(env.PRESELECT_COUNT, 14, dynamicCount, 24);

  const minTurnoverUsd = positiveNumber(
    env.MIN_TURNOVER_USD,
    DEFAULT_MIN_TURNOVER_USD
  );
  const minOiUsd = positiveNumber(
    env.MIN_OI_USD,
    DEFAULT_MIN_OI_USD
  );
  const minLiquidityPercentile = boundedNumber(
    env.MIN_LIQUIDITY_PERCENTILE,
    DEFAULT_MIN_LIQUIDITY_PERCENTILE,
    0,
    1
  );
  const minOiPercentile = boundedNumber(
    env.MIN_OI_PERCENTILE,
    DEFAULT_MIN_OI_PERCENTILE,
    0,
    1
  );

  const gh = {
    owner: env.GITHUB_OWNER || "zhaorui0521-pixel",
    repo: env.GITHUB_REPO || "hype-market-data",
    branch: env.GITHUB_BRANCH || "main",
    token: env.GITHUB_TOKEN
  };

  if (!gh.token) {
    throw new Error("Missing GITHUB_TOKEN");
  }

  const [
    oldLatestFile,
    oldSummaryFile,
    oldBitgetEvidenceStateFile
  ] = await Promise.all([
    githubGetJson(gh, "latest.json", subrequestBudget).catch(() => null),
    githubGetJson(gh, "summary.json", subrequestBudget).catch(() => null),
    githubGetShaOnly(
      gh,
      "bitget_evidence_state.json",
      subrequestBudget
    ).catch(() => null)
  ]);

  const oldLatest = oldLatestFile?.json || {};
  const oldSummary = oldSummaryFile?.json || {};

  // Stage A: dual-exchange broad market discovery + product purification.
  // OKX documents that market-data requests can hit independent caches, so we
  // compare timestamps and refetch stale universe snapshots before ranking.
  const [
    tickerResp,
    oiResp,
    instrumentResp,
    binanceBundle
  ] = await Promise.all([
    getFreshTickerUniverse(subrequestBudget),
    getFreshOiUniverse(subrequestBudget),
    getOkxSwapInstruments(subrequestBudget),
    getBinanceDiscoveryBundleSafe(
      subrequestBudget
    )
  ]);

  const okxInstrumentMap =
    buildOkxInstrumentMap(
      instrumentResp.data || []
    );

  const binanceAvailable =
    !!binanceBundle.ok;

  const binanceStatus =
    binanceBundle.status ||
    (
      binanceAvailable
        ? "ok"
        : "unavailable"
    );

  const binanceLastError =
    binanceBundle.error || null;

  const binanceCircuitBroken =
    !!binanceBundle.circuitBroken;

  const binanceAttemptedAt =
    binanceBundle.attemptedAt ||
    new Date().toISOString();

  const discoveryMode =
    binanceAvailable
      ? "dual_exchange"
      : "okx_only_degraded";

  const binanceUniverse =
    binanceAvailable
      ? buildBinanceUniverse(
          binanceBundle.exchangeInfo,
          binanceBundle.tickerRows
        )
      : new Map();

  const allTickers = (tickerResp.data || [])
    .filter(t =>
      isEligibleUsdtSwap(
        t.instId,
        okxInstrumentMap,
        excludedBases
      )
    )
    .map(normalizeTicker)
    .filter(t =>
      Number.isFinite(t.last) &&
      t.last > 0
    );

  if (!allTickers.length) {
    throw new Error(
      "No eligible OKX crypto USDT perpetual tickers returned after purification"
    );
  }

  const oiMap =
    buildOiMap(
      oiResp.data || [],
      allTickers
    );

  const stageAAll =
    rankStageADualExchange(
      allTickers,
      oiMap,
      binanceUniverse,
      {
        requireDualExchange,
        singleExchangeMinTurnoverUsd,
        binanceAvailable
      }
    );

  // Hard + relative quality gate. Fixed BTC/ETH/SOL bypass this gate later;
  // dynamic candidates do not.
  const stageA = stageAAll.filter(x =>
    x.eligibleForDynamic &&
    x.turnoverUsdEstimate >= minTurnoverUsd &&
    x.openInterestUsdEstimate >= minOiUsd &&
    x.liquidityScore >= minLiquidityPercentile &&
    x.oiScore >= minOiPercentile
  );

  // V6.2.4 layered scheduler:
  // Layer 1: BTC/ETH/SOL always receive fresh analysis first.
  // Layer 2: only the strongest N Stage-A dynamic candidates receive new
  //          15m/1h candle requests this run.
  // Layer 3: remaining Stage-A candidates may still compete from a recent
  //          <=10m cached MA7/25/60 snapshot, consuming zero candle requests.
  //
  // This changes the bridge from "refresh nearly everyone" to
  // "refresh the core + the most valuable opportunities".

  const fixedPreselected = fixedSymbols
    .filter(
      s =>
        allTickers.some(
          t => t.symbol === s
        )
    );

  const stageACandidateSymbols = stageA
    .filter(
      x =>
        !fixedSymbols.includes(
          x.symbol
        )
    )
    .slice(
      0,
      preselectCount
    )
    .map(
      x => x.symbol
    );

  const qualifiedWatchSymbols = watchSymbols
    .filter(
      s =>
        stageA.some(
          x => x.symbol === s
        )
    );

  const dynamicCandidateSymbols = unique([
    ...stageACandidateSymbols,
    ...qualifiedWatchSymbols
  ]).slice(
    0,
    Math.max(
      0,
      stageBSymbolLimit -
      fixedPreselected.length
    )
  );

  // Prefer candidates with no usable recent cache first only when their
  // Stage-A rank is strong; otherwise recent cache avoids unnecessary fetches.
  let dynamicFreshPreselected =
    dynamicCandidateSymbols
      .slice(
        0,
        dynamicFreshAnalysisCount
      );

  const anomalyRecheckCandidates = unique([
    ...qualifiedWatchSymbols,
    ...dynamicCandidateSymbols.slice(dynamicFreshAnalysisCount)
  ])
    .filter(symbol => !dynamicFreshPreselected.includes(symbol))
    .slice(0, anomalyRecheckMaxSymbols);

  const anomalyRechecks = [];

  for (const symbol of anomalyRecheckCandidates) {
    if (!canSpendSubrequest(
      subrequestBudget,
      ANOMALY_RECHECK_REQUEST_ESTIMATE + DYNAMIC_FRESH_REQUEST_ESTIMATE
    )) {
      anomalyRechecks.push({
        symbol,
        ok: false,
        promoted: false,
        reason: "budget_reserved_for_full_analysis"
      });
      continue;
    }

    anomalyRechecks.push(
      await recheckCandidateAnomaly(symbol, subrequestBudget)
    );
  }

  const anomalyPromotions = anomalyRechecks
    .filter(item => item.ok && item.promoted)
    .sort((a, b) => b.strength - a.strength)
    .map(item => item.symbol)
    .slice(0, dynamicFreshAnalysisCount);

  if (anomalyPromotions.length) {
    dynamicFreshPreselected = unique([
      ...anomalyPromotions,
      ...dynamicFreshPreselected
    ]).slice(0, dynamicFreshAnalysisCount);
  }

  const dynamicCachedOnly =
    dynamicCandidateSymbols.filter(
      symbol => !dynamicFreshPreselected.includes(symbol)
    );

  const preselected = unique([
    ...fixedPreselected,
    ...dynamicFreshPreselected
  ]);

  const tickerMap = new Map(
    allTickers.map(
      x => [
        x.symbol,
        x
      ]
    )
  );

  const detailedResults = [];
  const freshDynamicAnalyzedSymbols = [];
  const freshDynamicSkippedForBudget = [];

  // Let the Stage-A burst settle before the first BTC candle request.
  if (okxStageBStartDelayMs > 0) {
    await sleep(
      okxStageBStartDelayMs
    );
  }

  // ---------------- Layer 1: core ----------------
  // Core symbols always spend first. No dynamic request is allowed before
  // benchmark/core analysis finishes.
  for (
    const symbol of
    fixedPreselected
  ) {
    try {
      detailedResults.push(
        await analyzeSymbol(
          symbol,
          tickerMap.get(symbol),
          oiMap.get(symbol),
          oldSummary?.markets?.[symbol] ||
            null,
          subrequestBudget,
          {
            timeframeAttempts:
              coreTimeframeAttempts,
            interTimeframeDelayMs:
              okxInterTimeframeDelayMs,
            priority:
              "fixed_core"
          }
        )
      );
    } catch (err) {
      detailedResults.push({
        ok: false,
        symbol,
        error:
          String(
            err?.message ||
            err
          )
      });
    }

    if (
      okxInterSymbolDelayMs > 0
    ) {
      await sleep(
        okxInterSymbolDelayMs
      );
    }
  }

  // ---------------- Layer 2: expensive fresh dynamic analysis ----------------
  for (
    let i = 0;
    i <
      dynamicFreshPreselected.length;
    i++
  ) {
    const symbol =
      dynamicFreshPreselected[i];

    // Do not begin a dynamic symbol unless there is enough non-reserved
    // budget for its normal fresh analysis. GitHub reserve stays untouched.
    if (
      !canSpendSubrequest(
        subrequestBudget,
        DYNAMIC_FRESH_REQUEST_ESTIMATE
      )
    ) {
      freshDynamicSkippedForBudget.push(
        symbol
      );

      continue;
    }

    try {
      detailedResults.push(
        await analyzeSymbol(
          symbol,
          tickerMap.get(symbol),
          oiMap.get(symbol),
          oldSummary?.markets?.[symbol] ||
            null,
          subrequestBudget,
          {
            timeframeAttempts:
              dynamicTimeframeAttempts,
            interTimeframeDelayMs:
              okxInterTimeframeDelayMs,
            priority:
              "dynamic_fresh"
          }
        )
      );

      freshDynamicAnalyzedSymbols.push(
        symbol
      );
    } catch (err) {
      detailedResults.push({
        ok: false,
        symbol,
        error:
          String(
            err?.message ||
            err
          )
      });
    }

    if (
      okxInterSymbolDelayMs > 0
    ) {
      await sleep(
        okxInterSymbolDelayMs
      );
    }
  }

  const detailedMap = new Map(
    detailedResults.map(x => [x.symbol, x])
  );

  // Dynamic pool competition.
  // Fresh snapshots receive full weight. A previous MA7/25/60 snapshot may
  // temporarily participate for <=30 minutes when one symbol fetch fails.
  let dynamicRanked = stageA
    .filter(x => !fixedSymbols.includes(x.symbol))
    .map(x => {
      const freshMarket = detailedMap.get(x.symbol);
      const oldMarket = oldSummary?.markets?.[x.symbol];

      let market = null;
      let sourceState = "failed";
      let freshnessFactor = 0;

      if (freshMarket?.ok) {
        market = freshMarket;

        sourceState =
          freshMarket?.freshness?.status === "fresh"
            ? "fresh"
            : "degraded";

        freshnessFactor =
          sourceState === "fresh"
            ? 1
            : 0.55;
      } else {
        const oldAgeMinutes =
          updatedAgeMinutes(
            oldMarket?.updatedAt ||
            oldSummary?.updatedAt
          );

        if (
          isUsableStaleFallback(
            oldMarket,
            oldAgeMinutes,
            10
          )
        ) {
          // V6.2.4 recent-cache tier:
          // a <=10m MA7/25/60 snapshot remains useful for Stage-B competition
          // without spending two more candle requests.
          market = oldMarket;
          sourceState = "recent_cached";
          freshnessFactor = 0.75;
        } else if (
          isUsableStaleFallback(
            oldMarket,
            oldAgeMinutes,
            30
          )
        ) {
          market = oldMarket;
          sourceState = "stale_fallback";
          freshnessFactor = 0.35;
        }
      }

      if (!market) {
        return null;
      }

      const technical =
        technicalOpportunityScore(market);

      const adjustedTechnical =
        technical.score *
        freshnessFactor;

      const total = round6(
        0.65 * x.baseScore +
        0.35 * adjustedTechnical
      );

      return {
        symbol: x.symbol,
        score: total,

        sourceState,
        freshnessFactor,

        marketQualityScore: x.baseScore,

        technicalOpportunityScore:
          technical.score,

        adjustedTechnicalOpportunityScore:
          round6(adjustedTechnical),

        liquidityScore: x.liquidityScore,
        oiScore: x.oiScore,
        volatilityScore: x.volatilityScore,

        turnoverUsdEstimate:
          x.turnoverUsdEstimate,

        binanceTurnoverUsdEstimate:
          x.binanceTurnoverUsdEstimate,

        crossExchangeConfirmed:
          x.crossExchangeConfirmed,

        crossExchangePriceDiffPct:
          x.crossExchangePriceDiffPct,

        crossExchangeScore:
          x.crossExchangeScore,

        openInterestUsdEstimate:
          x.openInterestUsdEstimate,

        rangePct24h:
          x.rangePct24h,

        binanceRangePct24h:
          x.binanceRangePct24h,

        selectionReason:
          `${technical.reason}; discoveryMode=${discoveryMode}; dualExchange=${x.crossExchangeConfirmed}; priceDiffPct=${round6(x.crossExchangePriceDiffPct || 0)}; sourceState=${sourceState}`
      };
    })
    .filter(Boolean)
    .sort((a, b) => b.score - a.score);

  // Fetch Bitget before final ranking so it is a real model input. The three
  // core symbols are always first; remaining capacity is reserved for dynamic
  // candidates. A single Bitget field receives zero effective weight.
  const bitgetEvidenceTargets = unique([
    ...DEFAULT_BITGET_CORE,
    ...dynamicRanked.map(item => item.symbol)
  ]).slice(0, bitgetEvidenceMaxSymbols);

  const bitgetEvidence = await fetchBitgetEvidence(
    bitgetEvidenceTargets,
    subrequestBudget
  );

  bitgetEvidence.modelScores = Object.fromEntries(
    bitgetEvidenceTargets.map(symbol => [
      toBitgetSymbol(symbol),
      bitgetModelScoreForSymbol(bitgetEvidence, symbol)
    ])
  );

  dynamicRanked = dynamicRanked
    .map(item => {
      const bitgetModel =
        bitgetEvidence.modelScores[
          toBitgetSymbol(item.symbol)
        ] ?? bitgetModelScoreForSymbol(
          bitgetEvidence,
          item.symbol
        );

      const score = bitgetModel.eligible
        ? round6(
            (1 - bitgetModel.effectiveWeight) * item.score +
            bitgetModel.effectiveWeight * bitgetModel.opportunityScore
          )
        : item.score;

      return {
        ...item,
        preBitgetScore: item.score,
        score,
        bitgetModel,
        selectionReason:
          `${item.selectionReason}; bitget=${bitgetModel.reason}`
      };
    })
    .sort((a, b) => b.score - a.score);

  const dynamicSymbols = dynamicRanked
    .slice(0, dynamicCount)
    .map(x => x.symbol);

  const selectedSymbols = unique([
    ...fixedSymbols,
    ...dynamicSymbols
  ]);

  const marketsDetailed = {};
  const marketsCompact = {};

  const failedSymbols = [];
  const staleSymbols = [];

  for (const symbol of selectedSymbols) {
    const fresh = detailedMap.get(symbol);

    if (fresh?.ok) {
      marketsDetailed[symbol] = fresh;
      marketsCompact[symbol] =
        compactMarket(fresh);

      // Successful fetch != guaranteed fresh source.
      if (
        fresh.stale ||
        fresh?.freshness?.status !== "fresh"
      ) {
        staleSymbols.push(symbol);
      }

      continue;
    }

    const oldD =
      oldSummary?.markets?.[symbol];

    const oldC =
      oldLatest?.markets?.[symbol];

    const oldAgeMin =
      updatedAgeMinutes(
        oldD?.updatedAt ||
        oldSummary?.updatedAt ||
        oldLatest?.updatedAt
      );

    if (
      (oldD || oldC) &&
      oldAgeMin <= 60
    ) {
      const fallbackDetailed =
        oldD || {
          ok: true,
          symbol,
          price: oldC?.price,
          high24h: oldC?.high24h,
          low24h: oldC?.low24h,
          timeframe15m:
            oldC?.timeframe15m,
          timeframe1h:
            oldC?.timeframe1h
        };

      const lastSuccessfulAt =
        fallbackDetailed.updatedAt ||
        oldSummary.updatedAt ||
        oldLatest.updatedAt ||
        null;

      const fallbackStatus =
        oldAgeMin <= 30
          ? "degraded"
          : "auxiliary_only";

      marketsDetailed[symbol] = {
        ...fallbackDetailed,
        stale: true,

        staleReason:
          fresh?.error ||
          "fresh fetch unavailable",

        lastSuccessfulAt,

        freshness: {
          ...(fallbackDetailed.freshness || {}),
          status: fallbackStatus,

          fallbackAgeMinutes:
            round6(oldAgeMin)
        }
      };

      marketsCompact[symbol] = {
        ...(oldC ||
          compactMarket(
            fallbackDetailed
          )),

        stale: true,

        staleReason:
          fresh?.error ||
          "fresh fetch unavailable",

        lastSuccessfulAt,

        freshness: {
          ...(
            (
              oldC ||
              fallbackDetailed
            )?.freshness ||
            {}
          ),

          status: fallbackStatus,

          fallbackAgeMinutes:
            round6(oldAgeMin)
        }
      };

      staleSymbols.push(symbol);
    } else {
      failedSymbols.push(symbol);
    }
  }

  const publishedDynamic = dynamicSymbols.filter(
    s => marketsCompact[s]
  );

  const publishedSymbols = unique([
    ...fixedSymbols.filter(s => marketsCompact[s]),
    ...publishedDynamic
  ]);

  const selectionDetails = dynamicRanked
    .filter(x => publishedDynamic.includes(x.symbol))
    .map((x, index) => ({
      rank: index + 1,
      ...x
    }));

  const updatedAt = new Date().toISOString();
  const partial =
    failedSymbols.length > 0 ||
    staleSymbols.length > 0;

  const pool = {
    benchmarkSymbols: [
      "BTC-USDT-SWAP",
      "ETH-USDT-SWAP"
    ].filter(x => fixedSymbols.includes(x)),

    coreExecutionSymbols: [
      "SOL-USDT-SWAP"
    ].filter(x => fixedSymbols.includes(x)),

    fixedSymbols,
    watchContenders: watchSymbols,
    dynamicSymbols: publishedDynamic,
    publishedSymbols,

    dynamicCountRequested: dynamicCount,

    evidenceLayer: {
      bitget: {
        mode:
          "formal_multi_field_scoring",
        observedOnly: false,
        affectsScore: true,
        minimumIndependentDirectionalFields: 2,
        maxScoreWeight: BITGET_SCORE_WEIGHT,
        singleFieldCanAffectScore: false,
        canTriggerTrade: false,
        cronLiteEnabled: false,
        cronBehavior:
          "carry_forward_only",
        maxSymbols:
          bitgetEvidenceMaxSymbols,
        requestedSymbols:
          bitgetEvidenceTargets,
        status:
          bitgetEvidence.status
      }
    },

    scheduler: {
      mode:
        "layered_core_first_with_anomaly_fast_lane",
      fixedFreshSymbols:
        fixedPreselected,
      dynamicCandidates:
        dynamicCandidateSymbols,
      dynamicFreshTarget:
        dynamicFreshAnalysisCount,
      freshDynamicAnalyzedSymbols,
      cachedOnlyDynamicCandidates:
        dynamicCachedOnly,
      anomalyFastLane: {
        enabled: true,
        maxRecheckSymbols: anomalyRecheckMaxSymbols,
        candidates: anomalyRecheckCandidates,
        rechecks: anomalyRechecks,
        promotedSymbols: anomalyPromotions,
        minimumConfirmingFields: 2,
        singleFieldCanPromote: false,
        fields: [
          "price_change_5m_10m",
          "taker_buy_sell_5m",
          "open_interest_change_5m_10m"
        ]
      },
      freshDynamicSkippedForBudget,
      coreTimeframeAttempts,
      dynamicTimeframeAttempts,
      okxPacing: {
        stageBStartDelayMs:
          okxStageBStartDelayMs,
        interTimeframeDelayMs:
          okxInterTimeframeDelayMs,
        interSymbolDelayMs:
          okxInterSymbolDelayMs,
        rateLimitBackoffBaseMs:
          okx429BaseBackoffMs
      }
    },

    selectionMethod: {
      stageA:
        "OKX crypto USDT perpetuals + Binance USDⓈ-M perpetuals -> product purification -> cross-exchange confirmation -> liquidity + OI + volatility",
      qualityGate: {
        minTurnoverUsd,
        minOiUsd,
        minLiquidityPercentile,
        minOiPercentile,
        requireDualExchange,
        effectiveRequireDualExchange:
          requireDualExchange &&
          binanceAvailable,
        singleExchangeMinTurnoverUsd
      },

      purification: {
        okxCryptoOnly:
          "instCategory === 1",
        okxRuleType:
          "normal/rebase_contract only; pre_market/xperp futures are not part of SWAP discovery",
        excludedBases:
          [...excludedBases],
        binanceContractType:
          "PERPETUAL",
        binanceQuoteAsset:
          "USDT",
        binanceStatus:
          "TRADING"
      },

      crossExchangeDiscovery: {
        exchanges:
          ["OKX", "Binance"],
        discoveryMode,
        binanceAvailable,
        binanceStatus,
        binanceLastError,
        binanceAttemptedAt,
        dualExchangePreferred:
          true,
        singleExchangeAllowed:
          !(
            requireDualExchange &&
            binanceAvailable
          ),
        singleExchangeRequiresHigherTurnover:
          singleExchangeMinTurnoverUsd
      },
      stageB:
        "layered scheduler: BTC/ETH/SOL core remains first; cached-only/watch candidates receive bounded price+taker+OI anomaly rechecks; multi-field anomalies may enter the five full fresh 15m/1h slots",
      bridgeScoreWeights: {
        marketQuality: 0.65,
        technicalOpportunity: 0.35,
        marketQualityBreakdown: {
          okxLiquidity: 0.30,
          binanceLiquidity: 0.20,
          openInterest: 0.20,
          volatility: 0.15,
          crossExchangeConfirmation: 0.10,
          crossExchangePriceConsistency: 0.05
        },
        technicalBreakdown: {
          structureClarity: 0.25,
          structureAgreement: 0.20,
          maAgreement: 0.20,
          structureMaAgreement: 0.10,
          volumeParticipation: 0.15,
          keyLevelProximity: 0.10
        }
      },
      finalModelLayer:
        "BTC/ETH environment fit + exact key-level interpretation + 5m trigger + R:R"
    }
  };

  const dataQuality = {
    failedSymbols,
    staleSymbols,
    retryPolicy:
      "3 attempts; 429/5xx/50011 exponential backoff + jitter; non-retryable 4xx fail fast",
    closedCandlesOnly: true,
    publishOrder:
      "summary.json first, latest.json last; shared runId",
    githubConflictRetry:
      "409/422 -> refetch SHA and retry up to 2 times",
    perSymbolIsolation: true,

    subrequestBudget: {
      max: subrequestBudget.max,
      usedBeforePublish: subrequestBudget.used,
      githubWriteReserve: subrequestBudget.reserve,
      stageBSymbolLimit,
      dynamicFreshAnalysisCount,
      anomalyRecheckMaxSymbols,
      coreTimeframeAttempts,
      dynamicTimeframeAttempts,
      okxStageBStartDelayMs,
      okxInterTimeframeDelayMs,
      okxInterSymbolDelayMs,
      okx429BaseBackoffMs,
      freshDynamicAnalyzedCount:
        freshDynamicAnalyzedSymbols.length,
      cachedOnlyDynamicCount:
        dynamicCachedOnly.length,
      freshDynamicSkippedForBudget
    },

    discoveryQuality: {
      okxInstrumentPurification:
        true,
      okxCryptoCategoryOnly:
        true,
      binanceUniverseCrossCheck:
        binanceAvailable,
      discoveryMode,
      binanceAvailable,
      binanceStatus,
      binanceLastError,
      binanceAttemptedAt,
      requireDualExchange,
      effectiveRequireDualExchange:
        requireDualExchange &&
        binanceAvailable,
      singleExchangeMinTurnoverUsd,
      excludedBases:
        [...excludedBases]
    },

    freshnessPolicy: {
      fullWeightMinutes: 10,
      downWeightMinutes: 30,
      auxiliaryOnlyMinutes: 60,
      excludeAfterMinutes: 60
    }
  };

  const summary = {
    ok: true,
    partial,
    source:
      binanceAvailable
        ? "OKX + Binance official public APIs"
        : "OKX official public API; Binance unavailable",

    discoveryMode,
    binanceAvailable,
    binanceStatus,
    binanceCircuitBroken,
    binanceLastError,
    binanceAttemptedAt,

    runId,
    startedAt,
    updatedAt,

    pool,
    dataQuality,

    bitgetEvidence,

    selection: selectionDetails,
    symbols: publishedSymbols,

    markets: pickKeys(
      marketsDetailed,
      publishedSymbols
    )
  };

  const latest = {
    ok: true,
    partial,
    source:
      binanceAvailable
        ? "OKX + Binance official public APIs"
        : "OKX official public API; Binance unavailable",

    discoveryMode,
    binanceAvailable,
    binanceStatus,
    binanceCircuitBroken,
    binanceLastError,
    binanceAttemptedAt,

    runId,
    updatedAt,

    pool,
    dataQuality: {
      failedSymbols,
      staleSymbols
    },

    selection: selectionDetails,

    bitgetEvidence,

    markets: pickKeys(
      marketsCompact,
      publishedSymbols
    )
  };

  const bitgetEvidenceState = {
    version: "6.4.1",
    mode:
      "formal_multi_field_scoring",
    observedOnly: false,
    affectsScore: true,
    singleFieldCanAffectScore: false,
    canTriggerTrade: false,
    runId,
    updatedAt,
    bitgetEvidence
  };

  // Persist the small Bitget snapshot first so Cron Lite can carry it forward
  // without decoding the full summary payload or calling Bitget again.
  const bitgetEvidenceStateWrite =
    await githubPutJson(
      gh,
      "bitget_evidence_state.json",
      bitgetEvidenceState,
      oldBitgetEvidenceStateFile?.sha,
      `bridge: Bitget formal evidence ${updatedAt}`,
      subrequestBudget
    );

  // Publish summary first and latest.json last.
  // Both files carry runId, so consumers can detect a partial publication.
  const summaryWrite = await githubPutJson(
    gh,
    "summary.json",
    summary,
    oldSummaryFile?.sha,
    `bridge: detailed dynamic market update ${updatedAt}`,
    subrequestBudget
  );

  const latestWrite = await githubPutJson(
    gh,
    "latest.json",
    latest,
    oldLatestFile?.sha,
    `bridge: dynamic pool update ${updatedAt}`,
    subrequestBudget
  );

  return {
    ok: true,
    partial,
    runId,
    updatedAt,

    fixedSymbols,
    dynamicSymbols: publishedDynamic,
    selectedSymbols: publishedSymbols,

    discoveryMode,
    binanceAvailable,
    binanceStatus,
    binanceCircuitBroken,
    binanceLastError,
    binanceAttemptedAt,

    failedSymbols,
    staleSymbols,

    bitgetEvidence: {
      status:
        bitgetEvidence.status,
      observedOnly: false,
      affectsScore: true,
      singleFieldCanAffectScore: false,
      canTriggerTrade: false,
      requestedSymbols:
        bitgetEvidence.requestedSymbols ||
        [],
      healthyCount:
        bitgetEvidence.healthyCount ||
        0
    },

    scheduler: {
      fixedFreshSymbols:
        fixedPreselected,
      freshDynamicAnalyzedSymbols,
      cachedOnlyDynamicCandidates:
        dynamicCachedOnly,
      freshDynamicSkippedForBudget
    },

    subrequests: {
      used: subrequestBudget.used,
      max: subrequestBudget.max,
      remaining: Math.max(
        0,
        subrequestBudget.max - subrequestBudget.used
      )
    },

    github: {
      bitgetEvidenceCommit:
        bitgetEvidenceStateWrite.commitSha,
      latestCommit: latestWrite.commitSha,
      summaryCommit: summaryWrite.commitSha
    }
  };
}

// ============================================================
// STAGE A: MARKET QUALITY
// ============================================================

function rankStageADualExchange(
  tickers,
  oiMap,
  binanceUniverse,
  policy
) {
  const rows = tickers.map(t => {
    const oi =
      oiMap.get(t.symbol);

    const base =
      baseFromOkxSymbol(
        t.symbol
      );

    const binance =
      binanceUniverse.get(base);

    const turnoverUsdEstimate =
      Math.max(
        0,
        t.volCcy24h *
        t.last
      );

    const binanceTurnoverUsdEstimate =
      Math.max(
        0,
        Number(
          binance?.quoteVolume ||
          0
        )
      );

    const openInterestUsdEstimate =
      oi?.oiUsdEstimate || 0;

    const rangePct24h =
      t.open24h > 0
        ? Math.max(
            0,
            (
              t.high24h -
              t.low24h
            ) /
            t.open24h
          )
        : 0;

    const binanceRangePct24h =
      binance?.openPrice > 0
        ? Math.max(
            0,
            (
              binance.highPrice -
              binance.lowPrice
            ) /
            binance.openPrice
          )
        : 0;

    const crossExchangeConfirmed =
      !!binance?.eligible;

    const crossExchangePriceDiffPct =
      crossExchangeConfirmed &&
      t.last > 0 &&
      binance.lastPrice > 0
        ? Math.abs(
            t.last -
            binance.lastPrice
          ) /
          (
            (
              t.last +
              binance.lastPrice
            ) /
            2
          )
        : null;

    const priceConsistency =
      crossExchangeConfirmed &&
      Number.isFinite(
        crossExchangePriceDiffPct
      )
        ? Math.max(
            0,
            Math.min(
              1,
              1 -
              crossExchangePriceDiffPct /
              0.01
            )
          )
        : 0;

    const effectiveRequireDualExchange =
      !!policy.requireDualExchange &&
      !!policy.binanceAvailable;

    const eligibleForDynamic =
      crossExchangeConfirmed
        ? true
        : (
            !effectiveRequireDualExchange &&
            (
              !policy.binanceAvailable
                ? true
                : turnoverUsdEstimate >=
                    policy.singleExchangeMinTurnoverUsd
            )
          );

    return {
      symbol: t.symbol,
      base,
      turnoverUsdEstimate,
      binanceTurnoverUsdEstimate,
      openInterestUsdEstimate,
      rangePct24h,
      binanceRangePct24h,
      crossExchangeConfirmed,
      crossExchangePriceDiffPct,
      priceConsistency,
      eligibleForDynamic
    };
  });

  const okxLiqValues =
    rows.map(
      x =>
        x.turnoverUsdEstimate
    );

  const binanceLiqValues =
    rows
      .filter(
        x =>
          x.crossExchangeConfirmed
      )
      .map(
        x =>
          x.binanceTurnoverUsdEstimate
      );

  const oiValues =
    rows.map(
      x =>
        x.openInterestUsdEstimate
    );

  const volValues =
    rows.map(
      x =>
        Math.max(
          x.rangePct24h,
          x.binanceRangePct24h ||
          0
        )
    );

  return rows
    .map(x => {
      const liquidityScore =
        percentileScore(
          x.turnoverUsdEstimate,
          okxLiqValues
        );

      const binanceLiquidityScore =
        x.crossExchangeConfirmed
          ? percentileScore(
              x.binanceTurnoverUsdEstimate,
              binanceLiqValues
            )
          : 0;

      const oiScore =
        percentileScore(
          x.openInterestUsdEstimate,
          oiValues
        );

      const combinedVolatility =
        x.crossExchangeConfirmed
          ? (
              0.55 *
              x.rangePct24h +
              0.45 *
              x.binanceRangePct24h
            )
          : x.rangePct24h;

      const volatilityScore =
        percentileScore(
          combinedVolatility,
          volValues
        );

      const cappedVolatilityScore =
        Math.min(
          1,
          volatilityScore /
          0.85
        );

      const crossExchangeScore =
        x.crossExchangeConfirmed
          ? 1
          : 0;

      // Dual-exchange score when Binance is available.
      // If Binance is blocked/unavailable, gracefully renormalize to OKX-only
      // quality so the bridge keeps discovering candidates instead of failing.
      const baseScore =
        policy.binanceAvailable
          ? round6(
              0.30 * liquidityScore +
              0.20 * binanceLiquidityScore +
              0.20 * oiScore +
              0.15 * cappedVolatilityScore +
              0.10 * crossExchangeScore +
              0.05 * x.priceConsistency
            )
          : round6(
              0.50 * liquidityScore +
              0.30 * oiScore +
              0.20 * cappedVolatilityScore
            );

      return {
        ...x,

        liquidityScore:
          round6(
            liquidityScore
          ),

        binanceLiquidityScore:
          round6(
            binanceLiquidityScore
          ),

        oiScore:
          round6(
            oiScore
          ),

        volatilityScore:
          round6(
            volatilityScore
          ),

        cappedVolatilityScore:
          round6(
            cappedVolatilityScore
          ),

        crossExchangeScore:
          round6(
            crossExchangeScore
          ),

        crossExchangePriceDiffPct:
          Number.isFinite(
            x.crossExchangePriceDiffPct
          )
            ? round6(
                x.crossExchangePriceDiffPct
              )
            : null,

        priceConsistency:
          round6(
            x.priceConsistency
          ),

        baseScore
      };
    })
    .sort(
      (a, b) =>
        b.baseScore -
        a.baseScore
    );
}


// ============================================================
// STAGE B: TECHNICAL OPPORTUNITY
// ============================================================

function technicalOpportunityScore(market) {
  const tfStatus15 =
    market?.timeframeStatus?.["15m"];

  const tfStatus1h =
    market?.timeframeStatus?.["1h"];

  const tfWeight15 =
    tfStatus15?.realtimeWeight ?? 1;

  const tfWeight1h =
    tfStatus1h?.realtimeWeight ?? 1;

  const tf15 = market.timeframe15m;
  const tf1h = market.timeframe1h;

  const structure1h =
    tf1h?.structure?.state || "range";
  const structure15 =
    tf15?.structure?.state || "range";

  const dirFromStructure = state =>
    state === "higher_highs_higher_lows"
      ? "bullish"
      : state === "lower_highs_lower_lows"
        ? "bearish"
        : "neutral";

  const structureDir1h =
    dirFromStructure(structure1h);
  const structureDir15 =
    dirFromStructure(structure15);

  // Clarity answers: "is there a readable structure?"
  const confidenceWeight = value =>
    value === "high"
      ? 1
      : value === "medium"
        ? 0.75
        : 0.50;

  const baseStructureClarity =
    structureDir1h !== "neutral" &&
    structureDir15 !== "neutral"
      ? 1
      : structureDir1h !== "neutral"
        ? 0.75
        : structureDir15 !== "neutral"
          ? 0.55
          : 0.25;

  const structureClarity =
    baseStructureClarity *
    (
      0.60 * confidenceWeight(
        tf1h?.structure?.confidence
      ) +
      0.40 * confidenceWeight(
        tf15?.structure?.confidence
      )
    );

  // Agreement answers: "are 1h and 15m pointing in the same direction?"
  // The old version incorrectly gave full credit even when 1h was bullish
  // and 15m bearish, as long as both were non-range.
  const structureAgreement =
    structureDir1h === structureDir15 &&
    structureDir1h !== "neutral"
      ? 1
      : structureDir1h === "neutral" ||
        structureDir15 === "neutral"
        ? 0.50
        : 0.15;

  const maSignal = ma => {
    if (!ma) return "neutral";

    if (
      ma.order === "bullish" &&
      ma.direction === "up"
    ) {
      return "bullish";
    }

    if (
      ma.order === "bearish" &&
      ma.direction === "down"
    ) {
      return "bearish";
    }

    return "neutral";
  };

  const ma1h = maSignal(tf1h?.ma);
  const ma15 = maSignal(tf15?.ma);

  // The old implementation also gave full MA credit when one timeframe
  // was bullish and the other bearish. This fixes that.
  const maAgreement =
    ma1h === ma15 &&
    ma1h !== "neutral"
      ? 1
      : ma1h === "neutral" ||
        ma15 === "neutral"
        ? 0.55
        : 0.15;

  // Structure and MA agreeing inside each timeframe increases readability.
  const structureMaScore = (structureDir, maDir) =>
    structureDir === "neutral" ||
    maDir === "neutral"
      ? 0.50
      : structureDir === maDir
        ? 1
        : 0.20;

  const structureMaAgreement =
    0.60 * structureMaScore(
      structureDir1h,
      ma1h
    ) +
    0.40 * structureMaScore(
      structureDir15,
      ma15
    );

  // Do not use max(15m,1h) alone. A single isolated spike should not
  // receive the same score as broad multi-timeframe participation.
  const vol15 =
    Math.min(
      1,
      Number(tf15?.volume?.ratio || 0) / 2
    );

  const vol1h =
    Math.min(
      1,
      Number(tf1h?.volume?.ratio || 0) / 2
    );

  const volumeParticipation =
    0.65 * vol15 +
    0.35 * vol1h;

  // Candidates sitting near a meaningful 15m support/resistance are more
  // actionable than candidates floating in the middle of the range.
  // Normalize by 24h range so BTC and high-beta alts are treated comparably.
  const price = Number(market.price || 0);
  const support = Number(
    tf15?.levels?.recentSupport || 0
  );
  const resistance = Number(
    tf15?.levels?.recentResistance || 0
  );
  const dailyRange = Math.max(
    Number(market.high24h || 0) -
      Number(market.low24h || 0),
    price * 0.005
  );

  const distances = [
    support > 0 && support < price
      ? Math.abs(price - support)
      : Infinity,
    resistance > 0 && resistance > price
      ? Math.abs(resistance - price)
      : Infinity
  ];

  const nearestLevelDistance =
    Math.min(...distances);

  const normalizedLevelDistance =
    Number.isFinite(nearestLevelDistance)
      ? nearestLevelDistance / dailyRange
      : 1;

  // Full credit very close to a level; fades to zero around the middle
  // of the 24h range. This is only a bridge prefilter, not final R:R.
  const keyLevelProximity =
    Math.max(
      0,
      Math.min(
        1,
        1 - normalizedLevelDistance / 0.50
      )
    );

  const rawScore = round6(
    0.25 * structureClarity +
    0.20 * structureAgreement +
    0.20 * maAgreement +
    0.10 * structureMaAgreement +
    0.15 * volumeParticipation +
    0.10 * keyLevelProximity
  );

  const timeframeQuality =
    0.60 * tfWeight1h +
    0.40 * tfWeight15;

  const score = round6(
    rawScore *
    timeframeQuality
  );

  return {
    score,
    rawScore:
      typeof rawScore !== "undefined"
        ? rawScore
        : score,

    timeframeQuality:
      typeof timeframeQuality !== "undefined"
        ? round6(timeframeQuality)
        : 1,

    structureClarity:
      round6(structureClarity),

    structureAgreement:
      round6(structureAgreement),

    maAgreement:
      round6(maAgreement),

    structureMaAgreement:
      round6(structureMaAgreement),

    volumeParticipation:
      round6(volumeParticipation),

    keyLevelProximity:
      round6(keyLevelProximity),

    reason: [
      `1hStructure=${structure1h}`,
      `15mStructure=${structure15}`,
      `structureAgreement=${round6(structureAgreement)}`,
      `1hMA=${ma1h}`,
      `15mMA=${ma15}`,
      `maAgreement=${round6(maAgreement)}`,
      `structureMA=${round6(structureMaAgreement)}`,
      `volumeParticipation=${round6(volumeParticipation)}`,
      `keyLevelProximity=${round6(keyLevelProximity)}`
    ].join("; ")
  };
}

// ============================================================
// PER SYMBOL ANALYSIS
// ============================================================

async function analyzeSymbol(
  symbol,
  ticker,
  oi,
  previousMarket = null,
  subrequestBudget = null,
  fetchPolicy = {}
) {
  if (!ticker) {
    throw new Error(
      `Ticker missing for ${symbol}`
    );
  }

  const freshTicker =
    await ensureFreshTicker(
      symbol,
      ticker,
      subrequestBudget
    );

  const fetch15 =
    await fetchTimeframeWithFallback(
      symbol,
      "15m",
      120,
      15 * 60_000,
      previousMarket?.timeframe15m,
      previousMarket?.updatedAt,
      subrequestBudget,
      fetchPolicy.timeframeAttempts || 3
    );

  const interTimeframeDelayMs =
    Math.max(
      0,
      Number(
        fetchPolicy
          ?.interTimeframeDelayMs ||
        0
      )
    );

  if (
    interTimeframeDelayMs > 0
  ) {
    await sleep(
      interTimeframeDelayMs
    );
  }

  const fetch1h =
    await fetchTimeframeWithFallback(
      symbol,
      "1H",
      120,
      60 * 60_000,
      previousMarket?.timeframe1h,
      previousMarket?.updatedAt,
      subrequestBudget,
      fetchPolicy.timeframeAttempts || 3
    );

  const timeframe15m =
    fetch15.candles?.length >= 65
      ? analyzeTimeframe(
          fetch15.candles
        )
      : fetch15.fallbackTimeframe;

  const timeframe1h =
    fetch1h.candles?.length >= 65
      ? analyzeTimeframe(
          fetch1h.candles
        )
      : fetch1h.fallbackTimeframe;

  if (
    !timeframe15m &&
    !timeframe1h
  ) {
    throw new Error(
      `${symbol} both 15m and 1h unavailable`
    );
  }

  const now = Date.now();

  const tickerAgeMs =
    Math.max(
      0,
      now -
      freshTicker.ts
    );

  const oiAgeMs =
    oi?.ts
      ? Math.max(
          0,
          now - oi.ts
        )
      : null;

  const last15Ts =
    fetch15.lastClosedTs || 0;

  const last1hTs =
    fetch1h.lastClosedTs || 0;

  const partial =
    !fetch15.fetchOk ||
    !fetch1h.fetchOk ||
    !!fetch15.usedFallback ||
    !!fetch1h.usedFallback;

  const fallbackAges =
    [
      fetch15.fallbackAgeMinutes,
      fetch1h.fallbackAgeMinutes
    ].filter(
      x =>
        Number.isFinite(x)
    );

  const worstFallbackAge =
    fallbackAges.length
      ? Math.max(
          ...fallbackAges
        )
      : 0;

  let freshnessStatus =
    "fresh";

  if (worstFallbackAge > 60) {
    freshnessStatus =
      "expired";
  } else if (
    worstFallbackAge > 30
  ) {
    freshnessStatus =
      "auxiliary_only";
  } else if (
    partial ||
    tickerAgeMs >
      MAX_TICKER_AGE_MS ||
    (
      last15Ts &&
      !isLatestClosedCandlePresent(
        last15Ts,
        15 * 60_000,
        now
      )
    ) ||
    (
      last1hTs &&
      !isLatestClosedCandlePresent(
        last1hTs,
        60 * 60_000,
        now
      )
    )
  ) {
    freshnessStatus =
      worstFallbackAge <= 10
        ? "fallback_fresh"
        : "degraded";
  }

  const staleReasons =
    [
      fetch15.staleReason,
      fetch1h.staleReason
    ].filter(Boolean);

  const lastSuccessfulAt =
    latestIso(
      fetch15.lastSuccessfulAt,
      fetch1h.lastSuccessfulAt,
      previousMarket?.lastSuccessfulAt,
      previousMarket?.updatedAt
    );

  const turnoverUsdEstimate =
    freshTicker.volCcy24h *
    freshTicker.last;

  return {
    ok: true,
    symbol,

    updatedAt:
      new Date().toISOString(),

    price:
      freshTicker.last,

    high24h:
      freshTicker.high24h,

    low24h:
      freshTicker.low24h,

    open24h:
      freshTicker.open24h,

    changePct24h:
      freshTicker.open24h > 0
        ? (
            freshTicker.last -
            freshTicker.open24h
          ) /
          freshTicker.open24h
        : null,

    contractVolume24h:
      freshTicker.vol24h,

    baseVolume24h:
      freshTicker.volCcy24h,

    quoteVolume24hEstimate:
      turnoverUsdEstimate,

    turnoverUsdEstimate,

    openInterestUsdEstimate:
      oi?.oiUsdEstimate || 0,

    fetch15mOk:
      fetch15.fetchOk,

    fetch1hOk:
      fetch1h.fetchOk,

    retryCount15m:
      fetch15.retryCount,

    retryCount1h:
      fetch1h.retryCount,

    partial,

    stale:
      freshnessStatus !==
      "fresh",

    staleReason:
      staleReasons.length
        ? staleReasons.join(" | ")
        : null,

    lastSuccessfulAt,

    timeframeStatus: {
      "15m": {
        fetchOk:
          fetch15.fetchOk,

        usedFallback:
          !!fetch15.usedFallback,

        fallbackAgeMinutes:
          fetch15.fallbackAgeMinutes,

        realtimeWeight:
          fetch15.realtimeWeight,

        status:
          fetch15.status
      },

      "1h": {
        fetchOk:
          fetch1h.fetchOk,

        usedFallback:
          !!fetch1h.usedFallback,

        fallbackAgeMinutes:
          fetch1h.fallbackAgeMinutes,

        realtimeWeight:
          fetch1h.realtimeWeight,

        status:
          fetch1h.status
      }
    },

    freshness: {
      status:
        freshnessStatus,

      tickerTs:
        freshTicker.ts,

      tickerAgeMs,

      oiTs:
        oi?.ts || null,

      oiAgeMs,

      lastClosed15mTs:
        last15Ts || null,

      lastClosed1hTs:
        last1hTs || null,

      fallbackAgeMinutes:
        worstFallbackAge || 0
    },

    timeframe15m,
    timeframe1h
  };
}

function analyzeTimeframe(candles) {
  const closes = candles.map(
    c => c.close
  );

  const ma7Series = rollingMA(
    closes,
    7
  );

  const ma25Series = rollingMA(
    closes,
    25
  );

  const ma60Series = rollingMA(
    closes,
    60
  );

  const ma7 = lastFinite(
    ma7Series
  );

  const ma25 = lastFinite(
    ma25Series
  );

  const ma60 = lastFinite(
    ma60Series
  );

  const slope5 = {
    ma7: slopeOver(
      ma7Series,
      5
    ),
    ma25: slopeOver(
      ma25Series,
      5
    ),
    ma60: slopeOver(
      ma60Series,
      5
    )
  };

  const order =
    ma7 > ma25 &&
    ma25 > ma60
      ? "bullish"
      : ma7 < ma25 &&
        ma25 < ma60
        ? "bearish"
        : "mixed";

  const direction =
    slope5.ma7 > 0 &&
    slope5.ma25 > 0 &&
    slope5.ma60 > 0
      ? "up"
      : slope5.ma7 < 0 &&
        slope5.ma25 < 0 &&
        slope5.ma60 < 0
        ? "down"
        : "mixed";

  const structure =
    detectStructure(candles);

  const levels =
    detectLevels(
      candles,
      structure
    );

  const volume =
    detectVolume(candles);

  return {
    candleCount:
      candles.length,

    ma: {
      ma7,
      ma25,
      ma60,
      slope5,
      order,
      direction
    },

    structure,
    levels,
    volume,

    recentCandles:
      candles.slice(-12)
  };
}

// ============================================================
// STRUCTURE / LEVELS / VOLUME
// ============================================================

function detectStructure(candles) {
  const recent = candles.slice(-80);
  const atr = averageTrueRange(recent, 14);
  const lastClose = recent.at(-1)?.close || 0;

  // Use confirmed pivots only. A 2-bar left/right confirmation keeps the
  // structure responsive enough for 15m while avoiding raw-wick noise.
  const { highs: rawHighs, lows: rawLows } =
    findConfirmedPivots(recent, 2);

  // Filter tiny pivots whose separation is too small to matter.
  // 0.35 ATR is intentionally modest: bridge prefilter, not final entry logic.
  const minMove = Math.max(
    atr * 0.35,
    lastClose * 0.0005
  );

  const highs = compressPivots(rawHighs, minMove);
  const lows = compressPivots(rawLows, minMove);

  const h = highs.slice(-2);
  const l = lows.slice(-2);

  const previousHigh =
    h.length === 2 ? h[0].value : null;
  const recentHigh =
    h.length >= 1 ? h.at(-1).value : null;

  const previousLow =
    l.length === 2 ? l[0].value : null;
  const recentLow =
    l.length >= 1 ? l.at(-1).value : null;

  let state = "range";
  let confidence = "low";

  if (
    previousHigh != null &&
    recentHigh != null &&
    previousLow != null &&
    recentLow != null
  ) {
    const highDelta = recentHigh - previousHigh;
    const lowDelta = recentLow - previousLow;

    const threshold = Math.max(
      atr * 0.20,
      lastClose * 0.0003
    );

    if (
      highDelta > threshold &&
      lowDelta > threshold
    ) {
      state = "higher_highs_higher_lows";
      confidence = "high";
    } else if (
      highDelta < -threshold &&
      lowDelta < -threshold
    ) {
      state = "lower_highs_lower_lows";
      confidence = "high";
    } else {
      state = "range";
      confidence = "medium";
    }
  }

  return {
    state,
    confidence,
    previousHigh,
    recentHigh,
    previousLow,
    recentLow,
    atr,
    pivotHighCount: highs.length,
    pivotLowCount: lows.length,
    lastClose,
    lastCloseTs:
      recent.at(-1)?.ts || null
  };
}

function detectLevels(
  candles,
  structure
) {
  const recent = candles.slice(-80);
  const lastClose = recent.at(-1)?.close || 0;
  const atr =
    structure?.atr ||
    averageTrueRange(recent, 14);

  const { highs, lows } =
    findConfirmedPivots(recent, 2);

  // Raw candle highs/lows are NOT used directly as levels anymore.
  // Build clustered pivot zones instead, otherwise the nearest wick on any
  // candle can be mistaken for meaningful support/resistance.
  const tolerance = Math.max(
    atr * 0.30,
    lastClose * 0.001
  );

  const highClusters = clusterPivotLevels(
    highs,
    tolerance
  );

  const lowClusters = clusterPivotLevels(
    lows,
    tolerance
  );

  const resistanceZones = highClusters
    .filter(x => x.level > lastClose)
    .sort((a, b) => {
      const distance =
        (a.level - lastClose) -
        (b.level - lastClose);

      if (Math.abs(distance) > tolerance) {
        return distance;
      }

      return b.touches - a.touches;
    });

  const supportZones = lowClusters
    .filter(x => x.level < lastClose)
    .sort((a, b) => {
      const distance =
        (lastClose - a.level) -
        (lastClose - b.level);

      if (Math.abs(distance) > tolerance) {
        return distance;
      }

      return b.touches - a.touches;
    });

  const recentResistance =
    resistanceZones[0]?.level ?? null;

  const secondaryResistance =
    resistanceZones[1]?.level ?? null;

  const recentSupport =
    supportZones[0]?.level ?? null;

  const secondarySupport =
    supportZones[1]?.level ?? null;

  // When price has broken beyond every confirmed pivot on one side,
  // explicitly return null rather than inventing a resistance/support from
  // the latest candle wick. This lets the final model know price is in
  // price-discovery / fresh-breakdown territory.
  return {
    recentResistance,
    secondaryResistance,
    recentSupport,
    secondarySupport,

    recentResistanceTouches:
      resistanceZones[0]?.touches ?? 0,

    secondaryResistanceTouches:
      resistanceZones[1]?.touches ?? 0,

    recentSupportTouches:
      supportZones[0]?.touches ?? 0,

    secondarySupportTouches:
      supportZones[1]?.touches ?? 0,

    zoneTolerance: tolerance,

    aboveAllConfirmedResistance:
      highClusters.length > 0 &&
      highClusters.every(x => x.level <= lastClose),

    belowAllConfirmedSupport:
      lowClusters.length > 0 &&
      lowClusters.every(x => x.level >= lastClose)
  };
}

function findConfirmedPivots(candles, strength = 2) {
  const highs = [];
  const lows = [];

  for (
    let i = strength;
    i < candles.length - strength;
    i++
  ) {
    const c = candles[i];
    let isHigh = true;
    let isLow = true;

    for (let j = 1; j <= strength; j++) {
      if (
        c.high <= candles[i - j].high ||
        c.high <= candles[i + j].high
      ) {
        isHigh = false;
      }

      if (
        c.low >= candles[i - j].low ||
        c.low >= candles[i + j].low
      ) {
        isLow = false;
      }
    }

    if (isHigh) {
      highs.push({
        i,
        ts: c.ts,
        value: c.high
      });
    }

    if (isLow) {
      lows.push({
        i,
        ts: c.ts,
        value: c.low
      });
    }
  }

  return { highs, lows };
}

function compressPivots(pivots, minMove) {
  const out = [];

  for (const pivot of pivots) {
    const prev = out.at(-1);

    if (!prev) {
      out.push(pivot);
      continue;
    }

    if (Math.abs(pivot.value - prev.value) >= minMove) {
      out.push(pivot);
      continue;
    }

    // Nearby same-side pivots are treated as one structural area.
    // Keep the newer pivot because structure classification is recency-driven.
    out[out.length - 1] = pivot;
  }

  return out;
}

function clusterPivotLevels(pivots, tolerance) {
  const clusters = [];

  // More recent touches have mildly greater influence on the cluster center.
  for (let idx = 0; idx < pivots.length; idx++) {
    const pivot = pivots[idx];
    const recencyWeight =
      1 + idx / Math.max(1, pivots.length - 1);

    let best = null;
    let bestDistance = Infinity;

    for (const cluster of clusters) {
      const distance =
        Math.abs(cluster.level - pivot.value);

      if (
        distance <= tolerance &&
        distance < bestDistance
      ) {
        best = cluster;
        bestDistance = distance;
      }
    }

    if (!best) {
      clusters.push({
        level: pivot.value,
        touches: 1,
        weight: recencyWeight,
        lastTs: pivot.ts
      });
      continue;
    }

    const totalWeight =
      best.weight + recencyWeight;

    best.level =
      (
        best.level * best.weight +
        pivot.value * recencyWeight
      ) /
      totalWeight;

    best.weight = totalWeight;
    best.touches += 1;
    best.lastTs = Math.max(
      best.lastTs || 0,
      pivot.ts || 0
    );
  }

  return clusters.map(x => ({
    level: x.level,
    touches: x.touches,
    lastTs: x.lastTs
  }));
}

function averageTrueRange(candles, period = 14) {
  if (candles.length < 2) return 0;

  const trs = [];

  for (let i = 1; i < candles.length; i++) {
    const c = candles[i];
    const prevClose = candles[i - 1].close;

    trs.push(
      Math.max(
        c.high - c.low,
        Math.abs(c.high - prevClose),
        Math.abs(c.low - prevClose)
      )
    );
  }

  const use = trs.slice(-period);

  if (!use.length) return 0;

  return use.reduce((a, b) => a + b, 0) / use.length;
}

function detectVolume(candles) {
  const last =
    candles[
      candles.length - 1
    ]?.volume || 0;

  const prev20 =
    candles.slice(-21, -1);

  const average20 =
    prev20.length
      ? prev20.reduce(
          (sum, x) =>
            sum + x.volume,
          0
        ) / prev20.length
      : 0;

  return {
    lastVolume: last,
    average20,

    ratio:
      average20 > 0
        ? last / average20
        : 0
  };
}

// ============================================================
// OKX
// ============================================================

async function okxGet(
  path,
  attempts = 3,
  subrequestBudget = null
) {
  // Prefer non-Cloudflare egress when configured.
  if (ACTIVE_OKX_SIDECAR_URL) {
    try {
      return await sidecarFetchJson(
        ACTIVE_OKX_SIDECAR_URL,
        "okx",
        path,
        subrequestBudget,
        6500
      );
    } catch (sidecarErr) {
      // Sidecar failure must not kill the bridge.
      // Fall back to direct OKX so the worker can self-heal
      // if the sidecar is temporarily unavailable.
    }
  }

  let lastErr;

  for (
    let i = 0;
    i < attempts;
    i++
  ) {
    try {
      consumeSubrequest(
        subrequestBudget,
        `OKX ${path}`
      );

      const resp =
        await fetch(
          OKX_BASE + path,
          {
            headers: {
              "User-Agent":
                "dynamic-market-bridge-worker/6.3.1"
            }
          }
        );

      const retryAfter =
        Number(
          resp.headers.get(
            "Retry-After"
          ) || 0
        );

      if (!resp.ok) {
        const retryable =
          resp.status === 429 ||
          resp.status >= 500;

        if (!retryable) {
          throw new NonRetryableError(
            `OKX HTTP ${resp.status}`
          );
        }

        const err =
          new Error(
            `OKX HTTP ${resp.status}`
          );

        err.retryAfterMs =
          retryAfter > 0
            ? retryAfter * 1000
            : 0;

        throw err;
      }

      const body =
        await resp.json();

      if (body.code !== "0") {
        if (
          String(body.code) !==
          "50011"
        ) {
          throw new NonRetryableError(
            `OKX code=${body.code} msg=${body.msg || ""}`
          );
        }

        const err =
          new Error(
            `OKX rate limited: ${body.msg || "50011"}`
          );

        err.retryAfterMs = 0;
        throw err;
      }

      return body;

    } catch (err) {
      lastErr = err;

      if (
        err instanceof
        NonRetryableError
      ) {
        throw err;
      }

      if (
        err instanceof
        SubrequestBudgetError
      ) {
        throw err;
      }

      if (
        i <
        attempts - 1
      ) {
        const isRateLimit =
          /HTTP 429|rate limited|50011/i
            .test(
              String(
                err?.message ||
                ""
              )
            );

        const baseBackoff =
          isRateLimit
            ? okxRateLimitBackoffBase()
            : 800;

        const exponential =
          baseBackoff *
          (2 ** i);

        const jitter =
          Math.floor(
            Math.random() *
            300
          );

        await sleep(
          Math.max(
            err?.retryAfterMs || 0,
            exponential + jitter
          )
        );
      }
    }
  }

  throw lastErr;
}

class NonRetryableError
  extends Error {}

class SubrequestBudgetError
  extends Error {}

function createSubrequestBudget(
  max,
  reserve
) {
  return {
    max,
    reserve,
    used: 0
  };
}

function canSpendSubrequest(
  budget,
  count = 1,
  allowReserve = false
) {
  if (!budget) {
    return true;
  }

  const ceiling =
    allowReserve
      ? budget.max
      : budget.max -
        budget.reserve;

  return (
    budget.used +
    count
  ) <= ceiling;
}

function consumeSubrequest(
  budget,
  label = "external request",
  allowReserve = false
) {
  if (!budget) {
    return;
  }

  if (
    !canSpendSubrequest(
      budget,
      1,
      allowReserve
    )
  ) {
    throw new SubrequestBudgetError(
      `Subrequest budget exhausted before ${label}; used=${budget.used}, max=${budget.max}, reserve=${budget.reserve}`
    );
  }

  budget.used++;
}

async function getOkxSwapInstruments(
  subrequestBudget = null
) {
  return okxGet(
    "/api/v5/public/instruments?instType=SWAP",
    2,
    subrequestBudget
  );
}

function buildOkxInstrumentMap(
  rows
) {
  const map =
    new Map();

  for (
    const row of rows
  ) {
    if (!row?.instId) {
      continue;
    }

    map.set(
      row.instId,
      {
        instId:
          row.instId,

        instCategory:
          String(
            row.instCategory ||
            ""
          ),

        state:
          String(
            row.state ||
            ""
          ),

        ruleType:
          String(
            row.ruleType ||
            ""
          ),

        groupId:
          String(
            row.groupId ||
            ""
          ),

        uly:
          String(
            row.uly ||
            ""
          )
      }
    );
  }

  return map;
}

async function getBinanceDiscoveryBundleSafe(
  subrequestBudget = null
) {
  const attemptedAt = new Date().toISOString();

  // V6.2.3 circuit breaker: probe exchangeInfo first. A Cloudflare-region
  // Binance 403 is deterministic for the run, so do not waste a second
  // request on /ticker/24hr after the first 403. Next Cron run probes again.
  let exchangeInfo;
  try {
    exchangeInfo = await getBinanceExchangeInfo(subrequestBudget);
  } catch (err) {
    const classified = classifyBinanceError(err);
    return {
      ok: false,
      status: classified.status,
      attemptedAt,
      exchangeInfo: null,
      tickerRows: [],
      error: classified.message,
      circuitBroken: classified.status === "blocked_403"
    };
  }

  let tickerRows;
  try {
    tickerRows = await getBinance24hTickers(subrequestBudget);
  } catch (err) {
    const classified = classifyBinanceError(err);
    return {
      ok: false,
      status: classified.status,
      attemptedAt,
      exchangeInfo,
      tickerRows: [],
      error: classified.message,
      circuitBroken: classified.status === "blocked_403"
    };
  }

  return {
    ok: true,
    status: "ok",
    attemptedAt,
    exchangeInfo,
    tickerRows,
    error: null,
    circuitBroken: false
  };
}

function classifyBinanceError(
  err
) {
  const message =
    String(
      err?.message ||
      err ||
      "unknown Binance error"
    );

  if (/HTTP 403/i.test(message)) {
    return {
      status: "blocked_403",
      message
    };
  }

  if (/HTTP 429/i.test(message)) {
    return {
      status: "rate_limited",
      message
    };
  }

  if (/timeout|timed out|AbortError/i.test(message)) {
    return {
      status: "timeout",
      message
    };
  }

  if (/HTTP 5\d\d/i.test(message)) {
    return {
      status: "server_error",
      message
    };
  }

  if (/Subrequest budget exhausted/i.test(message)) {
    return {
      status: "budget_exhausted",
      message
    };
  }

  return {
    status: "unavailable",
    message
  };
}

async function getBinanceExchangeInfo(
  subrequestBudget = null
) {
  return binanceGet(
    "/fapi/v1/exchangeInfo",
    2,
    subrequestBudget
  );
}

async function getBinance24hTickers(
  subrequestBudget = null
) {
  return binanceGet(
    "/fapi/v1/ticker/24hr",
    2,
    subrequestBudget
  );
}

async function recheckCandidateAnomaly(
  okxSymbol,
  subrequestBudget = null
) {
  const binanceSymbol =
    `${baseFromOkxSymbol(okxSymbol)}USDT`;

  try {
    const [klinesRaw, oiRowsRaw] = await Promise.all([
      binanceGet(
        `/fapi/v1/klines?symbol=${encodeURIComponent(binanceSymbol)}&interval=5m&limit=4`,
        1,
        subrequestBudget
      ),
      binanceGet(
        `/futures/data/openInterestHist?symbol=${encodeURIComponent(binanceSymbol)}&period=5m&limit=3`,
        1,
        subrequestBudget
      )
    ]);

    const now = Date.now();
    const closedKlines = (Array.isArray(klinesRaw) ? klinesRaw : [])
      .filter(row => Array.isArray(row) && num(row[6]) <= now)
      .slice(-3);
    const oiRows = (Array.isArray(oiRowsRaw) ? oiRowsRaw : [])
      .filter(row => num(row?.sumOpenInterest) > 0)
      .slice(-3);

    if (closedKlines.length < 3 || oiRows.length < 3) {
      return {
        symbol: okxSymbol,
        ok: false,
        promoted: false,
        reason: "insufficient_lightweight_samples"
      };
    }

    const latest = closedKlines[2];
    const latestClose = num(latest[4]);
    const price5mPct = percentChange(latestClose, num(closedKlines[1][4]));
    const price10mPct = percentChange(latestClose, num(closedKlines[0][4]));
    const totalVolume = num(latest[5]);
    const takerBuyVolume = num(latest[9]);
    const takerSellVolume = Math.max(0, totalVolume - takerBuyVolume);
    const takerBuySellRatio = takerSellVolume > 0
      ? takerBuyVolume / takerSellVolume
      : null;

    const latestOi = num(oiRows[2].sumOpenInterest);
    const oi5mPct = percentChange(latestOi, num(oiRows[1].sumOpenInterest));
    const oi10mPct = percentChange(latestOi, num(oiRows[0].sumOpenInterest));

    const priceBull =
      price5mPct >= ANOMALY_PRICE_5M_PCT ||
      price10mPct >= ANOMALY_PRICE_10M_PCT;
    const priceBear =
      price5mPct <= -ANOMALY_PRICE_5M_PCT ||
      price10mPct <= -ANOMALY_PRICE_10M_PCT;
    const oiBull =
      oi5mPct >= ANOMALY_OI_5M_PCT ||
      oi10mPct >= ANOMALY_OI_10M_PCT;
    const oiBear =
      oi5mPct <= -ANOMALY_OI_5M_PCT ||
      oi10mPct <= -ANOMALY_OI_10M_PCT;
    const takerBull = takerBuySellRatio !== null &&
      takerBuySellRatio >= ANOMALY_TAKER_BULL;
    const takerBear = takerBuySellRatio !== null &&
      takerBuySellRatio <= ANOMALY_TAKER_BEAR;

    const bullishSignals = [priceBull, oiBull, takerBull].filter(Boolean).length;
    const bearishSignals = [priceBear, oiBear, takerBear].filter(Boolean).length;
    const direction = bullishSignals > bearishSignals
      ? "bullish"
      : bearishSignals > bullishSignals
        ? "bearish"
        : "mixed";
    const confirmedSignals = Math.max(bullishSignals, bearishSignals);
    const promoted = direction !== "mixed" && confirmedSignals >= 2;
    const strength = round6(
      confirmedSignals +
      Math.max(Math.abs(price5mPct), Math.abs(price10mPct)) +
      Math.max(Math.abs(oi5mPct), Math.abs(oi10mPct))
    );

    return {
      symbol: okxSymbol,
      ok: true,
      promoted,
      direction,
      confirmedSignals,
      strength,
      latestClosedAt: new Date(num(latest[6])).toISOString(),
      price: {
        latest: latestClose,
        change5mPct: round6(price5mPct),
        change10mPct: round6(price10mPct),
        significant: direction === "bullish" ? priceBull : priceBear
      },
      taker: {
        buySellRatio: takerBuySellRatio === null ? null : round6(takerBuySellRatio),
        significant: direction === "bullish" ? takerBull : takerBear
      },
      openInterest: {
        latest: latestOi,
        change5mPct: round6(oi5mPct),
        change10mPct: round6(oi10mPct),
        significant: direction === "bullish" ? oiBull : oiBear
      },
      reason: promoted
        ? `fast_lane_${direction}_${confirmedSignals}_of_3`
        : "no_multi_field_anomaly"
    };
  } catch (err) {
    return {
      symbol: okxSymbol,
      ok: false,
      promoted: false,
      reason: String(err?.message || err)
    };
  }
}

function percentChange(current, previous) {
  return previous > 0
    ? ((current - previous) / previous) * 100
    : 0;
}

async function binanceGet(
  path,
  attempts = 2,
  subrequestBudget = null
) {
  let lastErr = null;

  for (
    let i = 0;
    i < attempts;
    i++
  ) {
    try {
      // V6.3.1: full /run now uses the same sidecar-first transport
      // as the Cron probe. This fixes the V6.3.0 bug where full discovery
      // still called fapi.binance.com directly from Cloudflare.
      if (ACTIVE_BINANCE_SIDECAR_URL) {
        try {
          return await sidecarFetchJson(
            ACTIVE_BINANCE_SIDECAR_URL,
            "binance",
            path,
            subrequestBudget,
            BINANCE_TIMEOUT_MS
          );
        } catch (sidecarErr) {
          // Sidecar failure falls back to direct Binance.
          // This preserves resilience while preferring the non-Cloudflare egress.
          lastErr = sidecarErr;
        }
      }

      consumeSubrequest(
        subrequestBudget,
        `Binance direct ${path}`
      );

      const controller =
        new AbortController();

      const timeoutId =
        setTimeout(
          () =>
            controller.abort(),
          BINANCE_TIMEOUT_MS
        );

      let resp;

      try {
        resp =
          await fetch(
            BINANCE_FAPI_BASE +
            path,
            {
              headers: {
                "User-Agent":
                  "dynamic-market-bridge-worker/6.3.1"
              },
              signal:
                controller.signal
            }
          );
      } finally {
        clearTimeout(
          timeoutId
        );
      }

      if (!resp.ok) {
        const retryable =
          resp.status === 429 ||
          resp.status >= 500;

        if (!retryable) {
          throw new NonRetryableError(
            `Binance HTTP ${resp.status}`
          );
        }

        throw new Error(
          `Binance HTTP ${resp.status}`
        );
      }

      return await resp.json();

    } catch (err) {
      lastErr = err;

      if (
        err instanceof
        NonRetryableError ||
        err instanceof
        SubrequestBudgetError
      ) {
        throw err;
      }

      if (
        i <
        attempts - 1 &&
        canSpendSubrequest(
          subrequestBudget,
          1
        )
      ) {
        await sleep(
          700 +
          Math.floor(
            Math.random() *
            250
          )
        );
      }
    }
  }

  throw lastErr;
}

function buildBinanceUniverse(
  exchangeInfo,
  tickerRows
) {
  const metaBySymbol =
    new Map();

  for (
    const s of
    exchangeInfo?.symbols ||
    []
  ) {
    if (
      !s?.symbol ||
      s.quoteAsset !== "USDT" ||
      s.contractType !== "PERPETUAL" ||
      s.status !== "TRADING"
    ) {
      continue;
    }

    // Binance USDⓈ-M also carries TradFi/stock perpetuals.
    // Keep only instruments whose margin/base metadata looks like normal
    // crypto perpetuals. Cross-exchange validation still requires OKX crypto
    // category on the other side.
    metaBySymbol.set(
      s.symbol,
      {
        symbol:
          s.symbol,

        baseAsset:
          String(
            s.baseAsset ||
            ""
          ),

        quoteAsset:
          String(
            s.quoteAsset ||
            ""
          ),

        marginAsset:
          String(
            s.marginAsset ||
            ""
          ),

        contractType:
          s.contractType,

        status:
          s.status
      }
    );
  }

  const out =
    new Map();

  for (
    const row of
    tickerRows || []
  ) {
    const meta =
      metaBySymbol.get(
        row.symbol
      );

    if (!meta) {
      continue;
    }

    const base =
      normalizeBaseSymbol(
        meta.baseAsset
      );

    if (!base) {
      continue;
    }

    out.set(
      base,
      {
        eligible: true,
        symbol:
          meta.symbol,
        baseAsset:
          meta.baseAsset,

        lastPrice:
          num(
            row.lastPrice
          ),

        openPrice:
          num(
            row.openPrice
          ),

        highPrice:
          num(
            row.highPrice
          ),

        lowPrice:
          num(
            row.lowPrice
          ),

        quoteVolume:
          num(
            row.quoteVolume
          ),

        volume:
          num(
            row.volume
          )
      }
    );
  }

  return out;
}

function baseFromOkxSymbol(
  symbol
) {
  return normalizeBaseSymbol(
    String(
      symbol ||
      ""
    ).split("-")[0]
  );
}

function normalizeBaseSymbol(
  base
) {
  const value =
    String(
      base ||
      ""
    )
      .trim()
      .toUpperCase();

  if (!value) {
    return "";
  }

  return value;
}

async function getFreshTickerUniverse(subrequestBudget = null) {
  const first = await okxGet("/api/v5/market/tickers?instType=SWAP", 3, subrequestBudget);
  const firstRows = first.data || [];

  if (!snapshotNeedsRefresh(firstRows, MAX_TICKER_AGE_MS)) {
    return first;
  }

  await sleep(180);
  const second = await okxGet("/api/v5/market/tickers?instType=SWAP", 2, subrequestBudget);

  return {
    ...second,
    data: mergeNewestByInstId(firstRows, second.data || [])
  };
}

async function getFreshOiUniverse(subrequestBudget = null) {
  const first = await okxGet("/api/v5/public/open-interest?instType=SWAP", 3, subrequestBudget);
  const firstRows = first.data || [];

  if (!snapshotNeedsRefresh(firstRows, MAX_OI_AGE_MS)) {
    return first;
  }

  await sleep(180);
  const second = await okxGet("/api/v5/public/open-interest?instType=SWAP", 2, subrequestBudget);

  return {
    ...second,
    data: mergeNewestByInstId(firstRows, second.data || [])
  };
}

async function ensureFreshTicker(symbol, ticker, subrequestBudget = null) {
  const age = Math.max(0, Date.now() - Number(ticker.ts || 0));
  if (age <= MAX_TICKER_AGE_MS) return ticker;

  await sleep(120);
  const retry = await okxGet(
    `/api/v5/market/ticker?instId=${encodeURIComponent(symbol)}`,
    2,
    subrequestBudget
  );

  const candidate = normalizeTicker(retry.data?.[0] || {});
  return candidate.ts > ticker.ts ? candidate : ticker;
}

async function fetchTimeframeWithFallback(
  symbol,
  bar,
  limit,
  intervalMs,
  previousTimeframe,
  previousUpdatedAt,
  subrequestBudget = null,
  maxAttempts = 3
) {
  const path =
    `/api/v5/market/candles?instId=${encodeURIComponent(symbol)}&bar=${encodeURIComponent(bar)}&limit=${limit}`;

  let lastErr = null;
  let retryCount = 0;

  for (
    let attempt = 0;
    attempt < maxAttempts;
    attempt++
  ) {
    try {
      const raw =
        await okxGet(
          path,
          1,
          subrequestBudget
        );

      const candles =
        normalizeCandles(
          raw.data || []
        );

      const lastClosedTs =
        candles.at(-1)?.ts ||
        0;

      if (
        candles.length >= 65 &&
        isLatestClosedCandlePresent(
          lastClosedTs,
          intervalMs,
          Date.now()
        )
      ) {
        return {
          fetchOk: true,
          retryCount,
          candles,
          fallbackTimeframe: null,
          usedFallback: false,
          fallbackAgeMinutes: 0,
          realtimeWeight: 1,
          status: "fresh",
          staleReason: null,
          lastClosedTs,
          lastSuccessfulAt:
            new Date().toISOString()
        };
      }

      lastErr =
        new Error(
          `${symbol} ${bar} stale or insufficient candles`
        );

    } catch (err) {
      lastErr = err;

      if (
        err instanceof
        SubrequestBudgetError
      ) {
        break;
      }
    }

    if (
      attempt < maxAttempts - 1 &&
      canSpendSubrequest(
        subrequestBudget,
        1
      )
    ) {
      retryCount++;

      await sleep(
        450 *
        (2 ** attempt) +
        Math.floor(
          Math.random() *
          250
        )
      );
    }
  }

  const fallbackAgeMinutes =
    updatedAgeMinutes(
      previousUpdatedAt
    );

  if (
    previousTimeframe &&
    fallbackAgeMinutes <= 60
  ) {
    let status =
      "auxiliary_only";

    let realtimeWeight = 0;

    if (
      fallbackAgeMinutes <= 10
    ) {
      status =
        "fallback_fresh";

      realtimeWeight = 1;
    } else if (
      fallbackAgeMinutes <= 30
    ) {
      status =
        "degraded";

      realtimeWeight = 0.55;
    }

    return {
      fetchOk: false,
      retryCount,
      candles: null,

      fallbackTimeframe:
        previousTimeframe,

      usedFallback: true,

      fallbackAgeMinutes:
        round6(
          fallbackAgeMinutes
        ),

      realtimeWeight,

      status,

      staleReason:
        `${symbol} ${bar} fresh fetch failed after ${retryCount + 1} attempts: ${lastErr?.message || "unknown"}`,

      lastClosedTs:
        Number(
          previousTimeframe
            ?.structure
            ?.lastCloseTs ||
          previousTimeframe
            ?.lastClosedTs ||
          0
        ),

      lastSuccessfulAt:
        previousUpdatedAt ||
        null
    };
  }

  return {
    fetchOk: false,
    retryCount,
    candles: null,
    fallbackTimeframe: null,
    usedFallback: false,

    fallbackAgeMinutes:
      Number.isFinite(
        fallbackAgeMinutes
      )
        ? round6(
            fallbackAgeMinutes
          )
        : null,

    realtimeWeight: 0,
    status: "failed",

    staleReason:
      `${symbol} ${bar} unavailable: ${lastErr?.message || "unknown"}`,

    lastClosedTs: 0,
    lastSuccessfulAt: null
  };
}

function latestIso(...values) {
  const valid =
    values
      .filter(Boolean)
      .map(x => ({
        raw: x,
        ts: Date.parse(x)
      }))
      .filter(
        x =>
          Number.isFinite(
            x.ts
          )
      )
      .sort(
        (a, b) =>
          b.ts - a.ts
      );

  return valid[0]?.raw ||
    null;
}

async function fetchFreshClosedCandles(symbol, bar, limit, intervalMs) {
  const path =
    `/api/v5/market/candles?instId=${encodeURIComponent(symbol)}&bar=${encodeURIComponent(bar)}&limit=${limit}`;

  const firstRaw = await okxGet(path);
  let best = normalizeCandles(firstRaw.data || []);

  const lastTs = best.at(-1)?.ts || 0;

  if (!isLatestClosedCandlePresent(lastTs, intervalMs, Date.now())) {
    await sleep(160);
    const secondRaw = await okxGet(path);
    const second = normalizeCandles(secondRaw.data || []);

    if ((second.at(-1)?.ts || 0) > lastTs) {
      best = second;
    }
  }

  return best;
}

function isLatestClosedCandlePresent(lastTs, intervalMs, now = Date.now()) {
  if (!lastTs) return false;

  const currentBucketStart =
    Math.floor(now / intervalMs) * intervalMs;

  const expectedLatestClosedStart =
    currentBucketStart - intervalMs;

  // Allow one completed interval of tolerance around exchange/cache boundaries.
  return lastTs >= expectedLatestClosedStart - intervalMs;
}

function snapshotNeedsRefresh(rows, maxAgeMs) {
  const now = Date.now();
  const timestamps = rows
    .map(x => Number(x.ts || 0))
    .filter(x => Number.isFinite(x) && x > 0);

  if (!timestamps.length) return true;

  const staleCount = timestamps.filter(ts => now - ts > maxAgeMs).length;
  return staleCount / timestamps.length > 0.15;
}

function mergeNewestByInstId(a, b) {
  const map = new Map();

  for (const row of [...a, ...b]) {
    const key = row.instId;
    if (!key) continue;

    const old = map.get(key);
    if (!old || Number(row.ts || 0) > Number(old.ts || 0)) {
      map.set(key, row);
    }
  }

  return [...map.values()];
}

function normalizeTicker(t) {
  return {
    symbol: t.instId,

    last: num(t.last),
    open24h: num(t.open24h),
    high24h: num(t.high24h),
    low24h: num(t.low24h),

    vol24h: num(t.vol24h),
    volCcy24h:
      num(t.volCcy24h),

    ts: num(t.ts)
  };
}

function normalizeCandles(data) {
  return data
    .filter(
      x =>
        !x[8] ||
        String(x[8]) === "1"
    )
    .map(x => ({
      ts: num(x[0]),

      time:
        new Date(
          num(x[0])
        ).toISOString(),

      open: num(x[1]),
      high: num(x[2]),
      low: num(x[3]),
      close: num(x[4]),

      volume: num(x[5]),
      quoteVolume:
        num(x[7] || x[6])
    }))
    .sort(
      (a, b) =>
        a.ts - b.ts
    );
}

function buildOiMap(
  rows,
  tickers
) {
  const lastMap =
    new Map(
      tickers.map(
        t => [
          t.symbol,
          t.last
        ]
      )
    );

  const out =
    new Map();

  for (
    const x of rows
  ) {
    const symbol =
      x.instId;

    if (!symbol) continue;

    const last =
      lastMap.get(symbol) || 0;

    const directUsd =
      num(x.oiUsd);

    const oiCcy =
      num(x.oiCcy);

    const oi =
      num(x.oi);

    const oiUsdEstimate =
      directUsd > 0
        ? directUsd
        : oiCcy > 0
          ? oiCcy * last
          : oi * last;

    out.set(
      symbol,
      {
        oi,
        oiCcy,
        oiUsdEstimate,
        ts: num(x.ts)
      }
    );
  }

  return out;
}

function isEligibleUsdtSwap(
  symbol,
  okxInstrumentMap = null,
  excludedBases = new Set()
) {
  if (
    !symbol ||
    !symbol.endsWith(
      "-USDT-SWAP"
    )
  ) {
    return false;
  }

  const base =
    baseFromOkxSymbol(
      symbol
    );

  const stableLike =
    new Set([
      "USDC",
      "USDT",
      "DAI",
      "FDUSD",
      "TUSD",
      "USDE",
      "USDS",
      "USD1",
      "PYUSD"
    ]);

  if (
    stableLike.has(base) ||
    excludedBases.has(base)
  ) {
    return false;
  }

  const instrument =
    okxInstrumentMap?.get(
      symbol
    );

  if (instrument) {
    // Official OKX instCategory:
    // 1 Crypto, 3 Stocks, 4 Commodities, 5 Forex, 6 Bonds.
    // Dynamic crypto pool is intentionally crypto-only.
    if (
      instrument.instCategory &&
      instrument.instCategory !== "1"
    ) {
      return false;
    }

    if (
      instrument.state &&
      instrument.state !== "live"
    ) {
      return false;
    }

    // SWAP discovery should be normal crypto perpetuals.
    // Unknown/empty remains allowed because some legacy crypto instruments may
    // not expose every metadata field consistently.
    if (
      instrument.ruleType &&
      ![
        "normal",
        "rebase_contract"
      ].includes(
        instrument.ruleType
      )
    ) {
      return false;
    }
  }

  return true;
}

// ============================================================
// GITHUB
// ============================================================

async function githubGetJson(
  gh,
  path,
  subrequestBudget = null
) {
  const url =
    `https://api.github.com/repos/${gh.owner}/${gh.repo}/contents/${encodePath(path)}?ref=${encodeURIComponent(gh.branch)}`;

  consumeSubrequest(
    subrequestBudget,
    `GitHub GET ${path}`
  );

  const resp =
    await fetch(
      url,
      {
        headers:
          githubHeaders(gh)
      }
    );

  if (
    resp.status === 404
  ) {
    return null;
  }

  if (!resp.ok) {
    throw new Error(
      `GitHub GET ${path} HTTP ${resp.status}`
    );
  }

  const body =
    await resp.json();

  const text =
    base64ToUtf8(
      body.content || ""
    );

  return {
    sha: body.sha,
    json:
      JSON.parse(text)
  };
}

async function githubPutJson(
  gh,
  path,
  obj,
  sha,
  message,
  subrequestBudget = null
) {
  const url =
    `https://api.github.com/repos/${gh.owner}/${gh.repo}/contents/${encodePath(path)}`;

  const content =
    JSON.stringify(
      obj,
      null,
      2
    );

  let currentSha =
    sha || null;

  for (
    let attempt = 0;
    attempt < 3;
    attempt++
  ) {
    const payload = {
      message,

      content:
        utf8ToBase64(
          content
        ),

      branch: gh.branch
    };

    if (currentSha) {
      payload.sha =
        currentSha;
    }

    consumeSubrequest(
      subrequestBudget,
      `GitHub PUT ${path}`,
      true
    );

    const resp =
      await fetch(
        url,
        {
          method: "PUT",

          headers: {
            ...githubHeaders(gh),

            "Content-Type":
              "application/json"
          },

          body:
            JSON.stringify(
              payload
            )
        }
      );

    if (resp.ok) {
      const body =
        await resp.json();

      return {
        commitSha:
          body.commit?.sha ||
          null,

        contentSha:
          body.content?.sha ||
          null
      };
    }

    const errorText =
      await resp.text();

    if (
      (
        resp.status === 409 ||
        resp.status === 422
      ) &&
      attempt < 2
    ) {
      const current =
        await githubGetJson(
          gh,
          path,
          subrequestBudget
        ).catch(
          () => null
        );

      currentSha =
        current?.sha ||
        null;

      await sleep(
        350 +
        attempt * 350
      );

      continue;
    }

    throw new Error(
      `GitHub PUT ${path} HTTP ${resp.status}: ${errorText.slice(0, 300)}`
    );
  }

  throw new Error(
    `GitHub PUT ${path} exhausted conflict retries`
  );
}

function githubHeaders(gh) {
  return {
    "Authorization":
      `Bearer ${gh.token}`,

    "Accept":
      "application/vnd.github+json",

    "X-GitHub-Api-Version":
      "2022-11-28",

    "User-Agent":
      "dynamic-market-bridge-worker"
  };
}

// ============================================================
// OUTPUT COMPATIBILITY
// ============================================================

function compactMarket(m) {
  return {
    price: m.price,
    high24h: m.high24h,
    low24h: m.low24h,

    turnoverUsdEstimate:
      m.turnoverUsdEstimate,

    openInterestUsdEstimate:
      m.openInterestUsdEstimate,

    freshness:
      m.freshness || null,

    fetch15mOk:
      m.fetch15mOk ?? null,

    fetch1hOk:
      m.fetch1hOk ?? null,

    retryCount15m:
      m.retryCount15m ?? 0,

    retryCount1h:
      m.retryCount1h ?? 0,

    partial:
      !!m.partial,

    stale:
      !!m.stale,

    staleReason:
      m.staleReason || null,

    lastSuccessfulAt:
      m.lastSuccessfulAt || null,

    timeframeStatus:
      m.timeframeStatus || null,

    timeframe15m:
      compactTimeframe(
        m.timeframe15m
      ),

    timeframe1h:
      compactTimeframe(
        m.timeframe1h
      )
  };
}

function compactTimeframe(tf) {
  if (!tf) {
    return null;
  }

  return {
    ma: tf.ma,
    structure: tf.structure,
    levels: tf.levels,
    volume: tf.volume
  };
}

// ============================================================
// MATH / UTILS
// ============================================================

function rollingMA(
  values,
  period
) {
  const out =
    new Array(
      values.length
    ).fill(null);

  let sum = 0;

  for (
    let i = 0;
    i < values.length;
    i++
  ) {
    sum += values[i];

    if (
      i >= period
    ) {
      sum -=
        values[
          i - period
        ];
    }

    if (
      i >=
      period - 1
    ) {
      out[i] =
        sum / period;
    }
  }

  return out;
}

function slopeOver(
  series,
  bars
) {
  const values =
    series.filter(
      Number.isFinite
    );

  if (
    values.length <= bars
  ) {
    return 0;
  }

  return (
    values[
      values.length - 1
    ] -
    values[
      values.length - 1 - bars
    ]
  );
}

function lastFinite(arr) {
  for (
    let i =
      arr.length - 1;
    i >= 0;
    i--
  ) {
    if (
      Number.isFinite(arr[i])
    ) {
      return arr[i];
    }
  }

  return null;
}

function percentileScore(
  value,
  values
) {
  const clean =
    values
      .filter(
        Number.isFinite
      )
      .sort(
        (a, b) =>
          a - b
      );

  if (
    !clean.length
  ) {
    return 0;
  }

  let count = 0;

  for (
    const x of clean
  ) {
    if (
      x <= value
    ) {
      count++;
    }
  }

  if (
    clean.length === 1
  ) {
    return 1;
  }

  return (
    count - 1
  ) / (
    clean.length - 1
  );
}

async function mapLimit(
  items,
  concurrency,
  fn,
  pauseMs = 0
) {
  const results = [];

  for (
    let i = 0;
    i < items.length;
    i += concurrency
  ) {
    const batch =
      items.slice(
        i,
        i + concurrency
      );

    const batchResults =
      await Promise.all(
        batch.map(fn)
      );

    results.push(
      ...batchResults
    );

    if (
      i + concurrency <
        items.length &&
      pauseMs > 0
    ) {
      await sleep(
        pauseMs
      );
    }
  }

  return results;
}

function parseCsv(
  value,
  fallback
) {
  if (!value) {
    return [
      ...fallback
    ];
  }

  return unique(
    value
      .split(",")
      .map(
        x => x.trim()
      )
      .filter(Boolean)
  );
}


function updatedAgeMinutes(value) {
  if (!value) {
    return Infinity;
  }

  const ts =
    Date.parse(value);

  if (!Number.isFinite(ts)) {
    return Infinity;
  }

  return Math.max(
    0,
    (
      Date.now() -
      ts
    ) /
    60_000
  );
}

function hasCurrentMaSchema(
  market
) {
  const ma15 =
    market
      ?.timeframe15m
      ?.ma;

  const ma1h =
    market
      ?.timeframe1h
      ?.ma;

  return (
    Number.isFinite(
      Number(ma15?.ma7)
    ) &&
    Number.isFinite(
      Number(ma15?.ma25)
    ) &&
    Number.isFinite(
      Number(ma15?.ma60)
    ) &&
    Number.isFinite(
      Number(ma1h?.ma7)
    ) &&
    Number.isFinite(
      Number(ma1h?.ma25)
    ) &&
    Number.isFinite(
      Number(ma1h?.ma60)
    )
  );
}

function isUsableStaleFallback(
  market,
  ageMinutes,
  maxAgeMinutes
) {
  return (
    !!market &&
    ageMinutes <=
      maxAgeMinutes &&
    hasCurrentMaSchema(
      market
    )
  );
}

function positiveNumber(value, fallback) {
  const n = Number(value);
  return Number.isFinite(n) && n >= 0 ? n : fallback;
}

function boundedNumber(value, fallback, minV, maxV) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(minV, Math.min(maxV, n));
}

function clampInt(
  value,
  fallback,
  minV,
  maxV
) {
  const n =
    Number.parseInt(
      value,
      10
    );

  if (
    !Number.isFinite(n)
  ) {
    return fallback;
  }

  return Math.max(
    minV,
    Math.min(
      maxV,
      n
    )
  );
}

function pickKeys(
  obj,
  keys
) {
  const out = {};

  for (
    const key of keys
  ) {
    if (
      obj[key]
    ) {
      out[key] =
        obj[key];
    }
  }

  return out;
}

function unique(arr) {
  return [
    ...new Set(arr)
  ];
}

function uniqueNumbers(arr) {
  const seen =
    new Set();

  const out = [];

  for (
    const x of arr
  ) {
    if (
      !Number.isFinite(x)
    ) {
      continue;
    }

    const key =
      String(x);

    if (
      !seen.has(key)
    ) {
      seen.add(key);
      out.push(x);
    }
  }

  return out;
}

function num(v) {
  const n = Number(v);

  return Number.isFinite(n)
    ? n
    : 0;
}

function max(arr) {
  return Math.max(
    ...arr.filter(
      Number.isFinite
    )
  );
}

function min(arr) {
  return Math.min(
    ...arr.filter(
      Number.isFinite
    )
  );
}

function round6(v) {
  return Math.round(
    v * 1e6
  ) / 1e6;
}

function sleep(ms) {
  return new Promise(
    resolve =>
      setTimeout(
        resolve,
        ms
      )
  );
}

function encodePath(path) {
  return path
    .split("/")
    .map(
      encodeURIComponent
    )
    .join("/");
}

function utf8ToBase64(str) {
  const bytes =
    new TextEncoder()
      .encode(str);

  let binary = "";

  const CHUNK =
    0x8000;

  for (
    let i = 0;
    i < bytes.length;
    i += CHUNK
  ) {
    binary +=
      String.fromCharCode(
        ...bytes.subarray(
          i,
          i + CHUNK
        )
      );
  }

  return btoa(binary);
}

function base64ToUtf8(b64) {
  const binary =
    atob(
      String(b64)
        .replace(/\s/g, "")
    );

  const bytes =
    Uint8Array.from(
      binary,
      c =>
        c.charCodeAt(0)
    );

  return new TextDecoder()
    .decode(bytes);
}

function jsonResponse(
  obj,
  status = 200
) {
  return new Response(
    JSON.stringify(
      obj,
      null,
      2
    ),
    {
      status,

      headers: {
        "content-type":
          "application/json; charset=utf-8",

        "cache-control":
          "no-store"
      }
    }
  );
}
