const $ = (id) => document.getElementById(id);

const ENV_PRESETS = {
  okx_main_demo: {
    label: "OKX 主站模拟盘",
    baseUrl: "https://www.okx.com",
    simulated: true,
    publicWs: "wss://wspap.okx.com:8443/ws/v5/public",
    privateWs: "wss://wspap.okx.com:8443/ws/v5/private",
    businessWs: "wss://wspap.okx.com:8443/ws/v5/business",
    notice: "当前是主站模拟盘。REST 虽然仍走 okx.com，但会使用模拟盘专用 API Key、x-simulated-trading: 1，以及 wspap 私有/业务 WebSocket。",
  },
  okx_main_live: {
    label: "OKX 主站实盘",
    baseUrl: "https://www.okx.com",
    simulated: false,
    publicWs: "wss://ws.okx.com:8443/ws/v5/public",
    privateWs: "wss://ws.okx.com:8443/ws/v5/private",
    businessWs: "wss://ws.okx.com:8443/ws/v5/business",
    notice: "当前是主站实盘，请确认 API Key 已绑定正确权限和 IP。",
  },
  okx_us_live: {
    label: "OKX US 实盘",
    baseUrl: "https://us.okx.com",
    simulated: false,
    publicWs: "wss://wsus.okx.com:8443/ws/v5/public",
    privateWs: "wss://wsus.okx.com:8443/ws/v5/private",
    businessWs: "wss://wsus.okx.com:8443/ws/v5/business",
    notice: "当前是 US 区域地址，请确认你的账户和 API 权限属于对应区域。",
  },
  custom: {
    label: "自定义",
    baseUrl: "https://www.okx.com",
    simulated: true,
    publicWs: "-",
    privateWs: "-",
    businessWs: "-",
    notice: "当前是自定义环境，可手动改 REST 地址；若挂代理或区域站点，建议同步核对 WebSocket 地址。",
  },
};

const PAIR_PRESETS = {
  btc: {
    label: "BTC 主交易对",
    spot: "BTC-USDT",
    swap: "BTC-USDT-SWAP",
  },
  eth: {
    label: "ETH 主交易对",
    spot: "ETH-USDT",
    swap: "ETH-USDT-SWAP",
  },
  sol: {
    label: "SOL 主交易对",
    spot: "SOL-USDT",
    swap: "SOL-USDT-SWAP",
  },
  doge: {
    label: "DOGE 主交易对",
    spot: "DOGE-USDT",
    swap: "DOGE-USDT-SWAP",
  },
  xrp: {
    label: "XRP 主交易对",
    spot: "XRP-USDT",
    swap: "XRP-USDT-SWAP",
  },
};

const ONLY_STRATEGY_PRESET = "dip_swing";

const STRATEGY_PRESETS = {
  dip_swing: {
    label: "波段",
    description: "市场扫描 + 净优势过滤 + 目标驱动仓位。逐仓 10x 做趋势内波段，回踩或转强就接，固定 8% 止盈，强平缓冲不足时优先主动离场，目标余额 100x。",
    config: {
      strategyPreset: "dip_swing",
      bar: "15m",
      fastEma: 12,
      slowEma: 48,
      pollSeconds: 8,
      cooldownSeconds: 45,
      maxOrdersPerDay: 0,
      spotEnabled: false,
      spotQuoteBudget: "0",
      spotMaxExposure: "0",
      swapEnabled: true,
      swapContracts: "1",
      swapTdMode: "isolated",
      swapStrategyMode: "long_only",
      swapLeverage: "10",
      stopLossPct: "1.2",
      takeProfitPct: "8",
      maxDailyLossPct: "0.8",
      targetBalanceMultiple: "100",
      autostart: false,
      allowLiveTrading: false,
      allowLiveAutostart: false,
      enforceNetMode: true,
    },
  },
};

const MINER_MODE_META = {
  mac_lotto: {
    label: "Mac 本机乐透机",
    summary: "直接用这台 Mac 的 CPU 跑 BTC lottery miner，核心就是本机算力、矿池状态和理论收益判断。",
  },
  nerdminer_v2: {
    label: "NerdMiner v2",
    summary: "ESP32 Stratum 乐透矿机，适合桌面学习和低难度 share 池。",
  },
  leafminer: {
    label: "LeafMiner",
    summary: "更轻量的 ESP32 / ESP8266 乐透矿机，适合快速网页刷机。",
  },
  esp_miner: {
    label: "Bitaxe / ESP-Miner",
    summary: "ESP-Miner + AxeOS 真矿机路线，适合直接监控和控制 Bitaxe。",
  },
};

const WORKSPACE_VIEWS = {
  focus: {
    label: "聚焦工作台",
    chip: "Focus",
    description: "默认只保留驾驶舱、研究入口和当前状态。",
  },
  research: {
    label: "策略研究",
    chip: "Research",
    description: "集中看参数、回测、自动优化和策略日志。",
  },
  trade: {
    label: "交易配置",
    chip: "Trade",
    description: "环境配置、账户快照、风控与策略参数。",
  },
  orders: {
    label: "订单终端",
    chip: "Orders",
    description: "多币下单、最近订单、收益判断和成交细节。",
  },
  market: {
    label: "行情监控",
    chip: "Market",
    description: "主流币价格、K 线和行情连通状态。",
  },
  miner: {
    label: "矿机看板",
    chip: "Miner",
    description: "只看这台 Mac 的本机挖矿控制、进度和收益解释。",
  },
};

let automationPollTimer = null;
let liveFeedRestartTimer = null;
let snapshotPollTimer = null;
let minerPollTimer = null;
let orderPollTimer = null;
let autoAnalysisDebounceTimer = null;
let autoAnalysisInFlight = false;
let autoAnalysisLastAttemptAt = 0;
let tunnelBgState = null;
const requestFlightMap = new Map();

const AUTO_ANALYSIS_INTERVAL_MS = 45000;
const AUTO_ANALYSIS_DEBOUNCE_MS = 1200;
const LOCAL_REQUEST_TIMEOUT_MS = 12000;

const MAINSTREAM_MARKETS = [
  "BTC-USDT",
  "ETH-USDT",
  "SOL-USDT",
  "DOGE-USDT",
  "XRP-USDT",
];

const liveMarketState = {
  tickers: {},
  candles: [],
  tickerSocket: null,
  candleSocket: null,
  tickerPing: null,
  candlePing: null,
  quotePoll: null,
  candlePoll: null,
  tickerReady: false,
  candleReady: false,
};

const dashboardState = {
  account: null,
  automation: null,
  savedAutomationConfig: null,
  strategyApplyState: null,
  miner: null,
  research: null,
  routeHealth: null,
  config: null,
  configSaving: false,
  configTesting: false,
  currentView: "focus",
  recentOrders: [],
  recentOrdersAll: [],
  orderFeedMeta: null,
  orderJournal: null,
  orderJournalSymbols: [],
  selectedOrderSymbol: "",
  orderStateFilter: "all",
  orderMarketFilter: "all",
  orderExpandedSymbols: {},
  selectedOrderKey: null,
  accountDetailsLoadedOnce: false,
  accountSummaryLoadedOnce: false,
  ordersLoadedOnce: false,
};

function formatSignedMoney(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) return "--";
  const sign = num > 0 ? "+" : "";
  return `${sign}${formatMoney(num)}`;
}

function formatStrategyMode(mode) {
  if (mode === "short_only") return "只做空";
  if (mode === "trend_follow") return "顺势双向";
  return "只做多";
}

const WATCHLIST_OVERRIDE_EDITOR_FIELDS = [
  {
    key: "bar",
    label: "周期",
    kind: "select",
    options: [
      ["", "继承组合"],
      ["1m", "1m"],
      ["3m", "3m"],
      ["5m", "5m"],
      ["15m", "15m"],
      ["1H", "1H"],
      ["4H", "4H"],
    ],
  },
  { key: "fastEma", label: "快 EMA", kind: "int", min: 1, step: 1 },
  { key: "slowEma", label: "慢 EMA", kind: "int", min: 2, step: 1 },
  { key: "pollSeconds", label: "轮询秒数", kind: "int", min: 1, step: 1 },
  { key: "cooldownSeconds", label: "冷却秒数", kind: "int", min: 0, step: 1 },
  { key: "maxOrdersPerDay", label: "每日订单上限", kind: "int", min: 0, step: 1 },
  { key: "swapContracts", label: "永续张数", kind: "string-number", min: 0, step: 1 },
  { key: "swapLeverage", label: "永续杠杆", kind: "string-number", min: 1, max: 10, step: 1 },
  { key: "stopLossPct", label: "止损 (%)", kind: "string-number", min: 0, step: 0.1 },
  { key: "takeProfitPct", label: "止盈 (%)", kind: "string-number", min: 0, step: 0.1 },
  { key: "maxDailyLossPct", label: "日内最大回撤 (%)", kind: "string-number", min: 0, step: 0.1 },
];

const WATCHLIST_OVERRIDE_EDITOR_FIELD_MAP = Object.fromEntries(
  WATCHLIST_OVERRIDE_EDITOR_FIELDS.map((field) => [field.key, field])
);

function normalizeWatchlistOverrideSymbol(raw) {
  let value = String(raw || "").trim().toUpperCase();
  if (value.includes("-")) value = value.split("-")[0];
  return value.replace(/[^A-Z0-9]/g, "");
}

function parseWatchlistOverridesValue(raw, watchlist = []) {
  if (!raw || (typeof raw === "string" && !raw.trim())) return {};
  let parsed = raw;
  if (typeof raw === "string") {
    try {
      parsed = JSON.parse(raw);
    } catch (_) {
      return {};
    }
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return {};
  const watchlistSet = new Set((watchlist || []).map((symbol) => normalizeWatchlistOverrideSymbol(symbol)).filter(Boolean));
  const normalized = {};
  Object.entries(parsed).forEach(([symbolKey, override]) => {
    const symbol = normalizeWatchlistOverrideSymbol(symbolKey);
    if (!symbol || !override || typeof override !== "object" || Array.isArray(override)) return;
    if (watchlistSet.size && !watchlistSet.has(symbol)) return;
    const next = {};
    Object.entries(override).forEach(([field, value]) => {
      if (["fastEma", "slowEma", "pollSeconds", "cooldownSeconds", "maxOrdersPerDay"].includes(field)) {
        const num = Number(value);
        if (Number.isFinite(num)) next[field] = Math.trunc(num);
      } else if (["spotEnabled", "swapEnabled"].includes(field)) {
        next[field] = Boolean(value);
      } else if (
        [
          "bar",
          "spotQuoteBudget",
          "spotMaxExposure",
          "swapContracts",
          "swapTdMode",
          "swapStrategyMode",
          "swapLeverage",
          "stopLossPct",
          "takeProfitPct",
          "maxDailyLossPct",
        ].includes(field)
      ) {
        next[field] = String(value ?? "").trim();
      }
    });
    if (Object.keys(next).length) normalized[symbol] = next;
  });
  return normalized;
}

function serializeWatchlistOverrides(raw, watchlist = []) {
  const normalized = parseWatchlistOverridesValue(raw, watchlist);
  const ordered = {};
  Object.keys(normalized).sort().forEach((symbol) => {
    ordered[symbol] = normalized[symbol];
  });
  return Object.keys(ordered).length ? JSON.stringify(ordered, null, 2) : "";
}

function getWatchlistOverrideParseError(raw) {
  const text = String(raw || "").trim();
  if (!text) return "";
  try {
    const parsed = JSON.parse(text);
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      return "按币覆盖参数必须是 JSON 对象，例如 {\"BTC\": {\"fastEma\": 12}}";
    }
    return "";
  } catch (error) {
    return `按币覆盖参数不是合法 JSON: ${error.message}`;
  }
}

function collectWatchlistOverridesFromEditor() {
  const editor = $("watchlistOverrideEditor");
  const watchlist = parseWatchlistSymbols(
    $("autoWatchlistSymbols")?.value,
    $("spotInstId")?.value,
    $("swapInstId")?.value
  );
  if (!editor) {
    return parseWatchlistOverridesValue($("autoWatchlistOverrides")?.value, watchlist);
  }
  const overrides = {};
  editor.querySelectorAll("[data-watchlist-symbol]").forEach((card) => {
    const symbol = normalizeWatchlistOverrideSymbol(card.dataset.watchlistSymbol || "");
    if (!symbol) return;
    const next = {};
    card.querySelectorAll("[data-override-field]").forEach((input) => {
      const field = WATCHLIST_OVERRIDE_EDITOR_FIELD_MAP[input.dataset.overrideField];
      if (!field) return;
      const rawValue = String(input.value ?? "").trim();
      if (!rawValue) return;
      if (field.kind === "int") {
        const num = Number(rawValue);
        if (Number.isFinite(num)) next[field.key] = Math.trunc(num);
        return;
      }
      if (field.kind === "bool-select") {
        next[field.key] = rawValue === "true";
        return;
      }
      next[field.key] = rawValue;
    });
    if (Object.keys(next).length) overrides[symbol] = next;
  });
  return parseWatchlistOverridesValue(overrides, watchlist);
}

function syncWatchlistOverridesValueFromEditor() {
  const watchlist = parseWatchlistSymbols(
    $("autoWatchlistSymbols")?.value,
    $("spotInstId")?.value,
    $("swapInstId")?.value
  );
  const textarea = $("autoWatchlistOverrides");
  if (!textarea) return {};
  const overrides = collectWatchlistOverridesFromEditor();
  textarea.value = serializeWatchlistOverrides(overrides, watchlist);
  updateWatchlistOverrideEditorState(overrides);
  return overrides;
}

function updateWatchlistOverrideEditorState(overrides = collectWatchlistOverridesFromEditor()) {
  const editor = $("watchlistOverrideEditor");
  if (!editor) return;
  editor.querySelectorAll("[data-watchlist-symbol]").forEach((card) => {
    const symbol = normalizeWatchlistOverrideSymbol(card.dataset.watchlistSymbol || "");
    const activeKeys = Object.keys(overrides[symbol] || {});
    card.classList.toggle("is-active", activeKeys.length > 0);
    const badge = card.querySelector("[data-override-badge]");
    if (badge) {
      badge.textContent = activeKeys.length ? `${activeKeys.length} 项独立参数` : "继承组合";
    }
    const note = card.querySelector("[data-override-note]");
    if (note) {
      note.textContent = activeKeys.length
        ? `当前币已单独覆盖：${activeKeys.map((key) => WATCHLIST_OVERRIDE_EDITOR_FIELD_MAP[key]?.label || key).join("、")}`
        : "留空即继承组合总参数，不需要自己手写 JSON。";
    }
  });
}

function buildWatchlistOverrideCard(symbol, override = {}) {
  const fields = WATCHLIST_OVERRIDE_EDITOR_FIELDS.map((field) => {
    if (field.kind === "select" || field.kind === "bool-select") {
      const options = field.options
        .map(([value, label]) => `<option value="${escapeHtml(value)}"${String(override[field.key] ?? "") === String(value) ? " selected" : ""}>${escapeHtml(label)}</option>`)
        .join("");
      return `
        <label>
          <span>${field.label}</span>
          <select data-override-field="${field.key}">${options}</select>
        </label>
      `;
    }
    const value = override[field.key] ?? "";
    const maxAttr = field.max !== undefined ? ` max="${field.max}"` : "";
    return `
      <label>
        <span>${field.label}</span>
        <input
          data-override-field="${field.key}"
          type="number"
          value="${escapeHtml(value)}"
          min="${field.min ?? 0}"
          step="${field.step ?? 1}"${maxAttr}
          placeholder="继承组合"
        />
      </label>
    `;
  }).join("");
  return `
    <article class="watchlist-override-card" data-watchlist-symbol="${escapeHtml(symbol)}">
      <div class="watchlist-override-card-head">
        <div class="watchlist-override-card-title">
          <strong>${escapeHtml(symbol)}</strong>
          <small>${escapeHtml(symbol)}-USDT / ${escapeHtml(symbol)}-USDT-SWAP</small>
        </div>
        <span class="watchlist-override-badge" data-override-badge>继承组合</span>
      </div>
      <div class="watchlist-override-grid">${fields}</div>
      <div class="watchlist-override-note" data-override-note>留空即继承组合总参数，不需要自己手写 JSON。</div>
    </article>
  `;
}

function renderWatchlistOverrideEditor(rawOverrides = $("autoWatchlistOverrides")?.value || "") {
  const editor = $("watchlistOverrideEditor");
  const textarea = $("autoWatchlistOverrides");
  if (!editor || !textarea) return;
  const watchlist = parseWatchlistSymbols(
    $("autoWatchlistSymbols")?.value,
    $("spotInstId")?.value,
    $("swapInstId")?.value
  );
  const overrides = parseWatchlistOverridesValue(rawOverrides, watchlist);
  textarea.value = serializeWatchlistOverrides(overrides, watchlist);
  if (!watchlist.length) {
    editor.innerHTML = '<div class="watchlist-override-empty">先填多币 watchlist，例如 BTC,ETH,SOL。每个币的独立参数会直接在这里展开。</div>';
    return;
  }
  editor.innerHTML = watchlist.map((symbol) => buildWatchlistOverrideCard(symbol, overrides[symbol] || {})).join("");
  updateWatchlistOverrideEditorState(overrides);
}

function allocateWatchlistNumericField(symbols, overrides, field, total) {
  const normalizedSymbols = (symbols || []).map((symbol) => normalizeWatchlistOverrideSymbol(symbol)).filter(Boolean);
  const totalValue = Number(total || 0);
  const explicit = {};
  normalizedSymbols.forEach((symbol) => {
    if (overrides[symbol] && overrides[symbol][field] !== undefined && overrides[symbol][field] !== "") {
      const value = Number(overrides[symbol][field]);
      if (Number.isFinite(value)) explicit[symbol] = value;
    }
  });
  const reserved = Object.values(explicit).reduce((sum, value) => sum + Number(value || 0), 0);
  const remainingSymbols = normalizedSymbols.filter((symbol) => explicit[symbol] === undefined);
  const remaining = Math.max(totalValue - reserved, 0);
  const perSymbol = remainingSymbols.length ? remaining / remainingSymbols.length : 0;
  const allocations = {};
  normalizedSymbols.forEach((symbol) => {
    allocations[symbol] = explicit[symbol] !== undefined ? explicit[symbol] : perSymbol;
  });
  return allocations;
}

function buildDraftExecutionTargets(config = collectAutomationConfig()) {
  const normalized = normalizeAutomationConfigForCompare(config);
  const watchlist = normalized.watchlistSymbols.split(",").filter(Boolean);
  const overrides = parseWatchlistOverridesValue(normalized.watchlistOverrides, watchlist);
  const spotBudgetAllocations = allocateWatchlistNumericField(watchlist, overrides, "spotQuoteBudget", normalized.spotQuoteBudget);
  const spotCapAllocations = allocateWatchlistNumericField(watchlist, overrides, "spotMaxExposure", normalized.spotMaxExposure);
  const swapContractAllocations = allocateWatchlistNumericField(watchlist, overrides, "swapContracts", normalized.swapContracts);
  return watchlist.map((symbol, index) => {
    const override = overrides[symbol] || {};
    const target = {
      ...normalized,
      watchlistSymbols: watchlist.join(","),
      watchlistSymbol: symbol,
      watchlistIndex: index,
      watchlistCount: watchlist.length,
      spotInstId: `${symbol}-USDT`,
      swapInstId: `${symbol}-USDT-SWAP`,
      watchlistOverride: override,
    };
    if (target.spotEnabled) {
      target.spotQuoteBudget = String(spotBudgetAllocations[symbol] ?? 0);
      target.spotMaxExposure = String(spotCapAllocations[symbol] ?? 0);
    }
    if (target.swapEnabled) {
      target.swapContracts = String(swapContractAllocations[symbol] ?? 0);
    }
    return { ...target, ...override, watchlistOverride: override };
  });
}

function normalizeAutomationConfigForCompare(config = {}) {
  const spotInstId = String(config.spotInstId || "BTC-USDT").trim().toUpperCase();
  const swapInstId = String(config.swapInstId || "BTC-USDT-SWAP").trim().toUpperCase();
  const watchlistSymbols = parseWatchlistSymbols(config.watchlistSymbols, spotInstId, swapInstId).join(",");
  const watchlist = watchlistSymbols.split(",").filter(Boolean);
  return {
    strategyPreset: ONLY_STRATEGY_PRESET,
    spotInstId,
    swapInstId,
    watchlistSymbols,
    watchlistOverrides: serializeWatchlistOverrides(config.watchlistOverrides, watchlist),
    bar: String(config.bar || "5m"),
    fastEma: Number(config.fastEma ?? 9),
    slowEma: Number(config.slowEma ?? 21),
    pollSeconds: Number(config.pollSeconds ?? 20),
    cooldownSeconds: Number(config.cooldownSeconds ?? 180),
    maxOrdersPerDay: Number(config.maxOrdersPerDay ?? 20),
    spotEnabled: Boolean(config.spotEnabled),
    spotQuoteBudget: String(config.spotQuoteBudget ?? "100"),
    spotMaxExposure: String(config.spotMaxExposure ?? "300"),
    swapEnabled: Boolean(config.swapEnabled),
    swapContracts: String(config.swapContracts ?? "1"),
    swapTdMode: String(config.swapTdMode || "cross"),
    swapStrategyMode: String(config.swapStrategyMode || "trend_follow"),
    swapLeverage: String(config.swapLeverage ?? "5"),
    stopLossPct: String(config.stopLossPct ?? "1.2"),
    takeProfitPct: String(config.takeProfitPct ?? "2.4"),
    maxDailyLossPct: String(config.maxDailyLossPct ?? "3.0"),
    targetBalanceMultiple: String(config.targetBalanceMultiple ?? "1"),
    arbEntrySpreadPct: String(config.arbEntrySpreadPct ?? "0.18"),
    arbExitSpreadPct: String(config.arbExitSpreadPct ?? "0.05"),
    arbMinFundingRatePct: String(config.arbMinFundingRatePct ?? "0.005"),
    arbMaxHoldMinutes: Number(config.arbMaxHoldMinutes ?? 180),
    arbRequireFundingAlignment: config.arbRequireFundingAlignment !== false,
    autostart: Boolean(config.autostart),
    allowLiveManualOrders: Boolean(config.allowLiveManualOrders),
    allowLiveTrading: Boolean(config.allowLiveTrading),
    allowLiveAutostart: Boolean(config.allowLiveAutostart),
    enforceNetMode: config.enforceNetMode !== false,
  };
}

function isAutomationConfigDirty() {
  if (!dashboardState.savedAutomationConfig) return false;
  const draft = normalizeAutomationConfigForCompare(collectAutomationConfig());
  return JSON.stringify(draft) !== JSON.stringify(dashboardState.savedAutomationConfig);
}

function buildStrategyFormSummary(config = {}) {
  const normalized = normalizeAutomationConfigForCompare(config);
  const preset = STRATEGY_PRESETS[ONLY_STRATEGY_PRESET];
  const watchlist = normalized.watchlistSymbols
    ? normalized.watchlistSymbols.split(",").filter(Boolean)
    : ["BTC"];
  const overrideCount = Object.keys(parseWatchlistOverridesValue(normalized.watchlistOverrides, watchlist)).length;
  const presetDetail = `${normalized.bar} 波段 · 逐仓 ${normalized.swapLeverage}x · TP ${normalized.takeProfitPct}%`;
  return `${preset.label} · ${presetDetail} · ${watchlist.join(" / ")} · ${watchlist.length} 币组合${overrideCount ? ` · ${overrideCount} 币独立覆盖` : ""}`;
}

function setStrategyApplyState(stage, title, detail = "") {
  dashboardState.strategyApplyState = {
    stage,
    title,
    detail,
    appliedAt: new Date().toISOString(),
  };
  renderStrategyPortfolio();
}

function buildDraftPortfolioEntries(config = collectAutomationConfig()) {
  return buildDraftExecutionTargets(config).map((target) => {
    const symbol = target.watchlistSymbol || target.spotInstId.split("-")[0];
    const perSpotBudget = Number(target.spotQuoteBudget || 0);
    const perSpotCap = Number(target.spotMaxExposure || 0);
    const perSwapContracts = Number(target.swapContracts || 0);
    const overrideFields = Object.keys(target.watchlistOverride || {});
    const summaryDetail = [
      `${target.bar} · 趋势波段`,
      `逐仓 ${target.swapLeverage}x · SL ${target.stopLossPct}% / TP ${target.takeProfitPct}%`,
      overrideFields.length ? `独立覆盖 ${overrideFields.length} 项` : "沿用组合参数",
    ].join(" · ");
    return {
      symbol,
      allocation: {
      strategyPreset: target.strategyPreset || ONLY_STRATEGY_PRESET,
      spotBudget: String(target.spotQuoteBudget || "0"),
      spotMaxExposure: String(target.spotMaxExposure || "0"),
      swapContracts: String(target.swapContracts || "0"),
      swapLeverage: String(target.swapLeverage || "0"),
      swapTdMode: target.swapTdMode || "cross",
      swapStrategyMode: target.swapStrategyMode || "trend_follow",
      overrideKeys: overrideFields,
    },
    overrideActive: overrideFields.length > 0,
    spot: {
      enabled: target.spotEnabled,
      instId: `${symbol}-USDT`,
      signal: "hold",
      trend: "flat",
      positionSide: "flat",
      positionSize: "0",
      positionNotional: "0",
      lastMessage: target.spotEnabled ? "保存并启动后开始独立现货决策" : "现货策略未启用",
      floatingPnl: "0",
      floatingPnlPct: "0",
      riskLabel: `单次 ${formatMoney(perSpotBudget)}U · 上限 ${formatMoney(perSpotCap)}U`,
    },
    swap: {
      enabled: target.swapEnabled,
      instId: `${symbol}-USDT-SWAP`,
      signal: "hold",
      trend: "flat",
      positionSide: "flat",
      positionSize: "0",
      positionNotional: "0",
      lastMessage: target.swapEnabled ? "保存并启动后开始独立永续决策" : "永续策略未启用",
      floatingPnl: "0",
      floatingPnlPct: "0",
      riskLabel: `${formatMoney(perSwapContracts)} 张 · ${target.swapLeverage}x · ${target.swapTdMode === "isolated" ? "逐仓" : "全仓"}`,
    },
    summary: {
      status: "待启动",
      detail: summaryDetail,
      exposureTotal: "0",
      floatingPnl: "0",
      floatingPnlPct: "0",
      riskLabel: `现货 ${formatMoney(perSpotBudget)}U / ${formatMoney(perSpotCap)}U · 永续 ${formatMoney(perSwapContracts)} 张 · ${formatStrategyMode(target.swapStrategyMode)}`,
    },
  };
  });
}

function inferEnvPreset(envPreset, baseUrl, simulated) {
  const normalizedBase = (baseUrl || "").trim() || ENV_PRESETS.custom.baseUrl;
  const normalizedSimulated = Boolean(simulated);
  const preset = ENV_PRESETS[envPreset];
  if (
    preset &&
    (envPreset === "custom" ||
      (preset.baseUrl === normalizedBase && Boolean(preset.simulated) === normalizedSimulated))
  ) {
    return envPreset;
  }
  if (normalizedBase === ENV_PRESETS.okx_main_live.baseUrl) {
    return normalizedSimulated ? "okx_main_demo" : "okx_main_live";
  }
  if (normalizedBase === ENV_PRESETS.okx_us_live.baseUrl && !normalizedSimulated) {
    return "okx_us_live";
  }
  return "custom";
}

function syncEnvironmentUi({ envPreset, baseUrl, simulated }, { preserveBaseUrl = false } = {}) {
  const effectivePreset = inferEnvPreset(envPreset, baseUrl, simulated);
  const preset = ENV_PRESETS[effectivePreset] || ENV_PRESETS.custom;
  const resolvedBaseUrl = (baseUrl || "").trim() || preset.baseUrl;

  $("envPreset").value = effectivePreset;
  $("simulated").value = String(Boolean(simulated));
  $("simulated").disabled = effectivePreset !== "custom";
  $("baseUrl").readOnly = effectivePreset !== "custom";

  if (!preserveBaseUrl || !$("baseUrl").value.trim()) {
    $("baseUrl").value = resolvedBaseUrl;
  } else if (resolvedBaseUrl) {
    $("baseUrl").value = resolvedBaseUrl;
  }

  updateEndpointCards(effectivePreset);
  updateQuickState();
  renderDeskGuards();
}

function setPendingEnvironmentUi() {
  $("active-env-label").textContent = "正在同步环境";
  if ($("active-env-state")) $("active-env-state").textContent = "等待读取";
  $("active-pair-label").textContent = "正在同步组合";
  if ($("order-env-label")) $("order-env-label").textContent = "正在同步环境";
  if ($("order-spot-label")) $("order-spot-label").textContent = "等待标的";
  if ($("order-swap-label")) $("order-swap-label").textContent = "等待标的";
  if ($("order-watchlist-label")) $("order-watchlist-label").textContent = "等待 watchlist";
  if ($("strategy-application-status")) $("strategy-application-status").textContent = "等待应用策略";
  if ($("strategy-application-detail")) $("strategy-application-detail").textContent = "正在同步当前组合参数和执行状态";
  if ($("portfolio-context-main")) $("portfolio-context-main").textContent = "等待 watchlist";
  if ($("portfolio-context-sub")) $("portfolio-context-sub").textContent = "正在同步每个币的独立决策、风控和仓位摘要";
  if ($("rail-strategy-apply")) $("rail-strategy-apply").textContent = "等待应用";
  if ($("rail-strategy-pnl")) $("rail-strategy-pnl").textContent = "--";
  if ($("rail-strategy-status-sub")) $("rail-strategy-status-sub").textContent = "正在同步当前环境与策略状态";
  document.querySelectorAll("[data-env-preset]").forEach((button) => {
    button.classList.remove("active");
  });
}

function isSimulatedMode() {
  return $("simulated")?.value === "true";
}

function isRemoteExecutionMode() {
  return $("executionMode")?.value === "remote";
}

const RAIL_AUTOMATION_TOGGLES = [
  ["autoAutostart", "railAutoAutostart"],
  ["autoAllowLiveManualOrders", "railAutoAllowLiveManualOrders"],
  ["autoAllowLiveTrading", "railAutoAllowLiveTrading"],
  ["autoAllowLiveAutostart", "railAutoAllowLiveAutostart"],
];

function syncRailAutomationToggles() {
  RAIL_AUTOMATION_TOGGLES.forEach(([formId, railId]) => {
    const form = $(formId);
    const rail = $(railId);
    if (!form || !rail) return;
    rail.checked = Boolean(form.checked);
  });
}

function syncRailStrategyButtons() {
  const running = Boolean(dashboardState.automation?.running);
  const blocked = isLiveRouteBlocked();
  const start = $("rail-start-automation");
  const runOnce = $("rail-run-automation-once");
  const stop = $("rail-stop-automation");
  const save = $("rail-save-automation");
  const toggle = $("rail-strategy-toggle");

  if (save) {
    save.disabled = Boolean(dashboardState.configSaving || dashboardState.configTesting);
  }
  if (start) {
    start.disabled = running || blocked;
    start.title = blocked ? (dashboardState.routeHealth?.summary || "当前实盘链路不可用") : "";
  }
  if (runOnce) {
    runOnce.disabled = running || blocked;
    runOnce.title = blocked ? (dashboardState.routeHealth?.summary || "当前实盘链路不可用") : "";
  }
  if (stop) {
    stop.disabled = !running;
    stop.title = running ? "" : "当前策略未运行";
  }
  if (toggle) {
    toggle.disabled = !running && blocked;
    toggle.title = !running && blocked ? (dashboardState.routeHealth?.summary || "当前实盘链路不可用") : "";
    toggle.textContent = running ? "停止" : "启动";
    toggle.className = running ? "btn rail-strategy-toggle-stop" : "btn btn-primary";
  }
}

function renderRailStrategyControls() {
  const automation = dashboardState.automation || {};
  const envPreset = inferEnvPreset(
    $("envPreset")?.value || "custom",
    $("baseUrl")?.value || "",
    $("simulated")?.value === "true"
  );
  const env = ENV_PRESETS[envPreset] || ENV_PRESETS.custom;
  const watchlist = parseWatchlistSymbols(
    $("autoWatchlistSymbols")?.value,
    $("spotInstId")?.value,
    $("swapInstId")?.value
  );
  const simulated = isSimulatedMode();
  const running = Boolean(automation.running);
  const allowLiveTrading = Boolean($("autoAllowLiveTrading")?.checked);
  const allowLiveManualOrders = Boolean($("autoAllowLiveManualOrders")?.checked);
  const allowLiveAutostart = Boolean($("autoAllowLiveAutostart")?.checked);
  const state = $("rail-strategy-state");
  const pill = $("rail-strategy-pill");
  const meta = $("rail-strategy-meta");
  const statusSub = $("rail-strategy-status-sub");
  const watchlistLabel = $("rail-strategy-watchlist");
  const guard = $("rail-strategy-guard");
  const summaryText = running
    ? `${env.label} · 自动量化运行中`
    : (automation?.stopReason || automation?.lastError || `${env.label} · 当前待机`);

  if (state) {
    state.textContent = running ? "策略运行中" : "策略待机";
  }
  if (statusSub) {
    statusSub.textContent = summaryText;
  }
  if (meta) {
    meta.textContent = `${$("active-strategy-label")?.textContent || "策略未设置"} · ${env.label}`;
  }
  if (watchlistLabel) {
    watchlistLabel.textContent = watchlist.length > 1
      ? `${watchlist.join(" / ")} · ${watchlist.length} 币并行`
      : (watchlist[0] || "BTC");
  }
  if (guard) {
    guard.textContent = simulated
      ? "模拟盘已锁定真实权限"
      : [
          allowLiveManualOrders ? "手动实盘已开" : "手动实盘锁定",
          allowLiveTrading ? "自动实盘已开" : "自动实盘锁定",
          allowLiveAutostart ? "实盘自启已开" : "实盘自启锁定",
        ].join(" · ");
  }
  if (pill) {
    if (simulated) {
      pill.textContent = running ? "模拟运行" : "模拟待机";
      pill.style.color = "var(--accent-2)";
      pill.style.borderColor = "rgba(69, 214, 196, 0.24)";
    } else if (allowLiveTrading) {
      pill.textContent = running ? "实盘运行" : "实盘待机";
      pill.style.color = "var(--success)";
      pill.style.borderColor = "rgba(105, 240, 174, 0.22)";
    } else {
      pill.textContent = "实盘锁定";
      pill.style.color = "var(--accent)";
      pill.style.borderColor = "rgba(255, 184, 77, 0.24)";
    }
  }
  syncRailStrategyButtons();
}

function isLiveRouteBlocked() {
  if (isSimulatedMode()) return false;
  return dashboardState.routeHealth?.healthy === false;
}

function applyRouteHealth(route, { preserveMessage = false } = {}) {
  dashboardState.routeHealth = route || null;
  if (route && (Object.prototype.hasOwnProperty.call(route, "simulated") || route.baseUrl)) {
    syncEnvironmentUi(
      {
        envPreset: $("envPreset")?.value || "custom",
        baseUrl: route.baseUrl || $("baseUrl")?.value || ENV_PRESETS.custom.baseUrl,
        simulated: Object.prototype.hasOwnProperty.call(route, "simulated")
          ? route.simulated
          : isSimulatedMode(),
      },
      { preserveBaseUrl: false }
    );
  }
  const simulated = isSimulatedMode();
  const remote = isRemoteExecutionMode() || route?.executionMode === "remote";
  const blocked = !simulated && route && route.healthy === false;
  const manualLiveLocked = !simulated && !$("autoAllowLiveManualOrders").checked;
  const detail = route?.detail || "";
  const rawSummary = route?.summary || detail || "";
  const summary = remote
    ? (rawSummary ? `远端执行节点 · ${rawSummary}` : "远端执行节点已连接")
    : rawSummary;
  const technicalDetail = route?.technicalDetail || detail || "";

  if (!preserveMessage) {
    if (route?.healthy) {
      $("health-dot").style.background = "var(--success)";
      $("health-text").textContent = summary || "连接成功";
    } else if (blocked) {
      $("health-dot").style.background = "var(--danger)";
      $("health-text").textContent = summary || "实盘链路不可用";
    } else if (summary) {
      $("health-dot").style.background = "var(--accent)";
      $("health-text").textContent = summary;
    }
  }
  $("health-text").title = technicalDetail;

  const disabled = blocked || manualLiveLocked;
  const disabledTitle = blocked
    ? (summary || "当前实盘链路不可用")
    : "当前是实盘，未开启“允许手动实盘下单”";
  [
    $("start-automation"),
    $("run-automation-once"),
    $("cockpit-run-once"),
    $("rail-start-automation"),
    $("rail-run-automation-once"),
    $("spot-order-form")?.querySelector('button[type="submit"]'),
    $("swap-order-form")?.querySelector('button[type="submit"]'),
  ].forEach((node) => {
    if (!node) return;
    node.disabled = disabled;
    node.title = disabled ? disabledTitle : "";
  });
  syncRailStrategyButtons();
}

function setupTunnelBackground() {
  if (tunnelBgState) {
    tunnelBgState.cleanup();
    tunnelBgState = null;
  }
  const canvas = $("tunnel-bg");
  if (!canvas) return;
  const context = canvas.getContext("2d");
  if (!context) return;

  const reducedMotion = window.matchMedia?.("(prefers-reduced-motion: reduce)")?.matches ?? false;
  const state = {
    canvas,
    context,
    stars: [],
    dust: [],
    frameId: 0,
    width: 0,
    height: 0,
    dpr: 1,
    lastTime: 0,
    reducedMotion,
  };

  const spawnStar = () => ({
    x: Math.random() * state.width,
    y: Math.random() * state.height,
    size: 0.35 + Math.random() * 1.2,
    alpha: 0.12 + Math.random() * 0.35,
    twinklePhase: Math.random() * Math.PI * 2,
    twinkleSpeed: 0.18 + Math.random() * 0.55,
    cool: Math.random() > 0.72,
  });

  const spawnDust = () => {
    const x = Math.random() * state.width;
    return {
      x,
      y: Math.random() * state.height,
      driftX: -(state.reducedMotion ? 0.6 : 1.2) - Math.random() * (state.reducedMotion ? 1.2 : 2.4),
      driftY: (Math.random() - 0.5) * 0.08,
      alpha: 0.03 + Math.random() * 0.08,
      size: 0.4 + Math.random() * 1.6,
      hue: Math.random() > 0.8 ? 34 : Math.random() > 0.45 ? 198 : 214,
      twinklePhase: Math.random() * Math.PI * 2,
      twinkleSpeed: 0.08 + Math.random() * 0.24,
    };
  };

  const planetAnchor = () => ({
    x: state.width * 0.76,
    y: state.height * 0.5,
    radius: Math.min(state.width, state.height) * (state.reducedMotion ? 0.2 : 0.24),
  });

  const drawBackdrop = () => {
    const sky = context.createLinearGradient(0, 0, 0, state.height);
    sky.addColorStop(0, "rgba(2, 5, 12, 0.98)");
    sky.addColorStop(0.5, "rgba(3, 8, 18, 0.99)");
    sky.addColorStop(1, "rgba(1, 3, 8, 1)");
    context.fillStyle = sky;
    context.fillRect(0, 0, state.width, state.height);

    const haze = context.createRadialGradient(
      state.width * 0.78,
      state.height * 0.52,
      0,
      state.width * 0.78,
      state.height * 0.52,
      Math.max(state.width, state.height) * 0.58
    );
    haze.addColorStop(0, "rgba(77, 52, 24, 0.12)");
    haze.addColorStop(0.45, "rgba(22, 24, 42, 0.08)");
    haze.addColorStop(1, "rgba(1, 3, 8, 0)");
    context.fillStyle = haze;
    context.fillRect(0, 0, state.width, state.height);
  };

  const drawStars = (timeMs) => {
    for (const star of state.stars) {
      const twinkle = 0.74 + Math.sin(timeMs * 0.001 * star.twinkleSpeed + star.twinklePhase) * 0.14;
      context.fillStyle = star.cool
        ? `rgba(146, 182, 255, ${star.alpha * twinkle})`
        : `rgba(242, 234, 218, ${star.alpha * twinkle})`;
      context.beginPath();
      context.arc(star.x, star.y, star.size, 0, Math.PI * 2);
      context.fill();
    }
  };

  const drawDust = (timeMs, delta) => {
    for (const particle of state.dust) {
      particle.x += particle.driftX * delta;
      particle.y += particle.driftY * delta;
      if (particle.x < -16) {
        Object.assign(particle, spawnDust(), { x: state.width + 16 });
      }
      if (particle.y < -8 || particle.y > state.height + 8) {
        particle.y = ((particle.y % state.height) + state.height) % state.height;
      }
      const twinkle = 0.78 + Math.sin(timeMs * 0.001 * particle.twinkleSpeed + particle.twinklePhase) * 0.12;
      context.fillStyle = `hsla(${particle.hue}, 48%, 74%, ${particle.alpha * twinkle})`;
      context.beginPath();
      context.arc(particle.x, particle.y, particle.size, 0, Math.PI * 2);
      context.fill();
    }
  };

  const drawRingLayer = (planet, spin, front) => {
    context.save();
    context.translate(planet.x, planet.y);
    context.rotate(-0.34);
    context.scale(1, 0.26);

    const inner = planet.radius * 1.18;
    const outer = planet.radius * 1.82;
    for (let index = 0; index < 6; index += 1) {
      const t = index / 5;
      const radius = inner + (outer - inner) * t;
      const alphaBase = front ? 0.15 : 0.09;
      const lightness = front ? 72 : 60;
      const hue = 34 + t * 16;
      context.beginPath();
      context.strokeStyle = `hsla(${hue}, 44%, ${lightness}%, ${alphaBase - t * 0.03})`;
      context.lineWidth = Math.max(1.2, planet.radius * 0.016);
      const arcDrift = spin * 0.08 + t * 0.12;
      if (front) {
        context.ellipse(0, 0, radius, radius, 0, 0.16 * Math.PI + arcDrift, 0.84 * Math.PI + arcDrift);
      } else {
        context.ellipse(0, 0, radius, radius, 0, 1.16 * Math.PI + arcDrift, 1.84 * Math.PI + arcDrift);
      }
      context.stroke();
    }
    context.restore();
  };

  const drawPlanet = (planet, spin, timeMs) => {
    const rotation = spin * (state.reducedMotion ? 0.4 : 0.85);
    context.save();
    context.beginPath();
    context.arc(planet.x, planet.y, planet.radius, 0, Math.PI * 2);
    context.clip();

    const fill = context.createRadialGradient(
      planet.x - planet.radius * 0.34,
      planet.y - planet.radius * 0.28,
      planet.radius * 0.12,
      planet.x,
      planet.y,
      planet.radius * 1.1
    );
    fill.addColorStop(0, "rgba(227, 187, 132, 0.98)");
    fill.addColorStop(0.38, "rgba(205, 147, 91, 0.98)");
    fill.addColorStop(0.72, "rgba(134, 88, 56, 0.98)");
    fill.addColorStop(1, "rgba(60, 34, 22, 1)");
    context.fillStyle = fill;
    context.fillRect(planet.x - planet.radius, planet.y - planet.radius, planet.radius * 2, planet.radius * 2);

    const stripeCount = 17;
    for (let index = 0; index < stripeCount; index += 1) {
      const t = index / (stripeCount - 1);
      const y = planet.y - planet.radius + planet.radius * 2 * t;
      const wave = Math.sin(rotation + t * 8.8 + timeMs * 0.00008) * planet.radius * 0.045;
      const bandHeight = planet.radius * (0.09 + (index % 3) * 0.012);
      const hue = 22 + Math.sin(t * 4.8) * 6;
      const sat = 36 + (index % 2) * 8;
      const light = 58 + Math.cos(t * 6.2) * 10;
      context.fillStyle = `hsla(${hue}, ${sat}%, ${light}%, ${0.28 + (index % 2) * 0.08})`;
      context.fillRect(planet.x - planet.radius, y + wave, planet.radius * 2, bandHeight);
    }

    for (let index = 0; index < 9; index += 1) {
      const t = index / 8;
      const y = planet.y - planet.radius * 0.82 + t * planet.radius * 1.64;
      const swirl = Math.sin(rotation * 1.4 + t * 10.5) * planet.radius * 0.08;
      context.beginPath();
      context.strokeStyle = `rgba(255,255,255,${0.03 + index * 0.006})`;
      context.lineWidth = planet.radius * 0.012;
      context.moveTo(planet.x - planet.radius * 1.04, y + swirl);
      context.bezierCurveTo(
        planet.x - planet.radius * 0.42,
        y - planet.radius * 0.08 + swirl,
        planet.x + planet.radius * 0.38,
        y + planet.radius * 0.08 + swirl,
        planet.x + planet.radius * 1.06,
        y - swirl * 0.45
      );
      context.stroke();
    }

    const stormX = planet.x + Math.cos(rotation * 1.1) * planet.radius * 0.18;
    const stormY = planet.y + planet.radius * 0.2;
    context.beginPath();
    context.ellipse(stormX, stormY, planet.radius * 0.22, planet.radius * 0.1, 0.14, 0, Math.PI * 2);
    context.fillStyle = "rgba(177, 87, 54, 0.72)";
    context.fill();
    context.beginPath();
    context.ellipse(stormX + planet.radius * 0.025, stormY - planet.radius * 0.01, planet.radius * 0.12, planet.radius * 0.055, 0.14, 0, Math.PI * 2);
    context.fillStyle = "rgba(227, 164, 112, 0.34)";
    context.fill();

    const vignette = context.createRadialGradient(
      planet.x - planet.radius * 0.24,
      planet.y - planet.radius * 0.32,
      planet.radius * 0.1,
      planet.x,
      planet.y,
      planet.radius
    );
    vignette.addColorStop(0, "rgba(255,255,255,0.24)");
    vignette.addColorStop(0.4, "rgba(255,255,255,0.02)");
    vignette.addColorStop(1, "rgba(0,0,0,0.44)");
    context.fillStyle = vignette;
    context.fillRect(planet.x - planet.radius, planet.y - planet.radius, planet.radius * 2, planet.radius * 2);
    context.restore();
  };

  const resize = () => {
    state.dpr = Math.min(window.devicePixelRatio || 1, 1.6);
    state.width = window.innerWidth;
    state.height = window.innerHeight;
    canvas.width = Math.round(state.width * state.dpr);
    canvas.height = Math.round(state.height * state.dpr);
    canvas.style.width = `${state.width}px`;
    canvas.style.height = `${state.height}px`;
    context.setTransform(state.dpr, 0, 0, state.dpr, 0, 0);
    const starCount = Math.max(
      state.reducedMotion ? 90 : 140,
      Math.min(state.reducedMotion ? 160 : 220, Math.round((state.width * state.height) / 11000))
    );
    const dustCount = Math.max(
      state.reducedMotion ? 40 : 70,
      Math.min(state.reducedMotion ? 80 : 120, Math.round((state.width * state.height) / 22000))
    );
    state.stars = Array.from({ length: starCount }, () => spawnStar());
    state.dust = Array.from({ length: dustCount }, () => spawnDust());
  };

  const drawFrame = (timeMs = 0) => {
    if (document.hidden) {
      state.frameId = requestAnimationFrame(drawFrame);
      return;
    }
    const delta = state.lastTime ? Math.min(0.032, (timeMs - state.lastTime) / 1000) : 0.016;
    state.lastTime = timeMs;

    context.clearRect(0, 0, state.width, state.height);
    drawBackdrop();
    drawStars(timeMs);
    drawDust(timeMs, delta * 60);

    const planet = planetAnchor();
    const spin = timeMs * (state.reducedMotion ? 0.00008 : 0.00016);
    drawRingLayer(planet, spin, false);
    drawPlanet(planet, spin, timeMs);
    drawRingLayer(planet, spin, true);

    state.frameId = requestAnimationFrame(drawFrame);
  };

  const handleResize = () => resize();
  resize();
  state.frameId = requestAnimationFrame(drawFrame);
  window.addEventListener("resize", handleResize);
  tunnelBgState = {
    state,
    cleanup() {
      cancelAnimationFrame(state.frameId);
      window.removeEventListener("resize", handleResize);
    },
  };
}

function setWorkspaceView(view, { persist = true, scroll = true } = {}) {
  const key = view in WORKSPACE_VIEWS ? view : "focus";
  const meta = WORKSPACE_VIEWS[key];
  dashboardState.currentView = key;
  document.body.dataset.view = key;
  if ($("active-view-title")) $("active-view-title").textContent = meta.label;
  if ($("active-view-description")) $("active-view-description").textContent = meta.description;
  if ($("active-view-chip")) $("active-view-chip").textContent = meta.chip;
  document.querySelectorAll("[data-view]").forEach((button) => {
    button.classList.toggle("active", button.dataset.view === key);
  });
  if (persist) {
    try {
      window.localStorage.setItem("okx-desk-view", key);
    } catch (_) {}
  }
  if (scroll) {
    window.scrollTo({ top: 0, behavior: "smooth" });
  }
  if (key === "trade" && !dashboardState.accountDetailsLoadedOnce) {
    refreshSnapshot().catch(() => {});
  }
  if (key === "orders" && !dashboardState.accountDetailsLoadedOnce) {
    refreshSnapshot().catch(() => {});
  }
  if (key === "orders" && !dashboardState.ordersLoadedOnce) {
    refreshOrders().catch(() => {});
  }
  syncOrderPolling();
}

function runSingleFlight(key, task) {
  if (requestFlightMap.has(key)) {
    return requestFlightMap.get(key);
  }
  const promise = Promise.resolve()
    .then(task)
    .finally(() => {
      requestFlightMap.delete(key);
    });
  requestFlightMap.set(key, promise);
  return promise;
}

async function request(url, options = {}) {
  const { timeoutMs = LOCAL_REQUEST_TIMEOUT_MS, headers = {}, ...rest } = options;
  const controller = new AbortController();
  const timer = window.setTimeout(() => controller.abort("timeout"), timeoutMs);
  try {
    const response = await fetch(url, {
      headers: { "Content-Type": "application/json", ...headers },
      signal: controller.signal,
      ...rest,
    });
    const data = await response.json();
    if (!response.ok || data.ok === false) {
      throw new Error(data.error || `Request failed: ${response.status}`);
    }
    return data;
  } catch (error) {
    if (error?.name === "AbortError" || error === "timeout") {
      throw new Error("本地服务响应超时，已自动中断这一轮请求");
    }
    throw error;
  } finally {
    window.clearTimeout(timer);
  }
}

function setMessage(text, kind = "") {
  const el = $("config-message");
  el.textContent = text;
  el.className = `notice ${kind}`;
}

function setAutomationMessage(text, kind = "") {
  const el = $("automation-message");
  el.textContent = text;
  el.className = `notice ${kind}`;
}

function setMinerMessage(text, kind = "") {
  const el = $("miner-message");
  el.textContent = text;
  el.className = `notice ${kind}`;
}

function setLiveFeedStatus(kind, text, detail) {
  const dot = $("live-feed-dot");
  dot.style.background =
    kind === "ok" ? "var(--success)" : kind === "err" ? "var(--danger)" : "var(--accent)";
  $("live-feed-text").textContent = text;
  $("live-feed-detail").textContent = detail;
  if ($("guard-feed")) renderDeskGuards();
}

function updateEndpointCards(presetKey) {
  const preset = ENV_PRESETS[presetKey] || ENV_PRESETS.custom;
  const restBase = $("baseUrl").value.trim() || preset.baseUrl;
  const restMode = $("simulated").value === "true" ? "模拟专用（x-simulated-trading: 1）" : "实盘";
  $("endpoint-rest").textContent = `${restBase} · ${restMode}`;
  $("endpoint-public-ws").textContent = preset.publicWs;
  $("endpoint-private-ws").textContent = preset.privateWs;
  $("endpoint-business-ws").textContent = preset.businessWs;
  const hint = $("base-url-hint");
  if (hint) {
    hint.textContent = $("simulated").value === "true"
      ? "主站模拟盘的 REST 域名仍是 okx.com；真正区分依赖模拟盘专用 API Key、x-simulated-trading: 1，以及 wspap 私有/业务 WebSocket。"
      : "实盘和模拟盘可以共用主站 REST 域名，但必须使用对应环境的 API Key；实盘私有/业务 WebSocket 为 ws.okx.com。";
  }
}

function deriveMarketWsEndpoints() {
  const presetKey = $("envPreset").value;
  const preset = ENV_PRESETS[presetKey] || ENV_PRESETS.custom;
  if (preset.publicWs !== "-" && preset.businessWs !== "-") {
    return {
      publicWs: preset.publicWs,
      businessWs: preset.businessWs,
    };
  }

  const baseUrl = $("baseUrl").value.trim();
  const simulated = $("simulated").value === "true";
  const isUS = baseUrl.includes("us.okx.com");
  if (isUS) {
    return {
      publicWs: simulated
        ? "wss://wsuspap.okx.com:8443/ws/v5/public"
        : "wss://wsus.okx.com:8443/ws/v5/public",
      businessWs: simulated
        ? "wss://wsuspap.okx.com:8443/ws/v5/business"
        : "wss://wsus.okx.com:8443/ws/v5/business",
    };
  }
  return {
    publicWs: simulated
      ? "wss://wspap.okx.com:8443/ws/v5/public"
      : "wss://ws.okx.com:8443/ws/v5/public",
    businessWs: simulated
      ? "wss://wspap.okx.com:8443/ws/v5/business"
      : "wss://ws.okx.com:8443/ws/v5/business",
  };
}

function formatPrice(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) return "--";
  if (num >= 1000) return num.toLocaleString("en-US", { maximumFractionDigits: 2 });
  if (num >= 1) return num.toLocaleString("en-US", { maximumFractionDigits: 4 });
  return num.toLocaleString("en-US", { maximumFractionDigits: 6 });
}

function formatDockPrice(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) return "--";
  if (num >= 10000) return num.toLocaleString("en-US", { maximumFractionDigits: 1 });
  if (num >= 1) return num.toLocaleString("en-US", { maximumFractionDigits: 2 });
  if (num >= 0.1) return num.toLocaleString("en-US", { maximumFractionDigits: 4 });
  return num.toLocaleString("en-US", { maximumFractionDigits: 5 });
}

function formatRatio(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) return "--";
  const pct = num * 100;
  const sign = pct > 0 ? "+" : "";
  return `${sign}${pct.toFixed(2)}%`;
}

function formatMoney(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) return "--";
  return num.toLocaleString("en-US", {
    minimumFractionDigits: num >= 1000 ? 0 : 2,
    maximumFractionDigits: num >= 1000 ? 2 : 2,
  });
}

function formatPercentValue(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) return "--";
  const sign = num > 0 ? "+" : "";
  return `${sign}${num.toFixed(2)}%`;
}

function buildSmoothPath(points) {
  if (!points.length) return "";
  if (points.length === 1) return `M ${points[0][0]} ${points[0][1]}`;
  if (points.length === 2) return `M ${points[0][0]} ${points[0][1]} L ${points[1][0]} ${points[1][1]}`;
  let path = `M ${points[0][0]} ${points[0][1]}`;
  for (let index = 1; index < points.length - 1; index += 1) {
    const current = points[index];
    const next = points[index + 1];
    const midX = (current[0] + next[0]) / 2;
    const midY = (current[1] + next[1]) / 2;
    path += ` Q ${current[0]} ${current[1]} ${midX} ${midY}`;
  }
  const penultimate = points[points.length - 2];
  const last = points[points.length - 1];
  path += ` Q ${penultimate[0]} ${penultimate[1]} ${last[0]} ${last[1]}`;
  return path;
}

function buildAreaPath(points, baselineY) {
  if (!points.length) return "";
  const linePath = buildSmoothPath(points);
  const first = points[0];
  const last = points[points.length - 1];
  return `${linePath} L ${last[0]} ${baselineY} L ${first[0]} ${baselineY} Z`;
}

function buildGridMarkup(width, height, left, right, top, bottom, min, max, formatter, showLabels) {
  const rows = 4;
  const span = Math.max(max - min, 1);
  return Array.from({ length: rows }, (_, index) => {
    const ratio = index / (rows - 1);
    const y = top + (bottom - top) * ratio;
    const value = max - span * ratio;
    const label = formatter(value);
    return `
      <line x1="${left}" y1="${y}" x2="${right}" y2="${y}" stroke="rgba(255,255,255,0.08)" stroke-dasharray="4 8"></line>
      ${showLabels ? `<text x="${right - 6}" y="${y - 6}" text-anchor="end" fill="rgba(233,238,245,0.42)" font-size="11" font-family="Avenir Next, SF Pro Display, sans-serif">${label}</text>` : ""}
    `;
  }).join("");
}

function bindCurveAutoFollow(shell) {
  if (!shell || shell.dataset.followBound === "1") return;
  shell.dataset.followBound = "1";
  shell.dataset.autoFollow = "1";
  shell.addEventListener("scroll", () => {
    const tailGap = Math.max(shell.scrollWidth - shell.clientWidth - shell.scrollLeft, 0);
    shell.dataset.autoFollow = tailGap <= 24 ? "1" : "0";
  });
}

function computeCurveRenderWidth(sampleCount, shell, baseWidth, stepWidth, paddingX) {
  const visibleWidth = Math.max(shell?.clientWidth || 0, baseWidth);
  if (sampleCount <= 1) return visibleWidth;
  return Math.max(visibleWidth, paddingX * 2 + (sampleCount - 1) * stepWidth);
}

function applyCurveViewport(svg, renderWidth, renderHeight) {
  if (!svg) return;
  svg.setAttribute("viewBox", `0 0 ${renderWidth} ${renderHeight}`);
  svg.style.width = `${renderWidth}px`;
  const shell = svg.parentElement;
  if (!shell) return;
  bindCurveAutoFollow(shell);
  requestAnimationFrame(() => {
    if (shell.dataset.autoFollow !== "0") {
      shell.scrollLeft = Math.max(shell.scrollWidth - shell.clientWidth, 0);
    }
  });
}

function renderEquityCurve(curve) {
  const svg = $("equity-curve");
  const dockSvg = $("dock-equity-curve");
  const curveMeta = $("equity-curve-meta");
  const dockStats = $("dock-curve-stats");
  const samples = (curve || [])
    .map((point) => ({
      ts: point.ts,
      eq: Number(point.eq),
    }))
    .filter((point) => Number.isFinite(point.eq));

  if (!samples.length) {
    const emptyMarkup = `
      <rect x="0" y="0" width="960" height="220" rx="18" fill="rgba(255,255,255,0.015)"></rect>
      <line x1="48" y1="68" x2="912" y2="68" stroke="rgba(255,255,255,0.06)" stroke-dasharray="6 10"></line>
      <line x1="48" y1="150" x2="912" y2="150" stroke="rgba(255,255,255,0.06)" stroke-dasharray="6 10"></line>
      <text x="480" y="106" text-anchor="middle" fill="rgba(233,238,245,0.68)" font-size="20" font-family="Avenir Next, SF Pro Display, sans-serif">
        等待策略会话样本
      </text>
      <text x="480" y="132" text-anchor="middle" fill="rgba(140,152,169,0.85)" font-size="13" font-family="Avenir Next, SF Pro Display, sans-serif">
        曲线会在拿到收益点后自动补全峰谷、高亮和面积层
      </text>
    `;
    if (svg) {
      applyCurveViewport(svg, 960, 220);
      svg.innerHTML = emptyMarkup;
    }
    if (dockSvg) {
      applyCurveViewport(dockSvg, 960, 130);
      dockSvg.innerHTML = `
        <rect x="0" y="0" width="960" height="130" rx="14" fill="rgba(255,255,255,0.015)"></rect>
        <line x1="30" y1="44" x2="930" y2="44" stroke="rgba(255,255,255,0.06)" stroke-dasharray="5 8"></line>
        <line x1="30" y1="92" x2="930" y2="92" stroke="rgba(255,255,255,0.06)" stroke-dasharray="5 8"></line>
        <text x="480" y="70" text-anchor="middle" fill="rgba(233,238,245,0.62)" font-size="16" font-family="Avenir Next, SF Pro Display, sans-serif">
          等待收益曲线
        </text>
      `;
    }
    if (curveMeta) {
      curveMeta.textContent = "等待会话样本";
    }
    $("dock-equity-meta").textContent = "等待样本";
    if (dockStats) {
      dockStats.innerHTML = `
        <div class="dock-stat"><span>样本</span><strong>--</strong></div>
        <div class="dock-stat"><span>峰值</span><strong>--</strong></div>
        <div class="dock-stat"><span>低点</span><strong>--</strong></div>
        <div class="dock-stat"><span>变化</span><strong>--</strong></div>
      `;
    }
    return;
  }

  const width = computeCurveRenderWidth(samples.length, svg?.parentElement, 960, 38, 18);
  const height = 220;
  const paddingX = 18;
  const paddingY = 16;
  const values = samples.map((point) => point.eq);
  const min = Math.min(...values);
  const max = Math.max(...values);
  const span = Math.max(max - min, 1);
  const mainBottom = height - paddingY;
  const step = samples.length > 1 ? (width - paddingX * 2) / (samples.length - 1) : 0;
  const points = samples.map((point, index) => {
    const x = paddingX + step * index;
    const y = mainBottom - ((point.eq - min) / span) * (height - paddingY * 2);
    return [x, y];
  });

  const linePath = buildSmoothPath(points);
  const areaPath = buildAreaPath(points, mainBottom);
  const latest = samples[samples.length - 1];
  const earliest = samples[0];
  const delta = latest.eq - earliest.eq;
  const up = delta >= 0;
  const deltaPct = earliest.eq > 0 ? (delta / earliest.eq) * 100 : 0;
  const latestPoint = points[points.length - 1];
  const peakIndex = values.indexOf(max);
  const troughIndex = values.indexOf(min);
  const peakPoint = points[peakIndex];
  const troughPoint = points[troughIndex];
  const gridMarkup = buildGridMarkup(width, height, paddingX, width - paddingX, paddingY, mainBottom, min, max, formatMoney, true);

  applyCurveViewport(svg, width, height);

  if (svg) {
    svg.innerHTML = `
      <defs>
        <linearGradient id="equityFillMain" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stop-color="${up ? "rgba(105,240,174,0.30)" : "rgba(255,107,107,0.26)"}"></stop>
          <stop offset="100%" stop-color="rgba(255,255,255,0.01)"></stop>
        </linearGradient>
        <linearGradient id="equityStrokeMain" x1="0" y1="0" x2="1" y2="0">
          <stop offset="0%" stop-color="${up ? "#9ef8c6" : "#ff9a9a"}"></stop>
          <stop offset="100%" stop-color="${up ? "#45d6c4" : "#ff6b6b"}"></stop>
        </linearGradient>
        <filter id="equityGlowMain" x="-20%" y="-20%" width="140%" height="140%">
          <feGaussianBlur stdDeviation="6" result="blur"></feGaussianBlur>
          <feMerge>
            <feMergeNode in="blur"></feMergeNode>
            <feMergeNode in="SourceGraphic"></feMergeNode>
          </feMerge>
        </filter>
      </defs>
      <rect x="0" y="0" width="${width}" height="${height}" rx="18" fill="rgba(255,255,255,0.015)"></rect>
      ${gridMarkup}
      <path d="${areaPath}" fill="url(#equityFillMain)" stroke="none"></path>
      <path d="${linePath}" fill="none" stroke="url(#equityStrokeMain)" stroke-width="8" stroke-linecap="round" stroke-linejoin="round" opacity="0.18" filter="url(#equityGlowMain)"></path>
      <path d="${linePath}" fill="none" stroke="url(#equityStrokeMain)" stroke-width="3.4" stroke-linecap="round" stroke-linejoin="round"></path>
      <line x1="${latestPoint[0]}" y1="${paddingY}" x2="${latestPoint[0]}" y2="${mainBottom}" stroke="rgba(255,255,255,0.08)" stroke-dasharray="4 8"></line>
      <circle cx="${latestPoint[0]}" cy="${latestPoint[1]}" r="8" fill="rgba(11,15,20,0.92)" stroke="${up ? "#69f0ae" : "#ff6b6b"}" stroke-width="2.5"></circle>
      <circle cx="${latestPoint[0]}" cy="${latestPoint[1]}" r="3.2" fill="${up ? "#69f0ae" : "#ff6b6b"}"></circle>
      <circle cx="${peakPoint[0]}" cy="${peakPoint[1]}" r="3.2" fill="rgba(255,255,255,0.85)"></circle>
      <circle cx="${troughPoint[0]}" cy="${troughPoint[1]}" r="3.2" fill="rgba(255,255,255,0.6)"></circle>
      <text x="${paddingX}" y="${paddingY + 12}" fill="rgba(233,238,245,0.92)" font-size="13" font-family="Avenir Next, SF Pro Display, sans-serif">会话走势 ${formatPercentValue(deltaPct)}</text>
      <text x="${paddingX}" y="${paddingY + 30}" fill="rgba(140,152,169,0.92)" font-size="12" font-family="Avenir Next, SF Pro Display, sans-serif">峰值 ${formatMoney(max)} · 低点 ${formatMoney(min)}</text>
    `;
  }
  if (curveMeta) {
    curveMeta.textContent = `${samples.length} 个样本 · 最新 ${formatMoney(latest.eq)} USDT · ${formatPercentValue(deltaPct)}`;
  }
  $("dock-equity-meta").textContent = `${samples.length} 点 · ${formatPercentValue(deltaPct)} · ${formatMoney(latest.eq)} USDT`;
  $("dock-equity-title").textContent = delta >= 0 ? "收益曲线悬浮窗 · 维持上行观察" : "收益曲线悬浮窗 · 留意回撤";
  if (dockStats) {
    dockStats.innerHTML = `
      <div class="dock-stat"><span>样本</span><strong>${samples.length} 点</strong></div>
      <div class="dock-stat"><span>峰值</span><strong>${formatMoney(max)}</strong></div>
      <div class="dock-stat"><span>低点</span><strong>${formatMoney(min)}</strong></div>
      <div class="dock-stat"><span>变化</span><strong>${formatPercentValue(deltaPct)}</strong></div>
    `;
  }
  if (dockSvg) {
    const dockHeight = 130;
    const dockPaddingX = 14;
    const dockPaddingY = 12;
    const dockWidth = computeCurveRenderWidth(samples.length, dockSvg.parentElement, 960, 26, dockPaddingX);
    const dockBottom = dockHeight - dockPaddingY;
    const dockStep = samples.length > 1 ? (dockWidth - dockPaddingX * 2) / (samples.length - 1) : 0;
    const dockPoints = samples.map((point, index) => {
      const x = dockPaddingX + dockStep * index;
      const y = dockBottom - ((point.eq - min) / span) * (dockHeight - dockPaddingY * 2);
      return [x, y];
    });
    const dockLinePath = buildSmoothPath(dockPoints);
    const dockAreaPath = buildAreaPath(dockPoints, dockBottom);
    const dockLatestPoint = dockPoints[dockPoints.length - 1];
    const dockGridMarkup = buildGridMarkup(dockWidth, dockHeight, dockPaddingX, dockWidth - dockPaddingX, dockPaddingY, dockBottom, min, max, formatMoney, false);
    applyCurveViewport(dockSvg, dockWidth, dockHeight);
    dockSvg.innerHTML = `
      <defs>
        <linearGradient id="equityFillDock" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stop-color="${up ? "rgba(105,240,174,0.24)" : "rgba(255,107,107,0.22)"}"></stop>
          <stop offset="100%" stop-color="rgba(255,255,255,0.01)"></stop>
        </linearGradient>
        <linearGradient id="equityStrokeDock" x1="0" y1="0" x2="1" y2="0">
          <stop offset="0%" stop-color="${up ? "#9ef8c6" : "#ff9a9a"}"></stop>
          <stop offset="100%" stop-color="${up ? "#45d6c4" : "#ff6b6b"}"></stop>
        </linearGradient>
      </defs>
      <rect x="0" y="0" width="${dockWidth}" height="${dockHeight}" rx="14" fill="rgba(255,255,255,0.015)"></rect>
      ${dockGridMarkup}
      <path d="${dockAreaPath}" fill="url(#equityFillDock)" stroke="none"></path>
      <path d="${dockLinePath}" fill="none" stroke="url(#equityStrokeDock)" stroke-width="2.8" stroke-linecap="round" stroke-linejoin="round"></path>
      <line x1="${dockLatestPoint[0]}" y1="${dockPaddingY}" x2="${dockLatestPoint[0]}" y2="${dockBottom}" stroke="rgba(255,255,255,0.08)" stroke-dasharray="4 8"></line>
      <circle cx="${dockLatestPoint[0]}" cy="${dockLatestPoint[1]}" r="6.2" fill="rgba(11,15,20,0.92)" stroke="${up ? "#69f0ae" : "#ff6b6b"}" stroke-width="2"></circle>
      <circle cx="${dockLatestPoint[0]}" cy="${dockLatestPoint[1]}" r="2.6" fill="${up ? "#69f0ae" : "#ff6b6b"}"></circle>
    `;
  }
}

function renderDeskGuards() {
  const simulated = $("simulated").value === "true";
  const allowLiveManualOrders = $("autoAllowLiveManualOrders").checked;
  const allowLiveTrading = $("autoAllowLiveTrading").checked;
  const allowLiveAutostart = $("autoAllowLiveAutostart").checked;
  const autostart = $("autoAutostart").checked;
  const pollSeconds = Number($("autoPollSeconds").value || 0);
  const maxOrders = Number($("autoMaxOrdersPerDay").value || 0);
  const maxLoss = Number($("autoMaxDailyLossPct").value || 0);
  const feedHealthy = liveMarketState.tickerReady || liveMarketState.candleReady;
  const riskPill = $("desk-risk-pill");

  $("guard-env").textContent = simulated
    ? "模拟盘，适合先做回路验证"
    : `实盘 · ${$("baseUrl").value.trim() || "OKX"} 正在直连`;
  $("guard-live").textContent = simulated
    ? "模拟盘下无需额外实盘开关"
    : [
        allowLiveManualOrders ? "手动下单已解锁" : "手动下单仍锁着",
        allowLiveTrading
          ? `自动交易已解锁${autostart && allowLiveAutostart ? " / 自动启动" : ""}`
          : "自动交易仍锁着",
      ].join(" · ");
  $("guard-cycle").textContent = `${$("autoBar").value} 周期 · ${pollSeconds || "--"} 秒轮询`;
  $("guard-risk").textContent = `日内最大回撤 ${maxLoss || 0}% · 今日上限 ${maxOrders || 0} 单`;
  $("guard-feed").textContent = feedHealthy
    ? "WebSocket 正在推送"
    : "轮询兜底已启用，后台继续重连";

  if (simulated) {
    riskPill.textContent = "模拟护栏";
    riskPill.style.borderColor = "rgba(69, 214, 196, 0.28)";
    riskPill.style.color = "var(--accent-2)";
  } else if (!allowLiveManualOrders && !allowLiveTrading) {
    riskPill.textContent = "实盘锁定";
    riskPill.style.borderColor = "rgba(255, 184, 77, 0.24)";
    riskPill.style.color = "var(--accent)";
  } else if (!allowLiveManualOrders) {
    riskPill.textContent = "手动单受限";
    riskPill.style.borderColor = "rgba(255, 184, 77, 0.24)";
    riskPill.style.color = "var(--accent)";
  } else if (autostart && !allowLiveAutostart) {
    riskPill.textContent = "自动启动受限";
    riskPill.style.borderColor = "rgba(255, 107, 107, 0.24)";
    riskPill.style.color = "var(--danger)";
  } else {
    riskPill.textContent = "执行已解锁";
    riskPill.style.borderColor = "rgba(105, 240, 174, 0.22)";
    riskPill.style.color = "var(--success)";
  }
}

function hasLegacyNonSwingText(value = "") {
  const text = String(value || "").trim();
  if (!text) return false;
  return [
    "高频套利",
    "价差回归套利",
    "当前负基差",
    "不做这侧套利",
    "watchlist 可做",
    "市场候选",
    "反向",
    "套利入场",
    "套利窗口",
  ].some((marker) => text.includes(marker));
}

function getOnlySwingPrimarySymbol() {
  const config = dashboardState.savedAutomationConfig || normalizeAutomationConfigForCompare(collectAutomationConfig());
  const watchlist = parseWatchlistSymbols(
    config.watchlistSymbols,
    config.spotInstId || "BTC-USDT",
    config.swapInstId || "BTC-USDT-SWAP",
  );
  return watchlist[0] || "BTC";
}

function sanitizeAnalysisForSwingOnly(analysis = {}) {
  const data = { ...(analysis || {}) };
  const config = dashboardState.savedAutomationConfig || normalizeAutomationConfigForCompare(collectAutomationConfig());
  const symbol = getOnlySwingPrimarySymbol();
  const stale = (
    hasLegacyNonSwingText(data.selectedStrategyName)
    || hasLegacyNonSwingText(data.selectedStrategyDetail)
    || hasLegacyNonSwingText(data.decisionLabel)
    || hasLegacyNonSwingText(data.summary)
    || hasLegacyNonSwingText(data.marketRegime)
  );
  data.selectedStrategyName = `${symbol} 波段`;
  data.selectedStrategyDetail = `趋势转强 + 回踩接多 · 逐仓 ${config.swapLeverage || "10"}x · TP ${config.takeProfitPct || "8"}%`;
  if (stale) {
    const allowNewEntries = Boolean(data.allowNewEntries);
    data.decision = allowNewEntries ? "execute" : "observe";
    data.decisionLabel = allowNewEntries ? "允许波段开多" : "等待波段买点";
    data.summary = "当前只保留波段策略，旧套利分析结果已隐藏。";
    data.marketRegime = "波段观察";
    data.warnings = [];
    data.blockers = [];
  }
  return data;
}

function sanitizeAutomationStateForSwingOnly(state = {}) {
  const sanitized = { ...(state || {}) };
  sanitized.analysis = sanitizeAnalysisForSwingOnly(sanitized.analysis || {});
  if (hasLegacyNonSwingText(sanitized.modeText)) {
    sanitized.modeText = "波段";
  }
  const pipeline = { ...(sanitized.lastPipeline || {}) };
  if (hasLegacyNonSwingText(pipeline.summary)) {
    pipeline.summary = `${sanitized.analysis.selectedStrategyName || "波段"} · ${sanitized.analysis.decisionLabel || "等待波段买点"}`;
  }
  sanitized.lastPipeline = pipeline;
  return sanitized;
}

function renderAnalysisState(analysis) {
  const data = sanitizeAnalysisForSwingOnly(analysis || {});
  $("analysis-decision").textContent =
    data.decisionLabel
      ? `${data.decisionLabel}${data.selectedScore ? ` · 分数 ${data.selectedScore}` : ""}`
      : "等待首轮联网分析";
  $("analysis-strategy").textContent =
    data.selectedStrategyName
      ? `${data.selectedStrategyName}${data.selectedReturnPct ? ` · 收益 ${formatPercentValue(data.selectedReturnPct)}` : ""}${data.selectedDrawdownPct ? ` · 回撤 ${formatPercentValue(data.selectedDrawdownPct)}` : ""}`
      : "先联网分析，再给出本轮最优策略";
  $("analysis-market").textContent =
    data.marketRegime
      ? [
          data.marketRegime,
          `回撤 ${data.pullbackPct || "--"}%`,
          `反弹 ${data.reboundPct || "--"}%`,
          data.liquidationBufferPct ? `强平缓冲 ${data.liquidationBufferPct}%` : "",
        ].filter(Boolean).join(" · ")
      : "等待趋势、回撤、反弹和强平缓冲分析";
  $("analysis-refresh").textContent =
    data.lastAnalyzedAt
      ? [
          data.lastAnalyzedAt,
          data.fundingRatePct ? `资金费 ${data.fundingRatePct}%` : "",
          data.basisPct ? `基差 ${data.basisPct}%` : "",
          data.liquidationPrice ? `强平价 ${data.liquidationPrice}` : "",
        ].filter(Boolean).join(" · ")
      : "等待最新分析时间";
  const reasonBits = [];
  if (data.summary) reasonBits.push(data.summary);
  if (data.blockers?.length) reasonBits.push(`阻断: ${data.blockers.join("；")}`);
  else if (data.warnings?.length) reasonBits.push(`提醒: ${data.warnings.join("；")}`);
  $("analysis-reason").textContent =
    reasonBits.join(" | ") || "这里只保留波段开多、观察和风控解释。";

  const pill = $("analysis-pill");
  const dockMain = $("dock-status-main");
  const dockSub = $("dock-status-sub");
  if (data.decision === "execute") {
    pill.textContent = "允许执行";
    pill.style.color = "var(--success)";
    pill.style.borderColor = "rgba(105, 240, 174, 0.22)";
  } else if (data.decision === "observe") {
    pill.textContent = "观察中";
    pill.style.color = "var(--accent)";
    pill.style.borderColor = "rgba(255, 184, 77, 0.22)";
  } else if (data.decision === "skip") {
    pill.textContent = "跳过新开仓";
    pill.style.color = "var(--danger)";
    pill.style.borderColor = "rgba(255, 107, 107, 0.22)";
  } else {
    pill.textContent = "待分析";
    pill.style.color = "var(--accent-2)";
    pill.style.borderColor = "rgba(69, 214, 196, 0.24)";
  }
  if (dockMain) {
    dockMain.textContent = data.decisionLabel
      ? `${data.decisionLabel} · ${data.selectedStrategyName || "未选策略"}`
      : "等待联网预检";
  }
  if (dockSub) {
    const dockSummaryBits = [];
    if (data.marketRegime) dockSummaryBits.push(data.marketRegime);
    if (data.volatilityPct) dockSummaryBits.push(`波动 ${data.volatilityPct}%`);
    if (data.pullbackPct) dockSummaryBits.push(`回撤 ${data.pullbackPct}%`);
    if (data.reboundPct) dockSummaryBits.push(`反弹 ${data.reboundPct}%`);
    if (data.fundingRatePct) dockSummaryBits.push(`资金费 ${data.fundingRatePct}%`);
    if (data.selectedReturnPct) dockSummaryBits.push(`收益 ${formatPercentValue(data.selectedReturnPct)}`);
    if (data.blockers?.length) dockSummaryBits.push(`阻断 ${data.blockers[0]}`);
    else if (data.warnings?.length) dockSummaryBits.push(`提醒 ${data.warnings[0]}`);
    dockSub.textContent = dockSummaryBits.join(" · ") || "顶部固定显示当前策略、联网判断和执行环境。";
  }
}

function extractAutomationStopReason(statusText = "", lastError = "") {
  const rawStatus = String(statusText || "").trim();
  const rawError = String(lastError || "").trim();
  const trimmedStatus = rawStatus
    .replace(/^自动量化已停止[:：]?\s*/, "")
    .replace(/^组合策略已停止[:：]?\s*/, "")
    .replace(/^已停止[:：]?\s*/, "")
    .trim();
  if (rawError && rawError !== rawStatus) return rawError;
  if (trimmedStatus && trimmedStatus !== rawStatus) return trimmedStatus;
  return rawError || trimmedStatus || rawStatus;
}

function deriveDeskModePresentation(automation = {}, modeText = "模拟盘") {
  const statusText = String(automation.statusText || "").trim();
  const modeDetail = String(automation.modeText || "").trim();
  const running = Boolean(automation.running);
  const consecutiveErrors = Number(automation.consecutiveErrors || 0);
  const stopReason = extractAutomationStopReason(statusText, automation.lastError);
  const hasHardStop = !running && (
    consecutiveErrors > 0
    || /连续错误过多|错误|失败|异常/.test(statusText)
    || /连续错误过多|错误|失败|异常/.test(stopReason)
  );

  if (hasHardStop) {
    return {
      tone: "danger",
      title: "自动量化已停机",
      sub: `${modeText} · ${modeDetail || "组合交易执行已暂停"}`,
      alert: {
        eyebrow: "需要处理",
        main: /连续错误过多/.test(`${statusText} ${stopReason}`)
          ? "连续错误过多，系统已自动熔断"
          : "最近一轮执行失败，系统已停止",
        sub: stopReason && stopReason !== statusText
          ? stopReason
          : (consecutiveErrors > 0
            ? `最近已连续报错 ${consecutiveErrors} 次，请先处理后再重启。`
            : "当前组合已自动暂停，请先处理最近一次错误。"),
      },
    };
  }

  if (!running && /已停止|已手动停止/.test(statusText)) {
    return {
      tone: "warn",
      title: "自动量化已暂停",
      sub: `${modeText} · ${modeDetail || "组合交易未运行"}`,
      alert: {
        eyebrow: "当前待机",
        main: extractAutomationStopReason(statusText, "") || "已手动停止",
        sub: "仓位摘要和收益会继续展示，但不会再自动发单。",
      },
    };
  }

  if (running) {
    return {
      tone: "ok",
      title: `${modeText} · 自动量化运行中`,
      sub: modeDetail || "组合交易正在轮询、独立决策与风控。",
      alert: null,
    };
  }

  return {
    tone: "idle",
    title: `${modeText} · ${statusText || "未启动"}`,
    sub: modeDetail || "模拟 / 实盘 + 策略状态",
    alert: null,
  };
}

function renderDeskOverview() {
  const account = dashboardState.account || {};
  const summary = account.summary || {};
  const automation = dashboardState.automation || {};
  const miner = dashboardState.miner || {};
  const minerConfig = miner.config || {};
  const minerNetwork = miner.network || {};
  const minerProgress = miner.progress || {};
  const minerFees = minerNetwork.fees || {};
  // Keep account balance and strategy session equity on separate tracks.
  const totalEq = Number(summary.displayTotalEq || summary.totalEq || 0);
  const currentEq = Number(automation.currentEq || 0);
  const tradingTotalEq = Number(summary.tradingTotalEq || summary.adjEq || 0);
  const fundingTotalEq = Number(summary.fundingTotalEq || 0);
  const startEq = Number(automation.sessionStartEq || 0);
  const pnlAmount = startEq > 0 ? currentEq - startEq : 0;
  const pnlPct = startEq > 0 ? (pnlAmount / startEq) * 100 : 0;
  const drawdown = Number(automation.dailyDrawdownPct || 0);
  const maxOrders = Number($("autoMaxOrdersPerDay").value || 0);
  const pollSeconds = Number($("autoPollSeconds").value || 0);
  const cycleDurationMs = Number(automation.lastCycleDurationMs || 0);
  const modeText = $("simulated").value === "true" ? "模拟盘" : "实盘";
  const modePresentation = deriveDeskModePresentation(automation, modeText);
  const dockPnlMain = $("dock-pnl-main");
  const dockPnlSub = $("dock-pnl-sub");
  const minerDailyUsd = Number(minerProgress.dailyUsd || 0);
  const railBalanceMain = $("rail-balance-main");
  const railBalanceSub = $("rail-balance-sub");
  const railBalanceTarget = $("rail-balance-target");
  const railBalanceProgress = $("rail-balance-progress");
  const railMinerMain = $("rail-miner-main");
  const railMinerSub = $("rail-miner-sub");
  const balanceBreakdown = summary.displayBreakdown
    || (fundingTotalEq > 0
      ? `资金账户 ${formatMoney(fundingTotalEq)} USDT · 交易账户 ${formatMoney(tradingTotalEq)} USDT`
      : (tradingTotalEq > 0 ? `交易账户 ${formatMoney(tradingTotalEq)} USDT` : ""));
  const balanceGoalBase = startEq > 0 ? startEq : totalEq;
  const stateTargetEq = Number(automation.targetBalanceEq || 0);
  const stateTargetProgressPct = Number(automation.targetBalanceProgressPct || 0);
  const balanceTargetEq = stateTargetEq > 0 ? stateTargetEq : (balanceGoalBase > 0 ? balanceGoalBase * 10 : 0);
  const balanceTargetProgressPct = stateTargetEq > 0
    ? stateTargetProgressPct
    : (balanceTargetEq > 0 ? (totalEq / balanceTargetEq) * 100 : 0);

  $("desk-total-equity").textContent = totalEq > 0 ? `${formatMoney(totalEq)} USDT` : "--";
  $("desk-total-equity-sub").textContent =
    balanceBreakdown || (summary.adjEq ? `调整后权益 ${formatMoney(summary.adjEq)} USDT` : "等待账户快照");

  $("desk-session-pnl").textContent =
    startEq > 0 ? `${formatPercentValue(pnlPct)}` : "--";
  $("desk-session-pnl").style.color =
    pnlAmount > 0 ? "var(--success)" : pnlAmount < 0 ? "var(--danger)" : "var(--text)";
  $("desk-session-pnl-sub").textContent =
    startEq > 0 ? `约 ${pnlAmount >= 0 ? "+" : ""}${formatMoney(pnlAmount)} USDT` : "从本次自动化会话开始统计";
  if (dockPnlMain) {
    dockPnlMain.textContent = startEq > 0 ? formatPercentValue(pnlPct) : "--";
    dockPnlMain.style.color =
      pnlAmount > 0 ? "var(--success)" : pnlAmount < 0 ? "var(--danger)" : "var(--text)";
  }
  if (dockPnlSub) {
    dockPnlSub.textContent = startEq > 0
      ? `约 ${pnlAmount >= 0 ? "+" : ""}${formatMoney(pnlAmount)} USDT · 总权益 ${formatMoney(totalEq)} USDT`
      : (balanceBreakdown || (totalEq > 0 ? `总权益 ${formatMoney(totalEq)} USDT` : "等待会话收益"));
  }

  $("desk-drawdown-main").textContent = formatPercentValue(drawdown);
  $("desk-drawdown-main").style.color =
    drawdown < 0 ? "var(--danger)" : drawdown > 0 ? "var(--success)" : "var(--text)";
  $("desk-drawdown-sub").textContent = `风控阈值 ${$("autoMaxDailyLossPct").value || "0"}%`;

  $("desk-order-pace").textContent = `${automation.orderCountToday ?? 0} / ${maxOrders || 0}`;
  $("desk-order-pace-sub").textContent = "今日订单数 / 日上限";

  $("desk-cycle").textContent = `${$("autoBar").value} · ${pollSeconds || "--"}s`;
  $("desk-cycle-sub").textContent = cycleDurationMs
    ? `上一轮耗时 ${cycleDurationMs} ms`
    : "等待第一轮执行完成";

  $("desk-mode").textContent = modePresentation.title;
  $("desk-mode-sub").textContent = modePresentation.sub;
  const deskModeCard = $("desk-mode-card");
  if (deskModeCard) {
    deskModeCard.classList.remove("danger", "warn", "ok", "idle");
    deskModeCard.classList.add(modePresentation.tone || "idle");
  }
  const deskModeAlert = $("desk-mode-alert");
  const deskModeAlertEyebrow = $("desk-mode-alert-eyebrow");
  const deskModeAlertMain = $("desk-mode-alert-main");
  const deskModeAlertSub = $("desk-mode-alert-sub");
  if (deskModeAlert && deskModeAlertEyebrow && deskModeAlertMain && deskModeAlertSub) {
    if (modePresentation.alert) {
      deskModeAlert.classList.remove("hidden");
      deskModeAlertEyebrow.textContent = modePresentation.alert.eyebrow || "需要处理";
      deskModeAlertMain.textContent = modePresentation.alert.main || "";
      deskModeAlertSub.textContent = modePresentation.alert.sub || "";
    } else {
      deskModeAlert.classList.add("hidden");
      deskModeAlertEyebrow.textContent = "";
      deskModeAlertMain.textContent = "";
      deskModeAlertSub.textContent = "";
    }
  }

  const blockHeight = minerNetwork.tipHeight || "--";
  const fastFee = minerFees.fastestFee;
  const minerMode = MINER_MODE_META[minerConfig.mode]?.label || "--";
  $("desk-block-height").textContent = blockHeight;
  $("desk-block-height-sub").textContent =
    Number.isFinite(Number(fastFee)) ? `最快确认 ${fastFee} sat/vB` : "等待 BTC 网络数据";
  $("desk-miner-route").textContent = minerMode;
  $("desk-miner-route-sub").textContent =
    minerMode === "--"
      ? "等待矿机概览"
      : `${formatMoney(minerDailyUsd || 0)} USDT/天 · ${minerProgress.hashrateText || "0 H/s"}`;

  if (railBalanceMain) {
    railBalanceMain.textContent = formatMoney(totalEq || 0);
  }
  if (railBalanceSub) {
    railBalanceSub.textContent = balanceBreakdown || (totalEq > 0
      ? `USDT · 调整后 ${formatMoney(Number(summary.adjEq || totalEq))}`
      : "USDT");
  }
  if (railBalanceTarget) {
    railBalanceTarget.textContent = balanceTargetEq > 0
      ? `目标 ${formatMoney(balanceTargetEq)} USDT`
      : "目标等待余额";
  }
  if (railBalanceProgress) {
    railBalanceProgress.textContent = balanceTargetEq > 0
      ? `进度 ${formatPercentValue(balanceTargetProgressPct)}`
      : "进度 0%";
  }
  if (railMinerMain) {
    railMinerMain.textContent = formatMoney(minerDailyUsd || 0);
  }
  if (railMinerSub) {
    railMinerSub.textContent = "USDT / 天";
  }

  renderStrategyPortfolio();
  renderEquityCurve(automation.equityCurve || []);
  renderDeskGuards();
}

function renderMinerSources(sources) {
  const target = $("miner-sources");
  if (!target) return;
  if (!sources || !sources.length) {
    target.className = "table-like empty";
    target.innerHTML = "等待读取本地固件仓";
    return;
  }
  target.className = "table-like";
  target.innerHTML = sources.map((source) => `
    <div class="row">
      <div><b>路线</b><span>${source.name}</span></div>
      <div><b>用途</b><span>${source.route || "-"}</span></div>
      <div><b>本地提交</b><span>${source.commit || "未克隆"}</span></div>
      <div><b>最近更新</b><span>${source.updatedAt || "-"}</span></div>
      <div><b>说明</b><span>${source.summary || source.headline || "-"}</span></div>
    </div>
  `).join("");
}

function renderMinerPool(pool, ports) {
  const target = $("miner-pool");
  if (!target) return;
  const worker = pool?.worker || "-";
  const payload = pool?.payload || {};
  const payloadText = payload && typeof payload === "object"
    ? JSON.stringify(payload).slice(0, 220)
    : "-";
  target.className = "table-like";
  target.innerHTML = `
    <div class="row">
      <div><b>Worker</b><span>${worker}</span></div>
      <div><b>状态</b><span>${pool?.statusText || "等待刷新"}</span></div>
      <div><b>Pool 返回</b><span>${payloadText || "-"}</span></div>
    </div>
  `;

  const serialTarget = $("miner-serials");
  if (!serialTarget) return;
  if (!ports || !ports.length) {
    serialTarget.className = "table-like empty";
    serialTarget.innerHTML = "当前没有检测到候选串口";
    return;
  }
  serialTarget.className = "table-like";
  serialTarget.innerHTML = ports.map((port) => `
    <div class="row">
      <div><b>串口</b><span>${port}</span></div>
      <div><b>用途</b><span>可用于 ESP32 / NerdMiner / LeafMiner 刷机或串口日志</span></div>
    </div>
  `).join("");
}

function renderMinerDevices(devices) {
  const target = $("miner-devices");
  if (!target) return;
  if (!devices || !devices.length) {
    target.className = "table-like empty";
    target.innerHTML = "还没有配置 Bitaxe 地址，或当前没有探测到在线设备";
    return;
  }
  target.className = "table-like";
  target.innerHTML = devices.map((device) => {
    const info = device.info || {};
    const asic = device.asic || {};
    const dashboard = device.dashboard || {};
    return `
      <div class="row">
        <div><b>主机</b><span>${device.host}</span></div>
        <div><b>在线</b><span>${device.reachable ? "在线" : "离线"}</span></div>
        <div><b>模型</b><span>${info.hostname || info.model || "-"}</span></div>
        <div><b>频率 / 电压</b><span>${asic.frequency || asic.freq || "-"} / ${asic.coreVoltage || asic.coreVoltageMv || "-"}</span></div>
        <div><b>算力</b><span>${dashboard.hashRate || dashboard.hashrate || dashboard.bestDiff || "-"}</span></div>
        <div><b>状态</b><span>${device.statusText || "-"}</span></div>
      </div>
    `;
  }).join("");
}

function renderMinerMacState(macLotto) {
  const target = $("miner-mac-log");
  const logs = macLotto?.logTail || [];
  $("miner-cpu-hashrate").textContent = macLotto?.hashrateText || "--";
  if (!logs.length) {
    target.className = "log-list empty";
    target.innerHTML = macLotto?.running ? "本机矿机已启动，等待日志输出" : "本机矿机尚未启动";
    return;
  }
  target.className = "log-list";
  target.innerHTML = logs
    .slice()
    .reverse()
    .map((line) => `<div class="log-item"><span>${line}</span></div>`)
    .join("");
}

function renderMinerOptions(options) {
  const target = $("miner-options");
  if (!target) return;
  if (!options || !options.length) {
    target.className = "miner-option-grid empty";
    target.innerHTML = "暂时还没拉到在线矿机方案。";
    return;
  }
  target.className = "miner-option-grid";
  target.innerHTML = options.map((item) => `
    <article class="miner-option-card">
      <div class="miner-option-head">
        <span class="pill">${item.label || "扩展方案"}</span>
        <strong>${item.name || item.id}</strong>
      </div>
      <p>${item.note || item.description || "可作为本地矿机之外的扩展路线。"}</p>
      <div class="miner-option-meta">
        <span>${item.stars ? `★ ${item.stars}` : "GitHub 方案"}</span>
        <span>${item.updatedAt ? `更新 ${item.updatedAt}` : "待联网获取"}</span>
      </div>
      <a href="${item.url}" target="_blank" rel="noreferrer">打开方案</a>
    </article>
  `).join("");
}

function renderMinerProgress(progress) {
  const badge = $("miner-progress-badge");
  $("miner-progress-headline").textContent = progress?.headline || "等待矿机概览";
  $("miner-progress-detail").textContent =
    progress?.detail || "会根据本机算力、全网难度和矿池状态解释现在到底有没有在推进。";
  $("miner-progress-hashrate").textContent = progress?.hashrateText || "--";
  $("miner-progress-hashrate-sub").textContent = progress?.hashrateSource
    ? `${progress.hashrateSource} · 全网 ${progress.networkHashrateText || "--"}`
    : "等待本机基准";
  $("miner-progress-yield").textContent = progress?.dailyBtcText || "--";
  $("miner-progress-yield-sub").textContent =
    progress?.dailyUsdText || "等待网络数据";
  $("miner-progress-eta").textContent = progress?.expectedBlockText || "--";
  $("miner-progress-eta-sub").textContent = progress?.chancePerDayText
    ? `单日命中概率 ${progress.chancePerDayText}`
    : "等待全网难度";
  $("miner-progress-ratio").textContent = progress?.bestDifficultyText || "--";
  $("miner-progress-ratio-sub").textContent = progress?.progressSubtext
    ? `${progress.progressText} · ${progress.progressSubtext}`
    : (progress?.progressText || "等待最佳难度");
  $("miner-progress-uptime").textContent = progress?.uptimeText || "--";
  $("miner-progress-uptime-sub").textContent = progress?.startedAt
    ? `启动于 ${progress.startedAt}`
    : "等待启动时间";
  $("miner-progress-pool").textContent =
    progress?.acceptedShares !== "-" || progress?.rejectedShares !== "-"
      ? `${progress.acceptedShares} / ${progress.rejectedShares}`
      : (progress?.poolReachable ? "已连接" : "未读到");
  $("miner-progress-pool-sub").textContent =
    progress?.rewardModel || "等待矿池统计";

  if ((progress?.chancePerDayPct || 0) >= 0.01) {
    badge.textContent = "有概率但仍极低";
    badge.style.color = "var(--accent)";
    badge.style.borderColor = "rgba(255, 184, 77, 0.24)";
  } else {
    badge.textContent = "乐透级概率";
    badge.style.color = "var(--accent-2)";
    badge.style.borderColor = "rgba(69, 214, 196, 0.24)";
  }
}

function updateMinerSerialSelect(ports) {
  const select = $("minerSerialPort");
  if (!select || select.tagName !== "SELECT") return;
  const current = select.value;
  const options = ['<option value="">自动识别</option>']
    .concat((ports || []).map((port) => `<option value="${port}">${port}</option>`))
    .join("");
  select.innerHTML = options;
  if (current && (ports || []).includes(current)) {
    select.value = current;
  }
}

function renderMinerOverview(overview) {
  dashboardState.miner = overview || {};
  const network = overview?.network || {};
  const fees = network.fees || {};
  const btcTicker = network.btcTicker || {};
  const config = overview?.config || {};
  const macLotto = overview?.macLotto || {};
  const progress = overview?.progress || {};
  const macStatusLabel = progress?.headline || (macLotto.running ? "Mac 乐透机运行中" : "本机矿机待接入");
  const macStatusDetail = progress?.detail || "当前以本机多 worker 聚合方式运行。";

  $("miner-btc-price").textContent = btcTicker.last ? `${formatPrice(btcTicker.last)} USDT` : "--";
  $("miner-tip-height").textContent = network.tipHeight || "--";
  $("miner-fast-fee").textContent = Number.isFinite(Number(fees.fastestFee))
    ? `${fees.fastestFee} sat/vB`
    : "--";
  $("miner-worker-count").textContent = macLotto?.effectiveWorkerCount || config?.cpuWorkers || "--";
  $("miner-theory-yield").textContent = progress?.dailyUsdText || "--";
  $("miner-theory-chance").textContent = progress?.chancePerDayText || "--";
  $("miner-plan-count").textContent = (overview?.options || []).length || "--";
  $("miner-dot").style.background = macLotto.running
    ? "var(--success)"
    : progress?.chancePerDayPct > 0
      ? "var(--accent)"
        : "#59636f";
  $("miner-status").textContent = macStatusLabel;
  $("miner-status-detail").textContent = macStatusDetail;

  renderMinerMacState(macLotto);
  renderMinerProgress(progress);
  renderMinerOptions(overview?.options || []);

  renderDeskOverview();
}

function normalizeCandleRow(row) {
  if (!row || row.length < 6) return null;
  return {
    ts: Number(row[0]),
    open: Number(row[1]),
    high: Number(row[2]),
    low: Number(row[3]),
    close: Number(row[4]),
    vol: Number(row[5] || 0),
  };
}

function renderMainstreamBoard() {
  const target = $("mainstream-board");
  const markup = MAINSTREAM_MARKETS.map((instId) => {
    const ticker = liveMarketState.tickers[instId];
    if (!ticker) {
      return `
        <div class="board-card">
          <span>${instId}</span>
          <strong>--</strong>
          <small>等待实时价格</small>
        </div>
      `;
    }
    const change = Number(ticker.open24h) > 0
      ? (Number(ticker.last) - Number(ticker.open24h)) / Number(ticker.open24h)
      : Number(ticker.sodUtc0) > 0
        ? (Number(ticker.last) - Number(ticker.sodUtc0)) / Number(ticker.sodUtc0)
        : 0;
    const directionClass = change >= 0 ? "up" : "down";
    return `
      <div class="board-card ${directionClass}">
        <span>${instId}</span>
        <strong>${formatPrice(ticker.last)}</strong>
        <small>${formatRatio(change)}</small>
      </div>
    `;
  }).join("");
  target.innerHTML = markup;
  const dockTarget = $("dock-mainstream-board");
  if (dockTarget) {
    dockTarget.innerHTML = MAINSTREAM_MARKETS.map((instId) => {
      const ticker = liveMarketState.tickers[instId];
      const shortLabel = instId.split("-")[0] || instId;
      if (!ticker) {
        return `
          <div class="board-card">
            <span>${shortLabel}</span>
            <strong>--</strong>
            <small>等待价格</small>
          </div>
        `;
      }
      const change = Number(ticker.open24h) > 0
        ? (Number(ticker.last) - Number(ticker.open24h)) / Number(ticker.open24h)
        : Number(ticker.sodUtc0) > 0
          ? (Number(ticker.last) - Number(ticker.sodUtc0)) / Number(ticker.sodUtc0)
          : 0;
      const directionClass = change >= 0 ? "up" : "down";
      return `
        <div class="board-card ${directionClass}">
          <span>${shortLabel}</span>
          <strong>${formatDockPrice(ticker.last)}</strong>
          <small>${formatRatio(change)}</small>
        </div>
      `;
    }).join("");
  }
}

function renderSelectedTickers() {
  const spot = $("spotInstId").value.trim();
  const swap = $("swapInstId").value.trim();
  const spotTicker = liveMarketState.tickers[spot];
  const swapTicker = liveMarketState.tickers[swap];
  if (spotTicker) {
    flashTicker("spotTicker", `${formatPrice(spotTicker.last)} · ${spot}`);
  }
  if (swapTicker) {
    flashTicker("swapTicker", `${formatPrice(swapTicker.last)} · ${swap}`);
  }
}

function renderMarketChart() {
  const svg = $("market-chart");
  const chartTitle = $("market-chart-title");
  const chartMeta = $("market-chart-meta");
  const spot = $("spotInstId").value.trim();
  const bar = $("marketBar").value;
  chartTitle.textContent = `${spot} 实时 K 线`;
  const candles = liveMarketState.candles.slice(-60);
  chartMeta.textContent = `${bar} · ${candles.length || 0} 根 · 实时推送`;

  if (!candles.length) {
    svg.innerHTML = `
      <rect x="0" y="0" width="960" height="320" fill="transparent"></rect>
      <text x="480" y="164" text-anchor="middle" fill="rgba(255,255,255,0.45)" font-size="18">
        等待 K 线数据
      </text>
    `;
    return;
  }

  const width = 960;
  const height = 320;
  const padX = 24;
  const padY = 20;
  const innerW = width - padX * 2;
  const innerH = height - padY * 2;
  const highs = candles.map((item) => item.high);
  const lows = candles.map((item) => item.low);
  let max = Math.max(...highs);
  let min = Math.min(...lows);
  if (!Number.isFinite(max) || !Number.isFinite(min)) {
    svg.innerHTML = "";
    return;
  }
  if (max === min) {
    max += 1;
    min -= 1;
  }

  const y = (value) => padY + ((max - value) / (max - min)) * innerH;
  const step = innerW / candles.length;
  const candleWidth = Math.max(4, step * 0.52);

  const grid = [0, 0.25, 0.5, 0.75, 1].map((ratio) => {
    const py = padY + innerH * ratio;
    return `<line x1="${padX}" y1="${py}" x2="${width - padX}" y2="${py}" stroke="rgba(255,255,255,0.05)" stroke-width="1" />`;
  }).join("");

  const candleSvg = candles.map((item, index) => {
    const cx = padX + index * step + step / 2;
    const openY = y(item.open);
    const closeY = y(item.close);
    const highY = y(item.high);
    const lowY = y(item.low);
    const top = Math.min(openY, closeY);
    const bodyHeight = Math.max(2, Math.abs(closeY - openY));
    const color = item.close >= item.open ? "#45d6c4" : "#ff6b6b";
    return `
      <line x1="${cx}" y1="${highY}" x2="${cx}" y2="${lowY}" stroke="${color}" stroke-width="1.5" />
      <rect x="${cx - candleWidth / 2}" y="${top}" width="${candleWidth}" height="${bodyHeight}" fill="${color}" rx="1.5" />
    `;
  }).join("");

  const latest = candles[candles.length - 1];
  svg.innerHTML = `
    <rect x="0" y="0" width="${width}" height="${height}" fill="transparent"></rect>
    ${grid}
    ${candleSvg}
    <text x="${width - padX}" y="18" text-anchor="end" fill="rgba(255,255,255,0.8)" font-size="16">${formatPrice(latest.close)}</text>
    <text x="${width - padX}" y="${height - 8}" text-anchor="end" fill="rgba(255,255,255,0.4)" font-size="12">High ${formatPrice(max)} · Low ${formatPrice(min)}</text>
  `;
}

async function loadHistoricalCandles() {
  return runSingleFlight("historicalCandles", async () => {
    const spot = $("spotInstId").value.trim();
    const bar = $("marketBar").value;
    const data = await request(
      `/api/market/candles?instId=${encodeURIComponent(spot)}&bar=${encodeURIComponent(bar)}&limit=120`
    );
    liveMarketState.candles = (data.candles || [])
      .map(normalizeCandleRow)
      .filter(Boolean)
      .reverse()
      .slice(-120);
    renderMarketChart();
  });
}

function closeSocket(socket) {
  if (!socket) return;
  try {
    socket.onclose = null;
    socket.close();
  } catch (_) {}
}

function clearLiveFeedTimers() {
  if (liveMarketState.tickerPing) clearInterval(liveMarketState.tickerPing);
  if (liveMarketState.candlePing) clearInterval(liveMarketState.candlePing);
  if (liveMarketState.quotePoll) clearInterval(liveMarketState.quotePoll);
  if (liveMarketState.candlePoll) clearInterval(liveMarketState.candlePoll);
  liveMarketState.tickerPing = null;
  liveMarketState.candlePing = null;
  liveMarketState.quotePoll = null;
  liveMarketState.candlePoll = null;
}

function scheduleLiveFeedRestart() {
  if (liveFeedRestartTimer) clearTimeout(liveFeedRestartTimer);
  liveFeedRestartTimer = setTimeout(() => {
    startLiveFeeds().catch((err) => {
      setLiveFeedStatus("err", "实时行情重连失败", err.message);
    });
  }, 350);
}

function restartLiveFeedSoon() {
  scheduleLiveFeedRestart();
}

function handleTickerMessage(payload) {
  if (!payload?.data?.length) return;
  payload.data.forEach((item) => {
    if (item.instId) {
      liveMarketState.tickers[item.instId] = item;
    }
  });
  renderMainstreamBoard();
  renderSelectedTickers();
}

async function pollQuotesFallback() {
  return runSingleFlight("quoteFallback", async () => {
    const spot = $("spotInstId").value.trim();
    const swap = $("swapInstId").value.trim();
    const ids = [...new Set([...MAINSTREAM_MARKETS, spot, swap])];
    const responses = await Promise.all(
      ids.map((instId) =>
        request(`/api/market/ticker?instId=${encodeURIComponent(instId)}`).catch(() => ({ ticker: [] }))
      )
    );
    responses.forEach((response) => {
      const ticker = response.ticker?.[0];
      if (ticker?.instId) {
        liveMarketState.tickers[ticker.instId] = ticker;
      }
    });
    renderMainstreamBoard();
    renderSelectedTickers();
  });
}

function startFallbackPolling() {
  if (!liveMarketState.quotePoll) {
    liveMarketState.quotePoll = setInterval(() => {
      pollQuotesFallback().catch(() => {});
    }, 8000);
  }
  if (!liveMarketState.candlePoll) {
    liveMarketState.candlePoll = setInterval(() => {
      loadHistoricalCandles().catch(() => {});
    }, 15000);
  }
}

function handleCandleMessage(payload) {
  if (!payload?.data?.length) return;
  const row = normalizeCandleRow(payload.data[0]);
  if (!row) return;
  const existing = liveMarketState.candles.findIndex((item) => item.ts === row.ts);
  if (existing >= 0) {
    liveMarketState.candles[existing] = row;
  } else {
    liveMarketState.candles.push(row);
    liveMarketState.candles = liveMarketState.candles.slice(-120);
  }
  liveMarketState.candles.sort((a, b) => a.ts - b.ts);
  renderMarketChart();
}

function connectTickerSocket() {
  const { publicWs } = deriveMarketWsEndpoints();
  const spot = $("spotInstId").value.trim();
  const swap = $("swapInstId").value.trim();
  const subscribeInsts = [...new Set([...MAINSTREAM_MARKETS, spot, swap])];
  liveMarketState.tickerReady = false;
  closeSocket(liveMarketState.tickerSocket);
  const socket = new WebSocket(publicWs);
  liveMarketState.tickerSocket = socket;
  setLiveFeedStatus("wait", "正在连接实时行情", "连接主流币和当前交易对价格流");

  socket.onopen = () => {
    socket.send(
      JSON.stringify({
        op: "subscribe",
        args: subscribeInsts.map((instId) => ({ channel: "tickers", instId })),
      })
    );
    liveMarketState.tickerReady = true;
    if (liveMarketState.tickerPing) clearInterval(liveMarketState.tickerPing);
    liveMarketState.tickerPing = setInterval(() => {
      if (socket.readyState === WebSocket.OPEN) socket.send("ping");
    }, 20000);
    setLiveFeedStatus("wait", "实时价格已连接", "正在等待 K 线流同步");
  };

  socket.onmessage = (event) => {
    let payload;
    try {
      payload = JSON.parse(event.data);
    } catch (_) {
      return;
    }
    if (payload.event === "subscribe" || payload.event === "unsubscribe") return;
    if (payload.arg?.channel === "tickers") {
      handleTickerMessage(payload);
    }
  };

  socket.onerror = () => {
    liveMarketState.tickerReady = false;
    setLiveFeedStatus("wait", "实时价格流异常", "已切到轮询更新，并将在后台继续重连 WebSocket");
  };

  socket.onclose = () => {
    liveMarketState.tickerReady = false;
    if (liveMarketState.tickerPing) clearInterval(liveMarketState.tickerPing);
    setTimeout(() => {
      if (liveMarketState.tickerSocket === socket) {
        startLiveFeeds().catch(() => {});
      }
    }, 1800);
  };
}

function connectCandleSocket() {
  const { businessWs } = deriveMarketWsEndpoints();
  const spot = $("spotInstId").value.trim();
  const bar = $("marketBar").value;
  const channel = `candle${bar}`;
  liveMarketState.candleReady = false;
  closeSocket(liveMarketState.candleSocket);
  const socket = new WebSocket(businessWs);
  liveMarketState.candleSocket = socket;

  socket.onopen = () => {
    socket.send(
      JSON.stringify({
        op: "subscribe",
        args: [{ channel, instId: spot }],
      })
    );
    liveMarketState.candleReady = true;
    if (liveMarketState.candlePing) clearInterval(liveMarketState.candlePing);
    liveMarketState.candlePing = setInterval(() => {
      if (socket.readyState === WebSocket.OPEN) socket.send("ping");
    }, 20000);
    setLiveFeedStatus("ok", "实时行情已连接", `${spot} ${bar} K 线和主流币价格正在推送`);
  };

  socket.onmessage = (event) => {
    let payload;
    try {
      payload = JSON.parse(event.data);
    } catch (_) {
      return;
    }
    if (payload.event === "subscribe" || payload.event === "unsubscribe") return;
    if (payload.arg?.channel === channel) {
      handleCandleMessage(payload);
    }
  };

  socket.onerror = () => {
    liveMarketState.candleReady = false;
    setLiveFeedStatus("wait", "实时 K 线流异常", "已切到轮询更新，并将在后台继续重连 WebSocket");
  };

  socket.onclose = () => {
    liveMarketState.candleReady = false;
    if (liveMarketState.candlePing) clearInterval(liveMarketState.candlePing);
    setTimeout(() => {
      if (liveMarketState.candleSocket === socket) {
        startLiveFeeds().catch(() => {});
      }
    }, 1800);
  };
}

async function startLiveFeeds() {
  clearLiveFeedTimers();
  closeSocket(liveMarketState.tickerSocket);
  closeSocket(liveMarketState.candleSocket);
  liveMarketState.tickerSocket = null;
  liveMarketState.candleSocket = null;
  liveMarketState.tickerReady = false;
  liveMarketState.candleReady = false;
  setLiveFeedStatus("wait", "准备实时行情", "正在同步历史 K 线");
  startFallbackPolling();
  await pollQuotesFallback().catch(() => {});
  await loadHistoricalCandles();
  connectTickerSocket();
  connectCandleSocket();
}

function findPairPreset(spot, swap) {
  return Object.entries(PAIR_PRESETS).find(([, preset]) => {
    return preset.spot === spot && preset.swap === swap;
  });
}

function parseWatchlistSymbols(raw, fallbackSpot = "", fallbackSwap = "") {
  const input = String(raw || "");
  const tokens = input.split(/[\s,;/|]+/).map((item) => item.trim().toUpperCase()).filter(Boolean);
  const symbols = [];
  for (let token of tokens) {
    if (token.includes("-")) token = token.split("-")[0];
    token = token.replace(/[^A-Z0-9]/g, "");
    if (!token || symbols.includes(token)) continue;
    symbols.push(token);
  }
  if (!symbols.length) {
    for (const instId of [fallbackSpot, fallbackSwap]) {
      const symbol = String(instId || "").trim().toUpperCase().split("-")[0];
      if (symbol && !symbols.includes(symbol)) symbols.push(symbol);
    }
  }
  return symbols.length ? symbols : ["BTC"];
}

function updateQuickState() {
  const envPreset = inferEnvPreset(
    $("envPreset").value,
    $("baseUrl").value,
    $("simulated").value === "true"
  );
  const env = ENV_PRESETS[envPreset] || ENV_PRESETS.custom;
  const spot = $("spotInstId").value.trim();
  const swap = $("swapInstId").value.trim();
  const pairMatch = findPairPreset(spot, swap);
  const watchlistSymbols = parseWatchlistSymbols($("autoWatchlistSymbols")?.value, spot, swap);

  document.querySelectorAll("[data-env-preset]").forEach((button) => {
    button.classList.toggle("active", button.dataset.envPreset === envPreset);
  });

  document.querySelectorAll("[data-pair-preset]").forEach((button) => {
    button.classList.toggle("active", button.dataset.pairPreset === pairMatch?.[0]);
  });

  $("active-env-label").textContent =
    envPreset === "custom"
      ? `自定义 · ${$("baseUrl").value.trim() || env.baseUrl}`
      : env.label;

  $("active-pair-label").textContent = watchlistSymbols.length > 1
    ? `${watchlistSymbols.join(" / ")} · ${watchlistSymbols.length} 币并行`
    : (pairMatch
      ? `${pairMatch[1].spot} / ${pairMatch[1].swap}`
      : `${spot || "-"} / ${swap || "-"}`);

  if ($("order-env-label")) {
    $("order-env-label").textContent = env.label;
  }
  if ($("order-env-sub")) {
    $("order-env-sub").textContent = $("simulated").value === "true"
      ? "模拟链路会带 x-simulated-trading: 1"
      : "实盘链路会走真实私有 WS";
  }
  if ($("order-spot-label")) {
    $("order-spot-label").textContent = spot || "--";
  }
  if ($("order-swap-label")) {
    $("order-swap-label").textContent = swap || "--";
  }
  if ($("order-watchlist-label")) {
    $("order-watchlist-label").textContent = watchlistSymbols.length > 1
      ? `${watchlistSymbols.join(" / ")} · ${watchlistSymbols.length} 币`
      : (watchlistSymbols[0] || "BTC");
  }
  if ($("orderSpotInstMirror")) {
    $("orderSpotInstMirror").value = spot || "";
  }
  if ($("orderSwapInstMirror")) {
    $("orderSwapInstMirror").value = swap || "";
  }

  syncConfigActionState();
  renderRailStrategyControls();
  renderStrategyPortfolio();
}

function isConfigDirty() {
  if (!$("envPreset")) return false;
  const draft = collectConfig();
  const current = dashboardState.config || {};
  if (draft.apiKey || draft.secretKey || draft.passphrase || draft.remoteGatewayToken) {
    return true;
  }
  return (
    String(current.envPreset || "") !== String(draft.envPreset || "") ||
    String(current.baseUrl || "") !== String(draft.baseUrl || "") ||
    Boolean(current.simulated) !== Boolean(draft.simulated) ||
    String(current.executionMode || "") !== String(draft.executionMode || "") ||
    String(current.remoteGatewayUrl || "") !== String(draft.remoteGatewayUrl || "")
  );
}

function setButtonBusy(id, busy, idleText, busyText) {
  const button = $(id);
  if (!button) return;
  button.textContent = busy ? busyText : idleText;
  button.disabled = busy;
}

function setConfigControlsBusy(busy) {
  [
    "envPreset",
    "baseUrl",
    "simulated",
    "executionMode",
    "remoteGatewayUrl",
    "apiKey",
    "secretKey",
    "passphrase",
    "remoteGatewayToken",
    "persist",
  ].forEach((id) => {
    const node = $(id);
    if (node) node.disabled = busy;
  });
  document.querySelectorAll("[data-env-preset]").forEach((button) => {
    button.disabled = busy;
  });
}

function syncConfigActionState() {
  const saving = Boolean(dashboardState.configSaving);
  const testing = Boolean(dashboardState.configTesting);
  const busy = saving || testing;
  const dirty = !busy && isConfigDirty();
  const envPreset = $("envPreset")
    ? inferEnvPreset($("envPreset").value, $("baseUrl").value, $("simulated").value === "true")
    : "custom";
  const env = ENV_PRESETS[envPreset] || ENV_PRESETS.custom;
  const hint = $("active-env-state");
  if (hint) {
    if (saving) {
      hint.textContent = "正在切换并等待远端确认";
    } else if (testing) {
      hint.textContent = "正在校验当前环境链路";
    } else if (dirty) {
      hint.textContent = "已修改，保存后才会真正生效";
    } else if (dashboardState.routeHealth?.healthy) {
      hint.textContent = `${env.label} 已生效`;
    } else {
      hint.textContent = "当前环境已加载";
    }
  }
  setButtonBusy("save-config", saving, dirty ? "保存并生效" : "保存配置", "切换中...");
  setButtonBusy("test-config", testing, "测试连接", "校验中...");
  setConfigControlsBusy(busy);
}

function applyEnvironmentPreset(presetKey, { keepBaseUrl = false } = {}) {
  const preset = ENV_PRESETS[presetKey] || ENV_PRESETS.custom;
  const isCustom = presetKey === "custom";

  $("simulated").value = String(Boolean(preset.simulated));
  $("simulated").disabled = !isCustom;
  $("baseUrl").readOnly = !isCustom;

  if (!keepBaseUrl || !$("baseUrl").value.trim()) {
    $("baseUrl").value = preset.baseUrl;
  }

  updateEndpointCards(presetKey);
  setMessage(preset.notice);
  updateQuickState();
  renderDeskGuards();
}

function setPairPreset(presetKey, { refresh = false } = {}) {
  const preset = PAIR_PRESETS[presetKey];
  if (!preset) return;
  $("spotInstId").value = preset.spot;
  $("swapInstId").value = preset.swap;
  if ($("autoWatchlistSymbols")) {
    $("autoWatchlistSymbols").value = preset.spot.split("-")[0];
  }
  updateQuickState();
  if (refresh) {
    refreshMarket().catch(() => {});
  }
}

function collectConfig() {
  return {
    envPreset: $("envPreset").value,
    apiKey: $("apiKey").value.trim(),
    secretKey: $("secretKey").value.trim(),
    passphrase: $("passphrase").value.trim(),
    baseUrl: $("baseUrl").value.trim(),
    simulated: $("simulated").value === "true",
    executionMode: $("executionMode").value,
    remoteGatewayUrl: $("remoteGatewayUrl").value.trim(),
    remoteGatewayToken: $("remoteGatewayToken").value.trim(),
    persist: $("persist").checked,
  };
}

function collectPublicConfigForAnalysis() {
  return {
    envPreset: $("envPreset").value,
    baseUrl: $("baseUrl").value.trim(),
    simulated: $("simulated").value === "true",
  };
}

function collectAutomationConfig() {
  syncWatchlistOverridesValueFromEditor();
  const strategyPreset = ONLY_STRATEGY_PRESET;
  return {
    strategyPreset,
    spotInstId: $("spotInstId").value.trim(),
    swapInstId: $("swapInstId").value.trim(),
    watchlistSymbols: $("autoWatchlistSymbols").value.trim(),
    watchlistOverrides: $("autoWatchlistOverrides").value.trim(),
    bar: $("autoBar").value,
    fastEma: Number($("autoFastEma").value || 9),
    slowEma: Number($("autoSlowEma").value || 21),
    pollSeconds: Number($("autoPollSeconds").value || 20),
    cooldownSeconds: Number($("autoCooldownSeconds").value || 180),
    maxOrdersPerDay: Number($("autoMaxOrdersPerDay").value || 20),
    spotEnabled: $("autoSpotEnabled").checked,
    spotQuoteBudget: $("autoSpotQuoteBudget").value.trim(),
    spotMaxExposure: $("autoSpotMaxExposure").value.trim(),
    swapEnabled: $("autoSwapEnabled").checked,
    swapContracts: $("autoSwapContracts").value.trim(),
    swapTdMode: $("autoSwapTdMode").value,
    swapStrategyMode: $("autoSwapStrategyMode").value,
    swapLeverage: $("autoSwapLeverage").value.trim(),
    stopLossPct: $("autoStopLossPct").value.trim(),
    takeProfitPct: $("autoTakeProfitPct").value.trim(),
    maxDailyLossPct: $("autoMaxDailyLossPct").value.trim(),
    targetBalanceMultiple: "100",
    autostart: $("autoAutostart").checked,
    allowLiveManualOrders: $("autoAllowLiveManualOrders").checked,
    allowLiveTrading: $("autoAllowLiveTrading").checked,
    allowLiveAutostart: $("autoAllowLiveAutostart").checked,
    enforceNetMode: $("autoEnforceNetMode").checked,
  };
}

function collectResearchOptions() {
  return {
    historyLimit: Number($("researchHistoryLimit").value || 240),
    raceSize: Number($("researchRaceSize").value || 10),
    evolutionLoops: Number($("researchEvolutionLoops").value || 4),
    optimizationDepth: $("researchDepth").value,
    includeAltBars: $("researchIncludeAltBars").checked,
    enableHybrid: $("researchEnableHybrid").checked,
    enableFineTune: $("researchEnableFineTune").checked,
  };
}

function collectMinerConfig() {
  return {
    mode: "mac_lotto",
    wallet: $("minerWallet").value.trim(),
    workerName: $("minerWorkerName").value.trim(),
    poolHost: $("minerPoolHost").value.trim(),
    poolPort: Number($("minerPoolPort").value || 0),
    cpuWorkers: Number($("minerCpuWorkers").value || 1),
    poolPassword: $("minerPoolPassword").value.trim(),
    poolApiBase: $("minerPoolApiBase").value.trim(),
    bitaxeHosts: $("minerBitaxeHosts").value.trim(),
    serialPort: $("minerSerialPort").value,
    boardType: $("minerBoardType").value.trim(),
    refreshSeconds: Number($("minerRefreshSeconds").value || 20),
  };
}

function toggleArbitrageConfigVisibility(presetKey = $("autoStrategyPreset")?.value || ONLY_STRATEGY_PRESET) {
  return presetKey;
}

function setStrategyPresetUi(presetKey) {
  const lockedPresetKey = ONLY_STRATEGY_PRESET;
  const preset = STRATEGY_PRESETS[lockedPresetKey];
  $("autoStrategyPreset").value = lockedPresetKey;
  $("active-strategy-label").textContent = preset.label;
  $("active-strategy-description").textContent = preset.description;
  document.querySelectorAll("[data-strategy-preset]").forEach((button) => {
    button.classList.toggle("active", button.dataset.strategyPreset === lockedPresetKey);
  });
  toggleArbitrageConfigVisibility(lockedPresetKey);
}

function fillAutomationForm(config) {
  $("autoStrategyPreset").value = ONLY_STRATEGY_PRESET;
  $("spotInstId").value = config.spotInstId || "BTC-USDT";
  $("swapInstId").value = config.swapInstId || "BTC-USDT-SWAP";
  $("autoWatchlistSymbols").value = config.watchlistSymbols || (config.spotInstId || "BTC-USDT").split("-")[0];
  $("autoWatchlistOverrides").value = serializeWatchlistOverrides(
    config.watchlistOverrides,
    parseWatchlistSymbols(config.watchlistSymbols, config.spotInstId || "BTC-USDT", config.swapInstId || "BTC-USDT-SWAP")
  );
  renderWatchlistOverrideEditor($("autoWatchlistOverrides").value);
  $("autoBar").value = config.bar || "15m";
  $("autoFastEma").value = config.fastEma ?? 12;
  $("autoSlowEma").value = config.slowEma ?? 48;
  $("autoPollSeconds").value = config.pollSeconds ?? 8;
  $("autoCooldownSeconds").value = config.cooldownSeconds ?? 45;
  $("autoMaxOrdersPerDay").value = config.maxOrdersPerDay ?? 0;
  $("autoSpotEnabled").checked = false;
  $("autoSpotQuoteBudget").value = "0";
  $("autoSpotMaxExposure").value = "0";
  $("autoSwapEnabled").checked = true;
  $("autoSwapContracts").value = config.swapContracts ?? "1";
  $("autoSwapTdMode").value = "isolated";
  $("autoSwapStrategyMode").value = "long_only";
  $("autoSwapLeverage").value = config.swapLeverage ?? "10";
  $("autoStopLossPct").value = config.stopLossPct ?? "1.2";
  $("autoTakeProfitPct").value = config.takeProfitPct ?? "8";
  $("autoMaxDailyLossPct").value = config.maxDailyLossPct ?? "0.8";
  $("autoAutostart").checked = Boolean(config.autostart);
  $("autoAllowLiveManualOrders").checked = Boolean(config.allowLiveManualOrders);
  $("autoAllowLiveTrading").checked = Boolean(config.allowLiveTrading);
  $("autoAllowLiveAutostart").checked = Boolean(config.allowLiveAutostart);
  $("autoEnforceNetMode").checked = config.enforceNetMode !== false;
  setStrategyPresetUi(ONLY_STRATEGY_PRESET);
  syncRailAutomationToggles();
  updateQuickState();
  renderDeskGuards();
  renderStrategyPortfolio();
}

function fillMinerForm(config) {
  $("minerMode").value = "mac_lotto";
  $("minerWallet").value = config.wallet || "";
  $("minerWorkerName").value = config.workerName || "desk";
  $("minerPoolHost").value = config.poolHost || "solo.ckpool.org";
  $("minerPoolPort").value = config.poolPort ?? 3333;
  $("minerCpuWorkers").value = config.cpuWorkers ?? 8;
  $("minerPoolPassword").value = config.poolPassword || "x";
  $("minerPoolApiBase").value = config.poolApiBase || "";
  $("minerBitaxeHosts").value = config.bitaxeHosts || "";
  $("minerBoardType").value = config.boardType || "Mac 本机 CPU 集群";
  $("minerRefreshSeconds").value = config.refreshSeconds ?? 20;
  if (config.bitaxeHosts && !$("minerBitaxeActionHost").value.trim()) {
    $("minerBitaxeActionHost").value = String(config.bitaxeHosts).split(",")[0].trim();
  }
}

function applyStrategyPreset(presetKey, { announce = true, refresh = false } = {}) {
  const preset = STRATEGY_PRESETS[ONLY_STRATEGY_PRESET];
  if (!preset) return;
  fillAutomationForm({ ...collectAutomationConfig(), ...preset.config });
  if (announce) {
    setAutomationMessage(`已载入 ${preset.label} 预设。`, "ok");
  }
  if (refresh) {
    refreshMarket().catch(() => {});
  }
}

function renderRows(targetId, items, fields) {
  const target = $(targetId);
  if (!items || !items.length) {
    target.innerHTML = '<div class="empty">暂无数据</div>';
    return;
  }
  target.innerHTML = items
    .map((item) => {
      const cols = fields
        .map((field) => {
          const value = item[field.key] ?? "-";
          return `<div><b>${field.label}</b><span>${value}</span></div>`;
        })
        .join("");
      return `<div class="row">${cols}</div>`;
    })
    .join("");
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function getOrderKey(order) {
  return order?.ordId || order?.clOrdId || [order?.instId, order?.side, order?.cTime, order?.px, order?.sz].filter(Boolean).join("-");
}

function getOrderTone(state) {
  if (["filled"].includes(state)) return "tone-done";
  if (["canceled", "mmp_canceled"].includes(state)) return "tone-cancel";
  if (["order_failed", "failed"].includes(state)) return "tone-fail";
  return "tone-live";
}

function getOrderStateLabel(state) {
  const labels = {
    live: "工作中",
    effective: "已提交",
    partially_filled: "部分成交",
    filled: "已成交",
    canceled: "已撤",
    mmp_canceled: "风控撤单",
    order_failed: "失败",
    failed: "失败",
  };
  return labels[state] || state || "未知";
}

function getOrderSideLabel(order) {
  const side = order?.side === "sell" ? "卖出" : "买入";
  const posSide = order?.posSide && order.posSide !== "net" ? ` · ${order.posSide}` : "";
  const tdMode = order?.tdMode ? ` · ${order.tdMode}` : "";
  return `${side}${posSide}${tdMode}`;
}

function getOrderArbPhase(order) {
  const action = String(order?.strategyAction || "").trim().toLowerCase();
  if (["entry", "hedge", "exit", "cover", "rollback"].includes(action)) {
    return action === "cover" ? "exit" : action;
  }
  const tag = String(order?.strategyTag || order?.tag || "").trim().toLowerCase();
  if (!tag.startsWith("arb_")) return "";
  if (tag.includes("entry")) return "entry";
  if (tag.includes("hedge")) return "hedge";
  if (tag.includes("cover") || tag.includes("exit")) return "exit";
  if (tag.includes("rollback") || tag.endsWith("_rb")) return "rollback";
  return "arb";
}

function getOrderArbPhaseLabel(phase) {
  const labels = {
    entry: "现货开腿",
    hedge: "永续对冲",
    exit: "套利退场",
    rollback: "回滚处理",
    arb: "套利动作",
  };
  return labels[phase] || "普通订单";
}

function formatOrderTime(value) {
  const num = Number(value);
  if (!Number.isFinite(num) || num <= 0) return "--";
  return new Date(num).toLocaleString("zh-CN", {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });
}

function formatOrderValue(value) {
  if (value === undefined || value === null || value === "") return "--";
  const num = Number(value);
  if (!Number.isFinite(num)) return String(value);
  if (Math.abs(num) >= 1000) return num.toLocaleString("en-US", { maximumFractionDigits: 2 });
  if (Math.abs(num) >= 1) return num.toLocaleString("en-US", { maximumFractionDigits: 4 });
  return num.toLocaleString("en-US", { maximumFractionDigits: 6 });
}

function getOrderSymbol(order) {
  return String(order?.instId || "").trim().toUpperCase().split("-")[0] || "UNKNOWN";
}

function toOrderNumber(value) {
  const num = Number(value);
  return Number.isFinite(num) ? num : 0;
}

function isSwapOrder(order) {
  return String(order?.instType || "").toUpperCase() === "SWAP" || String(order?.instId || "").endsWith("-SWAP");
}

function getOrderCurrentPrice(order, account = dashboardState.account || {}) {
  const instId = String(order?.instId || "");
  const tickerPrice = toOrderNumber(liveMarketState.tickers?.[instId]?.last);
  if (tickerPrice > 0) return tickerPrice;

  if (isSwapOrder(order)) {
    const position = (account.positions || []).find((item) => item.instId === instId);
    const markPx = toOrderNumber(position?.markPx || position?.last || position?.lastPx);
    if (markPx > 0) return markPx;
    return 0;
  }

  const baseCcy = instId.split("-")[0] || "";
  const balanceRows = [...(account.tradingBalances || []), ...(account.balances || [])];
  const baseBalance = balanceRows.find((row) => String(row.ccy || "").toUpperCase() === baseCcy.toUpperCase());
  const baseQty = toOrderNumber(baseBalance?.eq || baseBalance?.cashBal || baseBalance?.availBal);
  const baseUsd = toOrderNumber(baseBalance?.eqUsd || baseBalance?.usdEq);
  if (baseQty > 0 && baseUsd > 0) {
    return baseUsd / baseQty;
  }
  return 0;
}

function getSpotOrderHolding(order, account = dashboardState.account || {}) {
  const instId = String(order?.instId || "");
  const baseCcy = instId.split("-")[0] || "";
  if (!baseCcy) {
    return { baseCcy: "", quantity: 0 };
  }
  const balanceRows = [...(account.tradingBalances || []), ...(account.balances || [])];
  const baseBalance = balanceRows.find((row) => String(row.ccy || "").toUpperCase() === baseCcy.toUpperCase());
  return {
    baseCcy,
    quantity: toOrderNumber(baseBalance?.eq || baseBalance?.cashBal || baseBalance?.availBal),
  };
}

function buildOrderLifecycleContext(order, meta = {}) {
  const account = dashboardState.account || {};
  const state = String(order?.state || "");
  const filledSize = toOrderNumber(order?.accFillSz || order?.fillSz || order?.sz);
  const side = String(order?.side || "").toLowerCase();
  const hasExplicitPnl = ["realizedPnl", "fillPnl", "pnl"].some(
    (key) => order?.[key] !== undefined && order?.[key] !== null && order?.[key] !== ""
  );

  if (state === "partially_filled" && filledSize > 0) {
    return {
      title: "当前状态",
      valueText: "部分成交",
      toneClass: "pnl-muted",
      scopeText: "订单还没完全成交完",
      detailText: "这笔单已经成交了一部分，但还没有完整结束，所以仓位和最终收益都还在变化。",
      noteText: "等它完全成交、撤单或结束后，这里会再明确告诉你现在到底是持仓中还是已离场。",
    };
  }

  if (state !== "filled" || filledSize <= 0) {
    return {
      title: "当前状态",
      valueText: "待成交",
      toneClass: "pnl-muted",
      scopeText: "订单还没形成最终仓位",
      detailText: "这笔单还没有完成成交，所以现在谈不上持仓还是已卖出/已平仓。",
      noteText: "等成交后，这里会直接告诉你当前还有没有同标的仓位。",
    };
  }

  if (isSwapOrder(order)) {
    const position = (account.positions || []).find(
      (item) => item.instId === order.instId && Math.abs(toOrderNumber(item.pos)) > 0
    );
    if (position) {
      const pos = toOrderNumber(position.pos);
      return {
        title: "当前状态",
        valueText: "持仓中",
        toneClass: "pnl-positive",
        scopeText: `同合约当前仍有 ${formatOrderValue(pos)} 张仓位`,
        detailText: "这笔合约单成交后，当前同合约还有活跃仓位，所以它不是已卖出/已平仓。",
        noteText: "收益金额优先看下面的持仓浮动；那代表当前整笔仓位还没锁定的盈亏。",
      };
    }
    return {
      title: "当前状态",
      valueText: hasExplicitPnl ? "已平仓" : "当前无仓",
      toneClass: "pnl-muted",
      scopeText: "当前同合约没有活跃仓位",
      detailText: hasExplicitPnl
        ? "这笔合约单已经离场，交易所也回了已实现收益。"
        : "这笔合约单已经成交，但当前同合约没有仓位；通常说明已经平仓或离场了。",
      noteText: meta?.source === "local_cache"
        ? "这是本地恢复出来的订单记录，仓位判断以当前账户快照为准。"
        : "如果你想确认最终赚亏多少，看交易所回报里的已实现收益；如果没有回，就只能确认它现在已经不在持仓里了。",
    };
  }

  const holding = getSpotOrderHolding(order, account);
  if (side === "buy") {
    if (holding.quantity > 0) {
      return {
        title: "当前状态",
        valueText: "持币中",
        toneClass: "pnl-positive",
        scopeText: `当前账户约有 ${formatOrderValue(holding.quantity)} ${holding.baseCcy || ""}`.trim(),
        detailText: "这笔现货买单成交后，当前账户里还有这枚币，说明它还没有全部卖掉。",
        noteText: "收益金额会按当前现价估算，这还是浮动收益，不是最终已实现收益。",
      };
    }
    return {
      title: "当前状态",
      valueText: "当前无币",
      toneClass: "pnl-muted",
      scopeText: `当前账户没有明显 ${holding.baseCcy || "该币"} 持仓`,
      detailText: "这笔现货买单成交后，目前账户里已经看不到这枚币；通常说明后来已经卖出或转走。",
      noteText: "如果你要精确已实现收益，仍然需要这笔买入对应的卖出成本口径。",
    };
  }

  if (side === "sell") {
    return {
      title: "当前状态",
      valueText: holding.quantity > 0 ? "已卖出 · 仍有余仓" : "已卖出",
      toneClass: "pnl-muted",
      scopeText: holding.quantity > 0
        ? `卖出后当前账户还剩 ${formatOrderValue(holding.quantity)} ${holding.baseCcy || ""}`.trim()
        : "卖出后当前账户已无这枚币",
      detailText: holding.quantity > 0
        ? "这笔卖单已经成交，但账户里还有同币余仓，所以它不是彻底清仓。"
        : "这笔卖单已经成交，而且当前账户里没有这枚币，说明它已经卖出了。",
      noteText: "单看卖单仍然不能直接算最终收益，需要结合对应买入成本。",
    };
  }

  return {
    title: "当前状态",
    valueText: "待判断",
    toneClass: "pnl-muted",
    scopeText: "当前口径不足",
    detailText: "这笔订单已经成交，但当前上下文还不足以判断它现在到底是持仓还是已离场。",
    noteText: "等账户持仓或交易所成交回报更完整后，这里会自动变得更明确。",
  };
}

function buildOrderPnlContext(order, meta = {}) {
  const account = dashboardState.account || {};
  const fillPrice = toOrderNumber(order?.avgPx || order?.fillPx || order?.px);
  const filledSize = toOrderNumber(order?.accFillSz || order?.fillSz || order?.sz);
  const currentPrice = getOrderCurrentPrice(order, account);
  const explicitPnlFields = ["realizedPnl", "fillPnl", "pnl"];

  for (const key of explicitPnlFields) {
    if (order?.[key] !== undefined && order?.[key] !== null && order?.[key] !== "") {
      const explicitPnl = toOrderNumber(order[key]);
      return {
        title: explicitPnl >= 0 ? "已实现收益" : "已实现亏损",
        valueText: `${explicitPnl >= 0 ? "+" : ""}${formatMoney(explicitPnl)}`,
        toneClass: explicitPnl > 0 ? "pnl-positive" : explicitPnl < 0 ? "pnl-negative" : "pnl-muted",
        scopeText: "交易所成交回报",
        currentPriceText: currentPrice > 0 ? formatOrderValue(currentPrice) : "--",
        detailText: "这已经是交易所返回的已实现收益，不是前端估算。",
        noteText: "如果订单还关联着未平仓持仓，请再结合持仓未实现收益一起看。",
      };
    }
  }

  if (!["filled", "partially_filled"].includes(String(order?.state || "")) || filledSize <= 0) {
    return {
      title: "收益状态",
      valueText: "待成交",
      toneClass: "pnl-muted",
      scopeText: "尚未形成可算收益",
      currentPriceText: currentPrice > 0 ? formatOrderValue(currentPrice) : "--",
      detailText: "订单还没完全成交前，不会形成明确收益。",
      noteText: "等成交后，这里会自动切成浮动收益、相关持仓收益或已实现收益。",
    };
  }

  if (isSwapOrder(order)) {
    const position = (account.positions || []).find(
      (item) => item.instId === order.instId && Math.abs(toOrderNumber(item.pos)) > 0
    );
    if (position && position.upl !== undefined && position.upl !== null && position.upl !== "") {
      const upl = toOrderNumber(position.upl);
      return {
        title: upl >= 0 ? "相关持仓浮盈" : "相关持仓浮亏",
        valueText: `${upl >= 0 ? "+" : ""}${formatMoney(upl)}`,
        toneClass: upl > 0 ? "pnl-positive" : upl < 0 ? "pnl-negative" : "pnl-muted",
        scopeText: "同合约当前持仓未实现收益",
        currentPriceText: currentPrice > 0 ? formatOrderValue(currentPrice) : "--",
        detailText: `当前持仓 ${formatOrderValue(position.pos)}，这是整笔持仓的浮动收益，不是单笔订单精确归因。`,
        noteText: "合约单的真实已实现收益要等平仓回报；没平仓前更适合看当前持仓的未实现收益。",
      };
    }
    return {
      title: "收益金额",
      valueText: "待回报",
      toneClass: "pnl-muted",
      scopeText: "当前无同合约持仓",
      currentPriceText: currentPrice > 0 ? formatOrderValue(currentPrice) : "--",
      detailText: "这是一笔合约成交，当前同合约已经没有持仓；通常说明它已经平仓/离场，但交易所还没把已实现收益直接回到这笔订单里。",
      noteText: meta?.source === "local_cache"
        ? "当前是本地恢复出来的订单记录，优先把它当成成交痕迹；现在能确定的是当前已经没有同合约仓位。"
        : "如果交易所稍后回了 realized PnL，这里会直接变成已实现收益；在那之前，只能先明确它现在已经不在持仓里。",
    };
  }

  if (String(order?.side || "").toLowerCase() === "buy" && fillPrice > 0 && filledSize > 0) {
    if (currentPrice > 0) {
      const pnl = (currentPrice - fillPrice) * filledSize;
      const pnlPct = ((currentPrice - fillPrice) / fillPrice) * 100;
      return {
        title: pnl >= 0 ? "当前浮盈" : "当前浮亏",
        valueText: `${pnl >= 0 ? "+" : ""}${formatMoney(pnl)}`,
        toneClass: pnl > 0 ? "pnl-positive" : pnl < 0 ? "pnl-negative" : "pnl-muted",
        scopeText: "按当前币价估算",
        currentPriceText: formatOrderValue(currentPrice),
        detailText: `现价相对成交均价 ${formatPercentValue(pnlPct)}，按本笔已成交数量估算。`,
        noteText: "这还是浮动收益，不是已实现收益。只有真正卖出后，收益才会锁定。",
      };
    }
    return {
      title: "收益金额",
      valueText: "待现价",
      toneClass: "pnl-muted",
      scopeText: "等待现价或账户快照",
      currentPriceText: "--",
      detailText: "当前还拿不到这笔现货的现价，所以暂时不能估算浮动收益。",
      noteText: "等行情或账户快照刷新后，这里会自动补出当前浮盈/浮亏。",
    };
  }

  if (String(order?.side || "").toLowerCase() === "sell") {
    const driftText = currentPrice > 0 && fillPrice > 0
      ? `卖出后现价 ${formatOrderValue(currentPrice)}，相对卖出价 ${formatPercentValue(((currentPrice - fillPrice) / fillPrice) * 100)}。`
      : "当前拿不到现价，无法显示卖出后的价格偏移。";
    return {
      title: "收益金额",
      valueText: "待成本口径",
      toneClass: "pnl-muted",
      scopeText: "仅凭卖单无法直接算已实现收益",
      currentPriceText: currentPrice > 0 ? formatOrderValue(currentPrice) : "--",
      detailText: "真实已实现收益必须结合你的买入成本或持仓均价，单看这笔卖单还不够。",
      noteText: driftText,
    };
  }

  return {
    title: "收益金额",
    valueText: "待判断",
    toneClass: "pnl-muted",
    scopeText: "当前口径不足",
    currentPriceText: currentPrice > 0 ? formatOrderValue(currentPrice) : "--",
    detailText: "这笔订单还没有足够上下文去判断收益。",
    noteText: "如果你希望看到精确已实现收益，需要这笔单的平仓回报或完整成本口径。",
  };
}

function estimateOrderPnl(order, meta = {}) {
  const account = dashboardState.account || {};
  const fillPrice = toOrderNumber(order?.avgPx || order?.fillPx || order?.px);
  const filledSize = toOrderNumber(order?.accFillSz || order?.fillSz || order?.sz);
  const currentPrice = getOrderCurrentPrice(order, account);
  const explicitPnlFields = ["realizedPnl", "fillPnl", "pnl"];

  for (const key of explicitPnlFields) {
    if (order?.[key] !== undefined && order?.[key] !== null && order?.[key] !== "") {
      return {
        value: toOrderNumber(order[key]),
        scope: "已实现",
        quality: "exact",
      };
    }
  }

  if (!["filled", "partially_filled"].includes(String(order?.state || "")) || filledSize <= 0) {
    return { value: null, scope: "待成交", quality: "pending" };
  }

  if (isSwapOrder(order)) {
    const position = (account.positions || []).find(
      (item) => item.instId === order.instId && Math.abs(toOrderNumber(item.pos)) > 0
    );
    if (position && position.upl !== undefined && position.upl !== null && position.upl !== "") {
      return {
        value: toOrderNumber(position.upl),
        scope: "相关持仓浮动",
        quality: "position",
      };
    }
    return { value: null, scope: "当前无仓，待交易所回报", quality: "unknown" };
  }

  if (String(order?.side || "").toLowerCase() === "buy" && fillPrice > 0 && filledSize > 0 && currentPrice > 0) {
    return {
      value: (currentPrice - fillPrice) * filledSize,
      scope: "现价估算",
      quality: "estimated",
    };
  }

  return { value: null, scope: "待成本口径", quality: "unknown" };
}

function groupOrdersBySymbol(orders, meta = {}) {
  const groups = new Map();
  (orders || []).forEach((order) => {
    const symbol = getOrderSymbol(order);
    if (!groups.has(symbol)) {
      groups.set(symbol, []);
    }
    groups.get(symbol).push(order);
  });

  return Array.from(groups.entries()).map(([symbol, items]) => {
    const executionStats = collectOrderExecutionStats(items);
    const arbCounts = {
      total: 0,
      entry: 0,
      hedge: 0,
      exit: 0,
      rollback: 0,
    };
    items.forEach((item) => {
      const phase = getOrderArbPhase(item);
      if (!phase) return;
      arbCounts.total += 1;
      if (phase === "entry") arbCounts.entry += 1;
      else if (phase === "hedge") arbCounts.hedge += 1;
      else if (phase === "exit") arbCounts.exit += 1;
      else if (phase === "rollback") arbCounts.rollback += 1;
    });
    const arbRealizedPnl = items.reduce((sum, item) => {
      if (!getOrderArbPhase(item)) return sum;
      return sum + toOrderNumber(item?.pnl || item?.realizedPnl || item?.fillPnl);
    }, 0);
    const arbTotalFees = items.reduce((sum, item) => {
      if (!getOrderArbPhase(item)) return sum;
      return sum + toOrderNumber(item?.fee);
    }, 0);
    const estimates = items.map((item) => estimateOrderPnl(item, meta));
    const numeric = estimates.filter((item) => Number.isFinite(item.value));
    const realizedPnl = estimates
      .filter((item) => item.quality === "exact" && Number.isFinite(item.value))
      .reduce((sum, item) => sum + Number(item.value || 0), 0);
    const positionBuckets = new Map();
    items.forEach((item, index) => {
      const estimate = estimates[index];
      if (!estimate || estimate.quality !== "position" || !Number.isFinite(estimate.value)) return;
      const key = item.instId || `${symbol}-SWAP`;
      const stamp = Number(item.uTime || item.cTime || 0);
      const previous = positionBuckets.get(key);
      if (!previous || stamp >= previous.stamp) {
        positionBuckets.set(key, { value: Number(estimate.value || 0), stamp });
      }
    });
    const positionPnl = Array.from(positionBuckets.values()).reduce((sum, item) => sum + Number(item.value || 0), 0);
    const estimatedPnl = estimates
      .filter((item) => item.quality === "estimated" && Number.isFinite(item.value))
      .reduce((sum, item) => sum + Number(item.value || 0), 0);
    const pnlTotal = realizedPnl + positionPnl + estimatedPnl;
    const pendingCount = estimates.filter((item) => item.quality === "pending").length;
    const unresolvedCount = estimates.filter((item) => item.quality === "unknown").length;
    const latestItem = items.slice().sort((a, b) => Number(b.uTime || b.cTime || 0) - Number(a.uTime || a.cTime || 0))[0];
    const hasExact = numeric.some((item) => item.quality === "exact");
    const hasPosition = positionBuckets.size > 0;
    const hasEstimate = numeric.some((item) => item.quality !== "exact");
    const scopeParts = [];
    if (hasExact) scopeParts.push("已实现");
    if (hasPosition) scopeParts.push("持仓浮动");
    if (numeric.some((item) => item.quality === "estimated")) scopeParts.push("现价估算");
    const scopeLabel = scopeParts.length ? `含${scopeParts.join(" / ")}` : "收益待更多成本口径";
    return {
      symbol,
      orders: items.slice().sort((a, b) => Number(b.uTime || b.cTime || 0) - Number(a.uTime || a.cTime || 0)),
      working: executionStats.working,
      filled: executionStats.filled,
      riskCount: executionStats.risk,
      successRate: executionStats.successRate,
      topCancelReason: executionStats.topCancelReason,
      latestCancelReason: executionStats.latestCancelReason,
      arbOrderCount: arbCounts.total,
      arbEntryOrders: arbCounts.entry,
      arbHedgeOrders: arbCounts.hedge,
      arbExitOrders: arbCounts.exit,
      arbRollbackOrders: arbCounts.rollback,
      arbRealizedPnl,
      arbTotalFees,
      arbNetPnl: arbRealizedPnl + arbTotalFees,
      orderCount: items.length,
      pnlTotal,
      realizedPnl,
      positionPnl,
      estimatedPnl,
      pendingCount,
      unresolvedCount,
      hasPnl: numeric.length > 0,
      scopeLabel,
      latestAt: formatOrderTime(latestItem?.uTime || latestItem?.cTime),
      latestInstId: latestItem?.instId || `${symbol}-USDT`,
    };
  }).sort((a, b) => {
    const aLatest = Number(a.orders[0]?.uTime || a.orders[0]?.cTime || 0);
    const bLatest = Number(b.orders[0]?.uTime || b.orders[0]?.cTime || 0);
    return bLatest - aLatest;
  });
}

function syncOrderContextToSymbol(symbol) {
  if (!symbol) return;
  const spot = `${symbol}-USDT`;
  const swap = `${symbol}-USDT-SWAP`;
  if ($("orderSpotInstMirror")) $("orderSpotInstMirror").value = spot;
  if ($("orderSwapInstMirror")) $("orderSwapInstMirror").value = swap;
  $("spotInstId").value = spot;
  $("swapInstId").value = swap;
  updateQuickState();
}

function buildOrderTimelineSeries(group, meta = {}) {
  const timelineOrders = (group?.orders || []).slice().sort((a, b) => Number(a.cTime || a.uTime || 0) - Number(b.cTime || b.uTime || 0));
  if (!timelineOrders.length) {
    return { mode: "无收益样本", points: [] };
  }

  const estimates = timelineOrders.map((order) => ({
    order,
    result: estimateOrderPnl(order, meta),
    stamp: Number(order.uTime || order.cTime || 0),
  }));

  const exactSamples = estimates.filter((item) => item.result.quality === "exact" && Number.isFinite(item.result.value));
  if (exactSamples.length) {
    let running = 0;
    const points = estimates.map((item) => {
      if (item.result.quality === "exact" && Number.isFinite(item.result.value)) {
        running += Number(item.result.value || 0);
      }
      return {
        stamp: item.stamp,
        value: running,
        label: formatOrderTime(item.order.uTime || item.order.cTime),
      };
    });
    return { mode: "已实现轨迹", points };
  }

  const positionSamples = estimates.filter((item) => item.result.quality === "position" && Number.isFinite(item.result.value));
  if (positionSamples.length) {
    const latestByInst = new Map();
    positionSamples.forEach((item) => {
      const key = item.order.instId || group.symbol;
      const previous = latestByInst.get(key);
      if (!previous || item.stamp >= previous.stamp) {
        latestByInst.set(key, item);
      }
    });
    return {
      mode: "持仓浮动轨迹",
      points: Array.from(latestByInst.values())
        .sort((a, b) => a.stamp - b.stamp)
        .map((item) => ({
          stamp: item.stamp,
          value: Number(item.result.value || 0),
          label: formatOrderTime(item.order.uTime || item.order.cTime),
        })),
    };
  }

  const estimatedSamples = estimates.filter((item) => item.result.quality === "estimated" && Number.isFinite(item.result.value));
  if (estimatedSamples.length) {
    return {
      mode: "现价估算轨迹",
      points: estimatedSamples.map((item) => ({
        stamp: item.stamp,
        value: Number(item.result.value || 0),
        label: formatOrderTime(item.order.uTime || item.order.cTime),
      })),
    };
  }

  return { mode: "待更多收益口径", points: [] };
}

function buildMiniTrendSvg(points, tone = "muted") {
  const width = 320;
  const height = 96;
  if (!points.length) {
    return `
      <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-hidden="true">
        <rect x="0" y="0" width="${width}" height="${height}" rx="14" fill="rgba(255,255,255,0.02)"></rect>
        <line x1="18" y1="32" x2="${width - 18}" y2="32" stroke="rgba(255,255,255,0.06)" stroke-dasharray="4 8"></line>
        <line x1="18" y1="64" x2="${width - 18}" y2="64" stroke="rgba(255,255,255,0.06)" stroke-dasharray="4 8"></line>
        <text x="${width / 2}" y="55" text-anchor="middle" fill="rgba(233,238,245,0.52)" font-size="12" font-family="Avenir Next, SF Pro Display, sans-serif">等待收益样本</text>
      </svg>
    `;
  }

  const color = tone === "positive" ? "#69f0ae" : tone === "negative" ? "#ff6b6b" : "#45d6c4";
  const fill = tone === "positive" ? "rgba(105,240,174,0.16)" : tone === "negative" ? "rgba(255,107,107,0.16)" : "rgba(69,214,196,0.12)";
  const left = 16;
  const right = width - 16;
  const top = 12;
  const bottom = height - 12;
  const values = points.map((point) => Number(point.value || 0));
  const min = Math.min(...values);
  const max = Math.max(...values);
  const span = Math.max(max - min, 1);
  const step = points.length > 1 ? (right - left) / (points.length - 1) : 0;
  const mapped = points.map((point, index) => {
    const x = left + step * index;
    const y = bottom - ((Number(point.value || 0) - min) / span) * (bottom - top);
    return [x, y];
  });
  const linePath = buildSmoothPath(mapped);
  const areaPath = buildAreaPath(mapped, bottom);
  const last = mapped[mapped.length - 1];
  return `
    <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-hidden="true">
      <rect x="0" y="0" width="${width}" height="${height}" rx="14" fill="rgba(255,255,255,0.02)"></rect>
      <line x1="${left}" y1="${top + 14}" x2="${right}" y2="${top + 14}" stroke="rgba(255,255,255,0.06)" stroke-dasharray="4 8"></line>
      <line x1="${left}" y1="${bottom - 14}" x2="${right}" y2="${bottom - 14}" stroke="rgba(255,255,255,0.06)" stroke-dasharray="4 8"></line>
      <path d="${areaPath}" fill="${fill}" stroke="none"></path>
      <path d="${linePath}" fill="none" stroke="${color}" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"></path>
      <circle cx="${last[0]}" cy="${last[1]}" r="4" fill="${color}"></circle>
    </svg>
  `;
}

function matchesOrderStateFilter(order, filter) {
  const state = String(order?.state || "");
  if (filter === "working") return ["live", "effective", "partially_filled"].includes(state);
  if (filter === "filled") return state === "filled";
  if (filter === "risk") return isRiskOrder(order);
  return true;
}

function matchesOrderMarketFilter(order, filter) {
  const instId = String(order?.instId || "");
  if (filter === "spot") return instId && !instId.endsWith("-SWAP");
  if (filter === "swap") return instId.endsWith("-SWAP");
  return true;
}

function isRiskOrder(order) {
  return ["canceled", "mmp_canceled", "order_failed", "failed", "rejected"].includes(String(order?.state || ""));
}

function getOrderCancelReason(order) {
  return String(
    order?.cancelSourceReason
      || order?.sMsg
      || order?.failCodeMsg
      || order?.msg
      || ""
  ).trim();
}

function summarizeOrderCancelReason(reason) {
  const text = String(reason || "").trim();
  if (!text) return "";
  const lower = text.toLowerCase();
  if (
    lower.includes("estimated fill price exceeded the price limit")
    || lower.includes("slipped beyond the best bid or ask price by at least 5%")
  ) {
    return "滑点 / 价格保护触发，交易所取消了这笔单。";
  }
  if (lower.includes("insufficient")) return "资金或可用仓位不足。";
  if (lower.includes("reduce only")) return "仅减仓限制触发。";
  if (lower.includes("post only")) return "Post Only 条件未满足。";
  if (lower.includes("risk")) return "交易所风控拦截。";
  if (text.length > 44) return `${text.slice(0, 44)}...`;
  return text;
}

function collectOrderExecutionStats(orders) {
  const list = orders || [];
  const working = list.filter((item) => ["live", "effective", "partially_filled"].includes(String(item.state || ""))).length;
  const filled = list.filter((item) => String(item.state || "") === "filled").length;
  const risk = list.filter((item) => isRiskOrder(item)).length;
  const successRate = list.length ? (filled / list.length) * 100 : 0;
  const reasonCounts = new Map();
  let latestRiskOrder = null;

  list.forEach((order) => {
    if (!isRiskOrder(order)) return;
    const summarized = summarizeOrderCancelReason(getOrderCancelReason(order));
    if (summarized) {
      reasonCounts.set(summarized, (reasonCounts.get(summarized) || 0) + 1);
    }
    const stamp = Number(order?.uTime || order?.cTime || 0);
    if (!latestRiskOrder || stamp >= Number(latestRiskOrder?.uTime || latestRiskOrder?.cTime || 0)) {
      latestRiskOrder = order;
    }
  });

  const topCancelReason = Array.from(reasonCounts.entries())
    .sort((left, right) => right[1] - left[1])[0]?.[0] || "";

  return {
    total: list.length,
    working,
    filled,
    risk,
    successRate,
    topCancelReason,
    latestCancelReason: summarizeOrderCancelReason(getOrderCancelReason(latestRiskOrder)),
  };
}

function buildDerivedOrderJournal(orders, source = "", symbols = []) {
  const list = Array.isArray(orders) ? orders : [];
  const stats = collectOrderExecutionStats(list);
  const canceledOrders = list.filter((item) => String(item?.state || "") === "canceled").length;
  const rejectedOrders = list.filter((item) => String(item?.state || "") === "rejected").length;
  const arbCounts = {
    total: 0,
    entry: 0,
    hedge: 0,
    exit: 0,
    rollback: 0,
  };
  list.forEach((item) => {
    const phase = getOrderArbPhase(item);
    if (!phase) return;
    arbCounts.total += 1;
    if (phase === "entry") arbCounts.entry += 1;
    else if (phase === "hedge") arbCounts.hedge += 1;
    else if (phase === "exit") arbCounts.exit += 1;
    else if (phase === "rollback") arbCounts.rollback += 1;
  });
  const arbRealizedPnl = list.reduce((sum, item) => {
    if (!getOrderArbPhase(item)) return sum;
    return sum + toOrderNumber(item?.pnl || item?.realizedPnl || item?.fillPnl);
  }, 0);
  const arbTotalFees = list.reduce((sum, item) => {
    if (!getOrderArbPhase(item)) return sum;
    return sum + toOrderNumber(item?.fee);
  }, 0);
  const latestStamp = list.reduce((latest, item) => {
    const stamp = Number(item?.uTime || item?.cTime || item?.fillTime || 0);
    return stamp > latest ? stamp : latest;
  }, 0);
  const insight = buildOrderJournalInsight({
    totalFees: list.reduce((sum, item) => sum + toOrderNumber(item?.fillFee ?? item?.fee), 0),
    realizedPnl: list.reduce((sum, item) => sum + toOrderNumber(item?.realizedPnl ?? item?.fillPnl ?? item?.pnl), 0),
  }, list);
  return {
    totalOrders: stats.total,
    workingOrders: stats.working,
    filledOrders: stats.filled,
    canceledOrders,
    rejectedOrders,
    lastCancelReason: stats.latestCancelReason || stats.topCancelReason || "",
    arbOrderCount: arbCounts.total,
    arbEntryOrders: arbCounts.entry,
    arbHedgeOrders: arbCounts.hedge,
    arbExitOrders: arbCounts.exit,
    arbRollbackOrders: arbCounts.rollback,
    arbRealizedPnl,
    arbTotalFees,
    arbNetPnl: arbRealizedPnl + arbTotalFees,
    insight,
    lastSource: source || "",
    lastReconciledAt: latestStamp ? String(latestStamp) : "",
    symbols: Array.isArray(symbols) ? symbols : [],
  };
}

function isCloseLikeOrder(order) {
  const instType = String(order?.instType || "").toUpperCase();
  const side = String(order?.side || "").toLowerCase();
  const subType = String(order?.tradeSubType || order?.subType || "").trim();
  const reason = String(order?.strategyReason || order?.lastMessage || "");
  if (order?.reduceOnly === true || String(order?.reduceOnly || "").toLowerCase() === "true") return true;
  if (instType === "SPOT") return side === "sell";
  if (["5", "6", "100", "101", "125", "126", "208", "209", "274", "275", "328", "329"].includes(subType)) return true;
  return ["平", "止盈", "止损", "退场", "回补", "卖出"].some((token) => reason.includes(token));
}

function buildOrderJournalInsight(journal, orders) {
  const list = Array.isArray(orders) ? orders : [];
  const filled = list.filter((item) => String(item?.state || "") === "filled");
  if (!filled.length) return "";
  const closeCount = filled.filter((item) => isCloseLikeOrder(item)).length;
  const openCount = filled.length - closeCount;
  const realized = Number(journal?.realizedPnl ?? 0);
  const totalFees = Number(journal?.totalFees ?? 0);
  if (closeCount === 0 && openCount > 0) {
    return `当前这批单全是开仓，还没看到平仓回报，所以已实现收益还是 0。${totalFees ? ` 当前已累计手续费 ${formatSignedMoney(totalFees)} USDT。` : ""}`;
  }
  if (closeCount > 0 && realized === 0) {
    return `这批单里已经有 ${closeCount} 笔平仓，但还没拿到明确的已实现收益字段。${totalFees ? ` 当前已累计手续费 ${formatSignedMoney(totalFees)} USDT。` : ""}`;
  }
  if (realized !== 0) {
    return `当前已实现${realized > 0 ? "盈利" : "亏损"} ${formatSignedMoney(realized)} USDT。`;
  }
  return "";
}

function renderOrderTerminalToolbar(groups, hasVisibleGroups = true) {
  const target = $("orderTerminalToolbar");
  if (!target) return;
  if (!groups.length) {
    target.className = "order-terminal-toolbar empty";
    target.textContent = "订单终端会在这里显示当前聚焦币和状态过滤器。";
    return;
  }

  const selected = groups.find((group) => group.symbol === dashboardState.selectedOrderSymbol) || groups[0];
  const filter = dashboardState.orderStateFilter || "all";
  const filterLabel = filter === "working"
    ? "只看工作中"
    : filter === "filled"
      ? "只看已成交"
      : filter === "risk"
        ? "只看异常"
        : "查看全部";
  const marketFilter = dashboardState.orderMarketFilter || "all";
  const marketLabel = marketFilter === "spot"
    ? "只看现货"
    : marketFilter === "swap"
      ? "只看永续"
      : "现货 + 永续";
  target.className = "order-terminal-toolbar";
  target.innerHTML = `
    <div class="order-terminal-toolbar-main">
      <div class="order-terminal-toolbar-copy">
        <span class="order-terminal-toolbar-eyebrow">订单流控制台</span>
        <strong>${escapeHtml(selected.symbol)} · ${escapeHtml(filterLabel)} · ${escapeHtml(marketLabel)}</strong>
        <small>${selected.orderCount} 笔订单 · ${selected.scopeLabel} · ${selected.latestInstId}${hasVisibleGroups ? "" : " · 当前筛选无匹配订单"}</small>
      </div>
      <button class="btn btn-ghost order-toolbar-clear-focus" type="button">清除聚焦</button>
    </div>
    <div class="order-terminal-toolbar-filter-row">
      <button class="chip ${filter === "all" ? "active" : ""}" type="button" data-order-filter="all">全部</button>
      <button class="chip ${filter === "working" ? "active" : ""}" type="button" data-order-filter="working">工作中</button>
      <button class="chip ${filter === "filled" ? "active" : ""}" type="button" data-order-filter="filled">已成交</button>
      <button class="chip ${filter === "risk" ? "active" : ""}" type="button" data-order-filter="risk">异常</button>
    </div>
    <div class="order-terminal-toolbar-filter-row">
      <button class="chip ${marketFilter === "all" ? "active" : ""}" type="button" data-order-market-filter="all">全市场</button>
      <button class="chip ${marketFilter === "spot" ? "active" : ""}" type="button" data-order-market-filter="spot">只看现货</button>
      <button class="chip ${marketFilter === "swap" ? "active" : ""}" type="button" data-order-market-filter="swap">只看永续</button>
    </div>
  `;

  target.querySelectorAll("[data-order-filter]").forEach((button) => {
    button.addEventListener("click", () => {
      dashboardState.orderStateFilter = button.dataset.orderFilter || "all";
      renderOrderFeed({
        orders: dashboardState.recentOrdersAll,
        source: dashboardState.orderFeedMeta?.source,
        stream: dashboardState.orderFeedMeta?.stream,
      });
    });
  });

  target.querySelectorAll("[data-order-market-filter]").forEach((button) => {
    button.addEventListener("click", () => {
      dashboardState.orderMarketFilter = button.dataset.orderMarketFilter || "all";
      renderOrderFeed({
        orders: dashboardState.recentOrdersAll,
        source: dashboardState.orderFeedMeta?.source,
        stream: dashboardState.orderFeedMeta?.stream,
      });
    });
  });

  target.querySelector(".order-toolbar-clear-focus")?.addEventListener("click", () => {
    dashboardState.selectedOrderSymbol = groups[0]?.symbol || "";
    dashboardState.orderStateFilter = "all";
    dashboardState.orderMarketFilter = "all";
    renderOrderFeed({
      orders: dashboardState.recentOrdersAll,
      source: dashboardState.orderFeedMeta?.source,
      stream: dashboardState.orderFeedMeta?.stream,
    });
  });
}

function renderOrderGlobalSummary(groups) {
  const target = $("orderGlobalSummary");
  if (!target) return;
  if (!groups.length) {
    target.className = "order-global-summary empty";
    target.textContent = "订单终端会在这里显示组合总收益、已实现、持仓浮动和现价估算。";
    return;
  }

  const total = groups.reduce((sum, group) => sum + Number(group.pnlTotal || 0), 0);
  const realized = groups.reduce((sum, group) => sum + Number(group.realizedPnl || 0), 0);
  const position = groups.reduce((sum, group) => sum + Number(group.positionPnl || 0), 0);
  const estimated = groups.reduce((sum, group) => sum + Number(group.estimatedPnl || 0), 0);
  const activeSymbols = groups.length;
  const visibleOrders = groups.reduce((sum, group) => sum + Number(group.orderCount || 0), 0);
  const journal = dashboardState.orderJournal || buildDerivedOrderJournal(groups.flatMap((group) => group.orders || []), dashboardState.orderFeedMeta?.source || "", dashboardState.orderJournalSymbols || []);
  const execution = collectOrderExecutionStats(groups.flatMap((group) => group.orders || []));
  const filledCount = Number(journal.filledOrders ?? execution.filled ?? 0);
  const riskCount = Number((journal.canceledOrders ?? 0)) + Number((journal.rejectedOrders ?? 0));
  const totalCount = Number(journal.totalOrders ?? execution.total ?? 0);
  const arbOrderCount = Number(journal.arbOrderCount ?? groups.reduce((sum, group) => sum + Number(group.arbOrderCount || 0), 0));
  const arbEntryOrders = Number(journal.arbEntryOrders ?? groups.reduce((sum, group) => sum + Number(group.arbEntryOrders || 0), 0));
  const arbHedgeOrders = Number(journal.arbHedgeOrders ?? groups.reduce((sum, group) => sum + Number(group.arbHedgeOrders || 0), 0));
  const arbExitOrders = Number(journal.arbExitOrders ?? groups.reduce((sum, group) => sum + Number(group.arbExitOrders || 0), 0));
  const arbRollbackOrders = Number(journal.arbRollbackOrders ?? groups.reduce((sum, group) => sum + Number(group.arbRollbackOrders || 0), 0));
  const arbRealizedPnl = Number(journal.arbRealizedPnl ?? groups.reduce((sum, group) => sum + Number(group.arbRealizedPnl || 0), 0));
  const arbTotalFees = Number(journal.arbTotalFees ?? groups.reduce((sum, group) => sum + Number(group.arbTotalFees || 0), 0));
  const arbNetPnl = Number(journal.arbNetPnl ?? (arbRealizedPnl + arbTotalFees));
  const successRate = totalCount ? (filledCount / totalCount) * 100 : execution.successRate;
  const totalTone = total > 0 ? "positive" : total < 0 ? "negative" : "muted";
  const stateLabelMap = {
    all: "全部状态",
    working: "只看工作中",
    filled: "只看已成交",
    risk: "只看异常",
  };
  const marketLabelMap = {
    all: "全市场",
    spot: "只看现货",
    swap: "只看永续",
  };
  const stateLabel = stateLabelMap[dashboardState.orderStateFilter || "all"] || "全部状态";
  const marketLabel = marketLabelMap[dashboardState.orderMarketFilter || "all"] || "全市场";

  target.className = "order-global-summary";
  target.innerHTML = `
    <div class="order-global-summary-head">
      <div>
        <span class="order-global-summary-eyebrow">组合订单收益总览</span>
        <strong class="tone-${totalTone}">${formatSignedMoney(total)} USDT</strong>
        <small>${activeSymbols} 个币种 · ${visibleOrders} 笔当前可见订单 · 账本成交 ${filledCount} / 异常 ${riskCount}${arbOrderCount ? ` · 套利 ${arbOrderCount} 笔` : ""}</small>
      </div>
    </div>
    <div class="order-global-summary-grid">
      <div class="order-global-summary-card">
        <span>已实现</span>
        <strong class="tone-${realized > 0 ? "positive" : realized < 0 ? "negative" : "muted"}">${formatSignedMoney(realized)} USDT</strong>
        <small>交易所明确回报</small>
      </div>
      <div class="order-global-summary-card">
        <span>持仓浮动</span>
        <strong class="tone-${position > 0 ? "positive" : position < 0 ? "negative" : "muted"}">${formatSignedMoney(position)} USDT</strong>
        <small>相关持仓未实现</small>
      </div>
      <div class="order-global-summary-card">
        <span>现价估算</span>
        <strong class="tone-${estimated > 0 ? "positive" : estimated < 0 ? "negative" : "muted"}">${formatSignedMoney(estimated)} USDT</strong>
        <small>没有明确收益字段时的估算</small>
      </div>
      <div class="order-global-summary-card">
        <span>成交 / 异常</span>
        <strong>${filledCount} / ${riskCount}</strong>
        <small>${totalCount ? `${formatPercentValue(successRate)} 成功率` : "等待订单样本"}</small>
      </div>
      <div class="order-global-summary-card">
        <span>当前聚焦</span>
        <strong>${escapeHtml(dashboardState.selectedOrderSymbol || groups[0]?.symbol || "--")}</strong>
        <small>${escapeHtml(stateLabel)} · ${escapeHtml(marketLabel)}</small>
      </div>
      ${arbOrderCount ? `
      <div class="order-global-summary-card">
        <span>套利净收益</span>
        <strong class="tone-${arbNetPnl > 0 ? "positive" : arbNetPnl < 0 ? "negative" : "muted"}">${formatSignedMoney(arbNetPnl)} USDT</strong>
        <small>已实现 ${formatSignedMoney(arbRealizedPnl)} · 手续费 ${formatSignedMoney(arbTotalFees)}</small>
      </div>
      ` : ""}
    </div>
    <div class="order-global-summary-note">
      <b>${arbOrderCount ? "套利腿进度" : "最近异常"}</b>
      <span>${escapeHtml(
        arbOrderCount
          ? `现货开腿 ${arbEntryOrders} · 永续对冲 ${arbHedgeOrders} · 退场 ${arbExitOrders} · 回滚 ${arbRollbackOrders}`
          : (journal.lastCancelReason || execution.latestCancelReason || execution.topCancelReason || "当前没有明显异常撤单，订单终端会在这里提示最近一次失败或取消的原因。")
      )}</span>
    </div>
  `;
}

function renderOrderSymbolOverview(groups, meta = {}) {
  const target = $("orderSymbolOverview");
  if (!target) return;
  if (!groups.length) {
    target.className = "order-symbol-overview empty";
    target.textContent = "更多币种订单进来后，这里会按币显示独立订单流和收益汇总。";
    return;
  }

  target.className = "order-symbol-overview";
  target.innerHTML = groups.map((group) => {
    const active = group.symbol === dashboardState.selectedOrderSymbol ? "active" : "";
    const pnlClass = group.pnlTotal > 0 ? "positive" : group.pnlTotal < 0 ? "negative" : "muted";
    return `
      <article class="order-symbol-card ${active}" data-order-symbol="${escapeHtml(group.symbol)}">
        <div class="order-symbol-head">
          <div>
            <strong>${escapeHtml(group.symbol)}</strong>
            <small>${escapeHtml(group.latestInstId)}</small>
          </div>
          <span class="order-state-pill ${group.working ? "tone-live" : group.filled ? "tone-done" : "tone-cancel"}">${group.orderCount} 笔</span>
        </div>
        <div class="order-symbol-metrics">
          <div><b>收益汇总</b><span class="tone-${pnlClass}">${group.hasPnl ? `${formatSignedMoney(group.pnlTotal)} USDT` : "--"}</span></div>
          <div><b>收益口径</b><span>${escapeHtml(group.scopeLabel)}</span></div>
          <div><b>套利净收益</b><span>${group.arbOrderCount ? `${formatSignedMoney(group.arbNetPnl)} USDT` : "--"}</span></div>
          <div><b>工作中 / 已成交 / 异常</b><span>${group.working} / ${group.filled} / ${group.riskCount}</span></div>
          <div><b>成交率</b><span>${group.orderCount ? formatPercentValue(group.successRate) : "--"}</span></div>
          <div><b>套利腿</b><span>${group.arbOrderCount ? `${group.arbEntryOrders}/${group.arbHedgeOrders}/${group.arbExitOrders}/${group.arbRollbackOrders}` : "--"}</span></div>
          <div><b>最新更新时间</b><span>${escapeHtml(group.latestAt)}</span></div>
        </div>
        ${group.topCancelReason ? `
          <div class="order-warning-note">
            <b>最近异常</b>
            <span>${escapeHtml(group.topCancelReason)}</span>
          </div>
        ` : ""}
        <div class="order-symbol-actions">
          <button class="btn btn-ghost order-symbol-filter" type="button" data-order-symbol="${escapeHtml(group.symbol)}">看这条订单流</button>
          <button class="btn btn-secondary order-symbol-switch" type="button" data-order-symbol="${escapeHtml(group.symbol)}">切到下单上下文</button>
        </div>
      </article>
    `;
  }).join("");

  target.querySelectorAll(".order-symbol-filter").forEach((button) => {
    button.addEventListener("click", () => {
      dashboardState.selectedOrderSymbol = button.dataset.orderSymbol || "";
      renderOrderFeed({
        orders: dashboardState.recentOrdersAll,
        source: meta?.source,
        stream: meta?.stream,
      });
    });
  });
  target.querySelectorAll(".order-symbol-switch").forEach((button) => {
    button.addEventListener("click", () => {
      const symbol = button.dataset.orderSymbol || "";
      dashboardState.selectedOrderSymbol = symbol;
      syncOrderContextToSymbol(symbol);
      renderOrderFeed({
        orders: dashboardState.recentOrdersAll,
        source: meta?.source,
        stream: meta?.stream,
      });
      setMessage(`订单终端已切到 ${symbol} 下单上下文。`, "ok");
    });
  });
}

function renderOrderTerminalFocus(selectedGroup, meta = {}) {
  const target = $("orderTerminalFocus");
  if (!target) return;
  if (!selectedGroup) {
    target.className = "order-terminal-focus empty";
    target.textContent = "聚焦某个币后，这里会显示当前订单流、收益拆分和下单上下文。";
    return;
  }

  const source = meta?.source === "private_ws"
    ? "私有 WS"
    : meta?.source === "local_cache"
      ? "本地缓存"
      : meta?.source === "local_echo"
        ? "本地回执"
        : meta?.source === "rest_multi"
          ? "REST 聚合"
          : "REST";
  const envLabel = $("order-env-label")?.textContent || "等待环境";
  const spotLabel = $("order-spot-label")?.textContent || `${selectedGroup.symbol}-USDT`;
  const swapLabel = $("order-swap-label")?.textContent || `${selectedGroup.symbol}-USDT-SWAP`;
  const watchlistLabel = $("order-watchlist-label")?.textContent || selectedGroup.symbol;
  const focusedMatchesContext = String(spotLabel).startsWith(`${selectedGroup.symbol}-`) || String(swapLabel).startsWith(`${selectedGroup.symbol}-`);
  const realizedTone = selectedGroup.realizedPnl > 0 ? "positive" : selectedGroup.realizedPnl < 0 ? "negative" : "muted";
  const positionTone = selectedGroup.positionPnl > 0 ? "positive" : selectedGroup.positionPnl < 0 ? "negative" : "muted";
  const estimatedTone = selectedGroup.estimatedPnl > 0 ? "positive" : selectedGroup.estimatedPnl < 0 ? "negative" : "muted";
  const focusInsight = buildOrderJournalInsight(
    {
      realizedPnl: selectedGroup.realizedPnl,
      totalFees: (selectedGroup.orders || []).reduce(
        (sum, item) => sum + toOrderNumber(item?.fillFee ?? item?.fee),
        0
      ),
    },
    selectedGroup.orders || []
  );

  target.className = "order-terminal-focus";
  target.innerHTML = `
    <div class="order-terminal-focus-head">
      <div>
        <span class="order-terminal-focus-eyebrow">当前聚焦订单流</span>
        <strong>${escapeHtml(selectedGroup.symbol)} · ${escapeHtml(selectedGroup.latestInstId)}</strong>
        <small>${escapeHtml(envLabel)} · ${escapeHtml(source)} · ${selectedGroup.orderCount} 笔订单</small>
      </div>
      <button class="btn btn-secondary order-terminal-sync" type="button">同步到当前下单上下文</button>
    </div>
    <div class="order-terminal-focus-grid">
      <div class="order-terminal-focus-card">
        <span>已实现收益</span>
        <strong class="tone-${realizedTone}">${formatSignedMoney(selectedGroup.realizedPnl)} USDT</strong>
        <small>交易所明确返回的成交收益</small>
      </div>
      <div class="order-terminal-focus-card">
        <span>持仓浮动</span>
        <strong class="tone-${positionTone}">${formatSignedMoney(selectedGroup.positionPnl)} USDT</strong>
        <small>和这条订单流相关的持仓未实现</small>
      </div>
      <div class="order-terminal-focus-card">
        <span>现价估算</span>
        <strong class="tone-${estimatedTone}">${formatSignedMoney(selectedGroup.estimatedPnl)} USDT</strong>
        <small>仅用于没有明确收益字段时的估算</small>
      </div>
      <div class="order-terminal-focus-card">
        <span>当前上下文</span>
        <strong>${focusedMatchesContext ? "已对齐" : "未对齐"}</strong>
        <small>${escapeHtml(spotLabel)} / ${escapeHtml(swapLabel)}</small>
      </div>
    </div>
    <div class="order-terminal-focus-meta">
      <div><b>收益口径</b><span>${escapeHtml(selectedGroup.scopeLabel)}</span></div>
      <div><b>待成交 / 待补口径</b><span>${selectedGroup.pendingCount} / ${selectedGroup.unresolvedCount}</span></div>
      <div><b>watchlist</b><span>${escapeHtml(watchlistLabel)}</span></div>
      <div><b>工作中 / 已成交</b><span>${selectedGroup.working} / ${selectedGroup.filled}</span></div>
      <div><b>异常 / 成交率</b><span>${selectedGroup.riskCount} / ${formatPercentValue(selectedGroup.successRate)}</span></div>
      <div><b>套利净收益</b><span>${selectedGroup.arbOrderCount ? `${formatSignedMoney(selectedGroup.arbNetPnl)} USDT` : "--"}</span></div>
      <div><b>套利腿进度</b><span>${selectedGroup.arbOrderCount ? `开腿 ${selectedGroup.arbEntryOrders} · 对冲 ${selectedGroup.arbHedgeOrders} · 退场 ${selectedGroup.arbExitOrders} · 回滚 ${selectedGroup.arbRollbackOrders}` : "当前不是套利腿订单流"}</span></div>
      <div><b>${focusInsight ? "收益判断" : "最近异常"}</b><span>${escapeHtml(focusInsight || selectedGroup.topCancelReason || "当前没有明显异常撤单")}</span></div>
    </div>
  `;

  target.querySelector(".order-terminal-sync")?.addEventListener("click", () => {
    syncOrderContextToSymbol(selectedGroup.symbol);
    renderOrderTerminalFocus(selectedGroup, meta);
    setMessage(`订单终端已把 ${selectedGroup.symbol} 同步为当前下单上下文。`, "ok");
  });
}

function renderOrderTimelineBoard(groups, meta = {}) {
  const target = $("orderTimelineBoard");
  if (!target) return;
  if (!groups.length) {
    target.className = "order-timeline-board empty";
    target.textContent = "多币订单进来后，这里会显示每个币的收益曲线和组合收益拆分时间线。";
    return;
  }

  const selectedSymbol = dashboardState.selectedOrderSymbol;
  const portfolioTotal = groups.reduce((sum, group) => sum + Number(group.pnlTotal || 0), 0);
  const portfolioTone = portfolioTotal > 0 ? "positive" : portfolioTotal < 0 ? "negative" : "muted";
  const ranked = groups.slice().sort((left, right) => Math.abs(Number(right.pnlTotal || 0)) - Math.abs(Number(left.pnlTotal || 0)));
  const maxAbs = Math.max(...ranked.map((group) => Math.abs(Number(group.pnlTotal || 0))), 1);

  target.className = "order-timeline-board";
  target.innerHTML = `
    <div class="order-timeline-head">
      <div>
        <span class="order-timeline-eyebrow">组合收益拆分时间线</span>
        <strong>${formatSignedMoney(portfolioTotal)} USDT</strong>
        <small>每个币独立形成订单收益轨迹，再汇总为组合视角。已实现、持仓浮动、现价估算分开展示。</small>
      </div>
      <span class="order-state-pill tone-${portfolioTone === "muted" ? "cancel" : portfolioTone === "positive" ? "done" : "fail"}">${ranked.length} 个币种</span>
    </div>
    <div class="order-timeline-grid">
      ${ranked.map((group) => {
        const tone = group.pnlTotal > 0 ? "positive" : group.pnlTotal < 0 ? "negative" : "muted";
        const series = buildOrderTimelineSeries(group, meta);
        const widthPct = Math.max((Math.abs(Number(group.pnlTotal || 0)) / maxAbs) * 100, 8);
        const activeClass = group.symbol === selectedSymbol ? "active" : "";
        return `
          <article class="order-timeline-card ${activeClass}" data-order-symbol="${escapeHtml(group.symbol)}">
            <div class="order-timeline-card-head">
              <div>
                <strong>${escapeHtml(group.symbol)}</strong>
                <small>${escapeHtml(series.mode)} · ${escapeHtml(group.scopeLabel)}</small>
              </div>
              <span class="tone-${tone}">${group.hasPnl ? `${formatSignedMoney(group.pnlTotal)} USDT` : "--"}</span>
            </div>
            <div class="order-timeline-curve">${buildMiniTrendSvg(series.points, tone)}</div>
            <div class="order-timeline-breakdown">
              <div><b>已实现</b><span class="tone-${group.realizedPnl > 0 ? "positive" : group.realizedPnl < 0 ? "negative" : "muted"}">${formatSignedMoney(group.realizedPnl)} USDT</span></div>
              <div><b>持仓浮动</b><span class="tone-${group.positionPnl > 0 ? "positive" : group.positionPnl < 0 ? "negative" : "muted"}">${formatSignedMoney(group.positionPnl)} USDT</span></div>
              <div><b>现价估算</b><span class="tone-${group.estimatedPnl > 0 ? "positive" : group.estimatedPnl < 0 ? "negative" : "muted"}">${formatSignedMoney(group.estimatedPnl)} USDT</span></div>
            </div>
            <div class="order-timeline-share">
              <span>${escapeHtml(group.symbol)} 在组合收益里的贡献</span>
              <div class="order-timeline-bar"><i class="tone-${tone}" style="width:${widthPct}%"></i></div>
            </div>
          </article>
        `;
      }).join("")}
    </div>
  `;

  target.querySelectorAll(".order-timeline-card").forEach((card) => {
    card.addEventListener("click", () => {
      const symbol = card.dataset.orderSymbol || "";
      if (!symbol) return;
      dashboardState.selectedOrderSymbol = symbol;
      const group = ranked.find((item) => item.symbol === symbol);
      if (group?.orders?.[0]) {
        dashboardState.selectedOrderKey = getOrderKey(group.orders[0]);
      }
      renderOrderFeed({
        orders: dashboardState.recentOrdersAll,
        source: dashboardState.orderFeedMeta?.source,
        stream: dashboardState.orderFeedMeta?.stream,
      });
    });
  });
}

function rerenderSelectedOrderDetail() {
  if (!dashboardState.ordersLoadedOnce || !dashboardState.recentOrdersAll?.length) return;
  const selectedOrder = dashboardState.recentOrdersAll.find((item) => getOrderKey(item) === dashboardState.selectedOrderKey)
    || dashboardState.recentOrdersAll[0];
  renderOrderDetail(selectedOrder, dashboardState.orderFeedMeta);
}

function renderOrderSummary(data, orders) {
  const source = data?.source === "private_ws"
    ? "私有 WS"
    : data?.source === "local_cache"
      ? "本地缓存"
    : data?.source === "local_echo"
      ? "本地回执"
      : data?.source === "rest_multi"
      ? "REST 聚合"
        : "REST";
  const stats = collectOrderExecutionStats(orders);
  const journal = data?.journal || dashboardState.orderJournal || {};
  const journalSource = journal?.lastSource === "private_ws"
    ? "私有账本"
    : journal?.lastSource === "paper_state_recovered"
      ? "本地恢复"
      : journal?.lastSource === "rest_multi"
        ? "REST 聚合"
        : journal?.lastSource === "rest"
          ? "REST"
          : journal?.lastSource
            ? journal.lastSource
            : "--";
  const reconciledAt = journal?.lastReconciledAt ? formatOrderTime(journal.lastReconciledAt) : "--";
  const totalCount = Number(journal.totalOrders ?? (((data?.orders || []).length) || 0));
  const workingCount = Number(journal.workingOrders ?? stats.working ?? 0);
  const filledCount = Number(journal.filledOrders ?? stats.filled ?? 0);
  const riskCount = Number(journal.canceledOrders ?? 0) + Number(journal.rejectedOrders ?? 0);
  const arbOrderCount = Number(journal.arbOrderCount ?? 0);
  const arbEntryOrders = Number(journal.arbEntryOrders ?? 0);
  const arbHedgeOrders = Number(journal.arbHedgeOrders ?? 0);
  const arbExitOrders = Number(journal.arbExitOrders ?? 0);
  const arbRollbackOrders = Number(journal.arbRollbackOrders ?? 0);
  const successRate = totalCount ? (filledCount / totalCount) * 100 : stats.successRate;
  const insight = journal.insight || buildOrderJournalInsight(journal, dashboardState.recentOrdersAll || []);
  $("order-summary-count").textContent = String(totalCount);
  $("order-summary-working").textContent = String(workingCount);
  $("order-summary-filled").textContent = String(filledCount);
  if ($("order-summary-risk")) $("order-summary-risk").textContent = String(riskCount);
  if ($("order-summary-rate")) $("order-summary-rate").textContent = totalCount ? formatPercentValue(successRate) : "--";
  $("order-summary-source").textContent = source;
  if ($("order-summary-journal-source")) $("order-summary-journal-source").textContent = journalSource;
  if ($("order-summary-reconciled")) $("order-summary-reconciled").textContent = reconciledAt;
  if ($("orderSummaryInsight")) {
    $("orderSummaryInsight").innerHTML = riskCount
      ? `<b>最近异常</b><span>${escapeHtml(journal.lastCancelReason || stats.latestCancelReason || stats.topCancelReason || "存在已撤或失败订单，详情区会继续显示交易所原始原因。")} · 账本 ${escapeHtml(journalSource)} · 最近对账 ${escapeHtml(reconciledAt)}${arbOrderCount ? ` · 套利开腿 ${arbEntryOrders} / 对冲 ${arbHedgeOrders} / 退场 ${arbExitOrders} / 回滚 ${arbRollbackOrders}` : ""}</span>`
      : `<b>当前概览</b><span>${insight || (totalCount ? `最近 ${totalCount} 笔里成交 ${filledCount} 笔，当前没有明显异常撤单。账本 ${journalSource}，最近对账 ${reconciledAt}。${arbOrderCount ? `套利开腿 ${arbEntryOrders} / 对冲 ${arbHedgeOrders} / 退场 ${arbExitOrders} / 回滚 ${arbRollbackOrders}。` : ""}` : "订单一进来，这里会直接告诉你成功率、账本来源和最近异常原因。")}</span>`;
  }
}

function renderOrderDetail(order, meta = {}) {
  const target = $("orderDetail");
  if (!order) {
    target.className = "order-detail empty";
    target.innerHTML = `
      <div class="order-detail-empty">
        <strong>选中一笔订单</strong>
        <span>这里会直接告诉你这笔单现在是不是还在持仓、有没有卖出，以及收益到底是浮动还是已实现。</span>
      </div>
    `;
    return;
  }

  const source = meta?.source === "private_ws"
    ? "私有 WS 回报"
    : meta?.source === "local_cache"
      ? "本地缓存恢复"
    : meta?.source === "local_echo"
      ? "本地下单回执"
      : meta?.source === "rest_multi"
        ? "REST 聚合拉取"
        : "REST 拉取";
  const tone = getOrderTone(order.state);
  const createdAt = formatOrderTime(order.cTime);
  const updatedAt = formatOrderTime(order.uTime || order.fillTime || order.cTime);
  const targetPrice = order.ordType === "market" ? "市价" : formatOrderValue(order.px);
  const avgPrice = formatOrderValue(order.avgPx || order.fillPx);
  const requestSize = formatOrderValue(order.sz);
  const filledSize = formatOrderValue(order.accFillSz || order.fillSz || 0);
  const lifecycle = buildOrderLifecycleContext(order, meta);
  const pnl = buildOrderPnlContext(order, meta);
  const cancelReason = getOrderCancelReason(order);
  const cancelReasonShort = summarizeOrderCancelReason(cancelReason);
  const arbPhase = getOrderArbPhase(order);
  const strategyAttribution = arbPhase
    ? `${getOrderArbPhaseLabel(arbPhase)} · ${order.strategyLeg === "swap" ? "永续腿" : order.strategyLeg === "spot" ? "现货腿" : "套利订单"}`
    : (order.strategyReason || "普通订单");
  const executionSummary = isRiskOrder(order)
    ? (cancelReasonShort || "这笔单被交易所撤销或拒绝。")
    : order.state === "filled"
      ? lifecycle.detailText
      : "这笔单当前还在工作中或等待成交完成。";

  target.className = "order-detail";
  target.innerHTML = `
    <div class="order-detail-head">
      <div class="order-detail-title">
        <strong>${escapeHtml(order.instId || "--")}</strong>
        <span>${escapeHtml(getOrderSideLabel(order))} · ${escapeHtml(order.ordType || "--")} · ${escapeHtml(source)}</span>
      </div>
      <span class="order-state-pill ${tone}">${escapeHtml(getOrderStateLabel(order.state))}</span>
    </div>
    <div class="order-detail-hero">
      <div class="order-hero-card">
        <span>委托价格</span>
        <strong>${escapeHtml(targetPrice)}</strong>
      </div>
      <div class="order-hero-card">
        <span>已成交</span>
        <strong>${escapeHtml(filledSize)}</strong>
      </div>
      <div class="order-hero-card">
        <span>成交均价</span>
        <strong>${escapeHtml(avgPrice)}</strong>
      </div>
      <div class="order-hero-card">
        <span>${escapeHtml(lifecycle.title)}</span>
        <strong class="${escapeHtml(lifecycle.toneClass)}">${escapeHtml(lifecycle.valueText)}</strong>
      </div>
    </div>
    <div class="order-detail-grid">
      <div><b>委托数量</b><span>${escapeHtml(requestSize)}</span></div>
      <div><b>保证金 / 模式</b><span>${escapeHtml(order.tdMode || "--")}</span></div>
      <div><b>当前状态</b><span>${escapeHtml(lifecycle.scopeText)}</span></div>
      <div><b>收益金额</b><span class="${escapeHtml(pnl.toneClass)}">${escapeHtml(pnl.valueText)}</span></div>
      <div><b>收益口径</b><span>${escapeHtml(pnl.scopeText)}</span></div>
      <div><b>当前价格 / 标记价</b><span>${escapeHtml(pnl.currentPriceText)}</span></div>
      <div><b>订单号</b><span>${escapeHtml(order.ordId || "--")}</span></div>
      <div><b>Client ID</b><span>${escapeHtml(order.clOrdId || "--")}</span></div>
      <div><b>创建时间</b><span>${escapeHtml(createdAt)}</span></div>
      <div><b>最近更新时间</b><span>${escapeHtml(updatedAt)}</span></div>
      <div><b>手续费</b><span>${escapeHtml(formatOrderValue(order.fee))}</span></div>
      <div><b>订单标签</b><span>${escapeHtml(order.tag || "--")}</span></div>
      <div><b>执行归因</b><span>${escapeHtml(strategyAttribution)}</span></div>
      <div><b>仅减仓</b><span>${order.reduceOnly ? "是" : "否"}</span></div>
      <div><b>成交来源</b><span>${escapeHtml(meta?.stream?.connected ? "实时推送" : source)}</span></div>
      <div><b>订单结果</b><span>${escapeHtml(executionSummary)}</span></div>
      <div><b>取消来源</b><span>${escapeHtml(order.cancelSource || "--")}</span></div>
      <div><b>仓位判断</b><span>${escapeHtml(lifecycle.detailText)}</span></div>
      <div><b>收益判断</b><span>${escapeHtml(pnl.detailText)}</span></div>
      <div class="span-wide"><b>状态说明</b><span>${escapeHtml(lifecycle.noteText)}</span></div>
      <div class="span-wide"><b>收益说明</b><span>${escapeHtml(pnl.noteText)}</span></div>
      <div class="span-wide"><b>取消原因</b><span>${escapeHtml(cancelReason || "当前没有取消原因，这笔单不是撤单/失败单，或交易所没有返回额外说明。")}</span></div>
    </div>
    <div class="order-detail-note">
      最近一笔会优先高亮；如果下单后私有 WS 还没回报，这里会先显示本地回执，再自动切换成交易所最新状态。现在这块会先把“当前还有没有仓”说清楚，再告诉你收益金额到底是已实现、持仓浮动，还是暂时拿不到精确口径。
    </div>
  `;
}

function renderOrderFeed(data) {
  const allOrders = data?.orders || [];
  const target = $("recentOrders");
  const fallbackJournal = buildDerivedOrderJournal(allOrders, data?.lastSource || data?.source || "", data?.symbols || []);
  dashboardState.recentOrdersAll = allOrders;
  dashboardState.orderJournal = data?.journal || fallbackJournal;
  dashboardState.orderJournalSymbols = data?.symbols || fallbackJournal.symbols || [];
  dashboardState.orderFeedMeta = {
    source: data?.source || "rest",
    stream: data?.stream || null,
    journal: data?.journal || fallbackJournal,
    symbols: data?.symbols || fallbackJournal.symbols || [],
    lastReconciledAt: data?.lastReconciledAt || fallbackJournal.lastReconciledAt || "",
    lastSource: data?.lastSource || fallbackJournal.lastSource || "",
  };
  const baseGroups = groupOrdersBySymbol(allOrders, dashboardState.orderFeedMeta);
  const stateFilter = dashboardState.orderStateFilter || "all";
  const marketFilter = dashboardState.orderMarketFilter || "all";
  const visibleOrders = allOrders.filter(
    (order) => matchesOrderStateFilter(order, stateFilter) && matchesOrderMarketFilter(order, marketFilter)
  );
  const groups = groupOrdersBySymbol(visibleOrders, dashboardState.orderFeedMeta);
  renderOrderSummary(data, allOrders);
  renderOrderGlobalSummary(groups);
  renderOrderSymbolOverview(groups, dashboardState.orderFeedMeta);

  if (!baseGroups.length) {
    target.className = "orders-feed empty";
    target.innerHTML = '<div class="empty">暂无订单数据</div>';
    dashboardState.recentOrders = [];
    dashboardState.selectedOrderSymbol = "";
    dashboardState.selectedOrderKey = null;
    renderOrderTerminalFocus(null);
    renderOrderTimelineBoard([]);
    renderOrderTerminalToolbar([]);
    renderOrderGlobalSummary([]);
    renderOrderDetail(null);
    $("metric-order-count").textContent = String(allOrders.length || 0);
    dashboardState.ordersLoadedOnce = true;
    return;
  }

  if (!groups.length) {
    target.className = "orders-feed empty";
    target.innerHTML = '<div class="empty">当前筛选条件下没有匹配订单</div>';
    dashboardState.recentOrders = [];
    dashboardState.selectedOrderKey = null;
    renderOrderTerminalFocus(null);
    renderOrderTimelineBoard([]);
    renderOrderTerminalToolbar(baseGroups, false);
    renderOrderGlobalSummary([]);
    renderOrderDetail(null);
    $("metric-order-count").textContent = String(allOrders.length || 0);
    dashboardState.ordersLoadedOnce = true;
    return;
  }

  const hasSelectedSymbol = groups.some((group) => group.symbol === dashboardState.selectedOrderSymbol);
  if (!dashboardState.selectedOrderSymbol || !hasSelectedSymbol) {
    dashboardState.selectedOrderSymbol = groups[0]?.symbol || "";
  }

  const prioritizedGroups = groups.slice().sort((left, right) => {
    const leftActive = left.symbol === dashboardState.selectedOrderSymbol ? 1 : 0;
    const rightActive = right.symbol === dashboardState.selectedOrderSymbol ? 1 : 0;
    if (leftActive !== rightActive) return rightActive - leftActive;
    return Number(right.orders[0]?.uTime || right.orders[0]?.cTime || 0)
      - Number(left.orders[0]?.uTime || left.orders[0]?.cTime || 0);
  });

  const selectedGroup = prioritizedGroups.find((group) => group.symbol === dashboardState.selectedOrderSymbol) || prioritizedGroups[0];
  const selectedGroupOrders = selectedGroup?.orders || [];
  dashboardState.recentOrders = selectedGroupOrders.slice(0, 8);
  renderOrderTimelineBoard(prioritizedGroups, dashboardState.orderFeedMeta);
  renderOrderTerminalFocus(selectedGroup, dashboardState.orderFeedMeta);
  renderOrderTerminalToolbar(baseGroups, true);

  const allVisibleOrders = prioritizedGroups.flatMap((group) => group.orders);

  if (!dashboardState.selectedOrderKey || !allVisibleOrders.some((item) => getOrderKey(item) === dashboardState.selectedOrderKey)) {
    dashboardState.selectedOrderKey = selectedGroupOrders[0] ? getOrderKey(selectedGroupOrders[0]) : null;
  }

  const selectedOrderSymbol = dashboardState.selectedOrderSymbol;
  target.className = "orders-feed";
  target.innerHTML = `
    <div class="orders-group-list">
      ${prioritizedGroups.map((group) => {
        const isActiveGroup = group.symbol === selectedOrderSymbol;
        const filteredOrders = group.orders;
        const expanded = Boolean(dashboardState.orderExpandedSymbols?.[group.symbol]);
        const previewCount = isActiveGroup ? 8 : 3;
        const groupOrders = expanded ? filteredOrders : filteredOrders.slice(0, previewCount);
        const pnlClass = group.pnlTotal > 0 ? "positive" : group.pnlTotal < 0 ? "negative" : "muted";
        return `
          <section class="orders-group-section ${isActiveGroup ? "active" : ""}" data-order-symbol="${escapeHtml(group.symbol)}">
            <div class="orders-group-head">
              <div class="orders-group-head-main">
                <div class="orders-group-title-line">
                  <strong>${escapeHtml(group.symbol)}</strong>
                  <span class="order-state-pill ${group.working ? "tone-live" : group.filled ? "tone-done" : "tone-cancel"}">${group.orderCount} 笔订单</span>
                </div>
                <span class="orders-group-subline">${escapeHtml(group.latestInstId)}</span>
              </div>
              <button class="btn ${isActiveGroup ? "btn-ghost" : "btn-secondary"} orders-group-focus" type="button" data-order-symbol="${escapeHtml(group.symbol)}">
                ${isActiveGroup ? "当前聚焦" : "聚焦此币"}
              </button>
            </div>
            <div class="orders-group-metrics">
              <div><b>收益汇总</b><span class="tone-${pnlClass}">${group.hasPnl ? `${formatSignedMoney(group.pnlTotal)} USDT` : "--"}</span></div>
              <div><b>收益口径</b><span>${escapeHtml(group.scopeLabel)}</span></div>
              <div><b>工作中 / 已成交 / 异常</b><span>${group.working} / ${group.filled} / ${group.riskCount}</span></div>
              <div><b>成交率</b><span>${group.orderCount ? formatPercentValue(group.successRate) : "--"}</span></div>
              <div><b>最近更新时间</b><span>${escapeHtml(group.latestAt)}</span></div>
            </div>
            ${group.topCancelReason ? `
              <div class="order-warning-note">
                <b>最近异常</b>
                <span>${escapeHtml(group.topCancelReason)}</span>
              </div>
            ` : ""}
            ${filteredOrders.length ? `<div class="orders-group-cards">
              ${groupOrders.map((order) => {
                const key = getOrderKey(order);
                const tone = getOrderTone(order.state);
                const sideClass = order.side === "sell" ? "down" : "up";
                const activeClass = key === dashboardState.selectedOrderKey ? "active" : "";
                const targetPrice = order.ordType === "market" ? "市价" : formatOrderValue(order.px);
                const cancelReason = summarizeOrderCancelReason(getOrderCancelReason(order));
                return `
                  <button type="button" class="order-card compact ${sideClass} ${activeClass}" data-order-key="${escapeHtml(key)}" data-order-symbol="${escapeHtml(group.symbol)}">
                    <div class="order-card-head">
                      <div>
                        <span class="order-card-inst">${escapeHtml(order.instId || "--")}</span>
                        <span class="order-card-side">${escapeHtml(getOrderSideLabel(order))}</span>
                      </div>
                      <span class="order-state-pill ${tone}">${escapeHtml(getOrderStateLabel(order.state))}</span>
                    </div>
                    <div class="order-card-meta">
                      <div><b>委托</b><span>${escapeHtml(targetPrice)}</span></div>
                      <div><b>数量</b><span>${escapeHtml(formatOrderValue(order.sz))}</span></div>
                      <div><b>已成交</b><span>${escapeHtml(formatOrderValue(order.accFillSz || order.fillSz || 0))}</span></div>
                    </div>
                    ${cancelReason ? `
                      <div class="order-card-reason">
                        <b>异常原因</b>
                        <span>${escapeHtml(cancelReason)}</span>
                      </div>
                    ` : ""}
                    <div class="order-card-foot">
                      <span>${escapeHtml(formatOrderTime(order.cTime))}</span>
                      <span>${escapeHtml(order.clOrdId || order.ordId || "--")}</span>
                    </div>
                  </button>
                `;
              }).join("")}
            </div>` : `<div class="orders-group-empty">当前筛选条件下，这个币没有匹配订单。</div>`}
            ${(filteredOrders.length > groupOrders.length || filteredOrders.length > previewCount) ? `
              <div class="orders-group-foot-row">
                <div class="orders-group-foot">当前展示 ${groupOrders.length} / ${filteredOrders.length} 笔匹配订单。${expanded ? "当前已展开整条订单流。" : "聚焦后会优先展开这条订单流。"}</div>
                ${filteredOrders.length > previewCount ? `<button class="btn btn-ghost orders-group-expand" type="button" data-order-symbol="${escapeHtml(group.symbol)}">${expanded ? "收起" : "展开更多"}</button>` : ""}
              </div>
            ` : ""}
          </section>
        `;
      }).join("")}
    </div>
  `;

  target.querySelectorAll(".order-card").forEach((button) => {
    button.addEventListener("click", () => {
      dashboardState.selectedOrderKey = button.dataset.orderKey;
      dashboardState.selectedOrderSymbol = button.dataset.orderSymbol || dashboardState.selectedOrderSymbol;
      renderOrderFeed({
        orders: dashboardState.recentOrdersAll,
        source: dashboardState.orderFeedMeta?.source,
        stream: dashboardState.orderFeedMeta?.stream,
      });
    });
  });

  target.querySelectorAll(".orders-group-focus").forEach((button) => {
    button.addEventListener("click", () => {
      const symbol = button.dataset.orderSymbol || "";
      dashboardState.selectedOrderSymbol = symbol;
      const group = prioritizedGroups.find((item) => item.symbol === symbol);
      if (group?.orders?.[0]) {
        dashboardState.selectedOrderKey = getOrderKey(group.orders[0]);
      }
      renderOrderFeed({
        orders: dashboardState.recentOrdersAll,
        source: dashboardState.orderFeedMeta?.source,
        stream: dashboardState.orderFeedMeta?.stream,
      });
    });
  });

  target.querySelectorAll(".orders-group-expand").forEach((button) => {
    button.addEventListener("click", () => {
      const symbol = button.dataset.orderSymbol || "";
      if (!symbol) return;
      dashboardState.orderExpandedSymbols = {
        ...(dashboardState.orderExpandedSymbols || {}),
        [symbol]: !dashboardState.orderExpandedSymbols?.[symbol],
      };
      renderOrderFeed({
        orders: dashboardState.recentOrdersAll,
        source: dashboardState.orderFeedMeta?.source,
        stream: dashboardState.orderFeedMeta?.stream,
      });
    });
  });

  const selectedOrder = allVisibleOrders.find((item) => getOrderKey(item) === dashboardState.selectedOrderKey) || selectedGroupOrders[0] || allVisibleOrders[0] || allOrders[0];
  renderOrderDetail(selectedOrder, dashboardState.orderFeedMeta);
  $("metric-order-count").textContent = String(allOrders.length || 0);
  dashboardState.ordersLoadedOnce = true;
}

function renderLogs(logs) {
  const target = $("bot-logs");
  if (!logs || !logs.length) {
    target.className = "log-list empty";
    target.innerHTML = "暂无日志";
    return;
  }
  target.className = "log-list";
  target.innerHTML = logs
    .slice()
    .reverse()
    .map(
      (entry) => `
        <div class="log-item">
          <small>${entry.ts || "-"}</small>
          <b>${entry.level || "info"}</b>
          <span>${entry.message || ""}</span>
        </div>
      `
    )
    .join("");
}

function renderResearchState(research) {
  dashboardState.research = research || {};
  const summary = research?.summary || {};
  const bestConfig = research?.bestConfig || {};
  const leaderboard = research?.leaderboard || [];
  const generationSummaries = research?.generationSummaries || [];
  const pipeline = research?.pipeline || {};
  const capital = pipeline?.capital || {};
  const notes = research?.notes || [];

  $("research-status").textContent = research?.statusText || "未运行";
  $("research-status-sub").textContent = research?.lastRunAt
    ? `${research.mode === "optimize" ? "进化优化" : "历史回测"} · ${research.lastRunAt}${pipeline?.evaluatedCount ? ` · 已评估 ${pipeline.evaluatedCount} 条` : ""}${pipeline?.populationSize ? ` · 持续种群 ${pipeline.populationSize}` : ""}${capital?.horseSlots ? ` · 资金槽位 ${capital.horseSlots}` : ""}`
    : "先运行回测或自动优化";

  $("research-return").textContent = summary.returnPct ? formatPercentValue(summary.returnPct) : "--";
  $("research-return").style.color =
    Number(summary.returnPct || 0) > 0 ? "var(--success)" : Number(summary.returnPct || 0) < 0 ? "var(--danger)" : "var(--text)";
  $("research-return-sub").textContent = summary.tradeCount
    ? `${summary.tradeCount} 笔平仓 · 胜率 ${formatPercentValue(summary.winRatePct || 0)}`
    : "净收益率";

  $("research-drawdown").textContent = summary.maxDrawdownPct ? formatPercentValue(summary.maxDrawdownPct) : "--";
  $("research-drawdown").style.color =
    Number(summary.maxDrawdownPct || 0) < 0 ? "var(--danger)" : "var(--text)";
  $("research-drawdown-sub").textContent = research?.sampleCount
    ? `${research.sampleCount} 根样本 K 线`
    : "研究样本内";

  $("research-best").textContent = bestConfig.fastEma
    ? `${bestConfig.bar} · EMA ${bestConfig.fastEma}/${bestConfig.slowEma}`
    : "--";
  $("research-best-sub").textContent = bestConfig.stopLossPct
    ? `SL ${bestConfig.stopLossPct}% / TP ${bestConfig.takeProfitPct}%${pipeline?.evolutionLoops ? ` · ${pipeline.evolutionLoops} 轮进化` : ""}${pipeline?.populationSize ? ` · 池 ${pipeline.populationSize}` : ""}${capital?.perHorseCapital ? ` · 单马约 ${Number(capital.perHorseCapital).toFixed(2)}U` : ""}`
    : "自动优化后可直接应用";

  const board = $("research-board");
  if (!leaderboard.length) {
    board.className = "table-like empty";
    board.innerHTML = "自动优化后会列出带名字、收益、回撤和单独导出动作的候选策略。";
  } else {
    board.className = "table-like";
    board.innerHTML = leaderboard
      .map(
        (item, index) => `
          <div class="row row-action">
            <div class="research-name-cell">
              <b>策略名</b>
              <span class="research-name">${item.name || item.label}</span>
              <small class="research-detail">${item.detail || item.label}</small>
            </div>
            <div><b>代次</b><span>G${item.generation || "-"}</span></div>
            <div><b>来源</b><span>${item.originLabel || item.origin || "-"}</span></div>
            <div><b>方向</b><span>${item.config?.swapStrategyMode === "short_only" ? "只做空" : item.config?.swapStrategyMode === "trend_follow" ? "顺势双向" : "只做多"}</span></div>
            <div><b>杠杆</b><span>${item.config?.swapLeverage ? `${item.config.swapLeverage}x` : "-"}</span></div>
            <div><b>收益</b><span>${formatPercentValue(item.returnPct)}</span></div>
            <div><b>回撤</b><span>${formatPercentValue(item.maxDrawdownPct)}</span></div>
            <div><b>胜率</b><span>${formatPercentValue(item.winRatePct)}</span></div>
            <div><b>分数</b><span>${Number(item.score).toFixed(2)}</span></div>
            <div class="research-actions-row">
              <button class="btn btn-ghost research-apply" type="button" data-index="${index}">应用</button>
              <button class="btn btn-ghost research-export" type="button" data-index="${index}">导出</button>
            </div>
          </div>
        `
      )
      .join("");
    board.querySelectorAll(".research-apply").forEach((button) => {
      button.addEventListener("click", () => {
        const item = leaderboard[Number(button.dataset.index || -1)];
        if (!item?.fullConfig && !item?.config) return;
        const appliedConfig = { ...collectAutomationConfig(), ...(item.fullConfig || item.config) };
        fillAutomationForm(appliedConfig);
        setStrategyApplyState(
          "draft",
          "已回填候选策略，待保存",
          `${item.name || item.label} · ${buildStrategyFormSummary(appliedConfig)}`
        );
        setAutomationMessage(`已回填候选参数: ${item.name || item.label}`, "ok");
      });
    });
    board.querySelectorAll(".research-export").forEach((button) => {
      button.addEventListener("click", async () => {
        const index = Number(button.dataset.index || -1);
        try {
          await exportStrategies(index);
        } catch (err) {
          setAutomationMessage(err.message, "err");
        }
      });
    });
  }

  const notesTarget = $("research-notes");
  if (!notes.length && !generationSummaries.length) {
    notesTarget.className = "table-like empty";
    notesTarget.innerHTML = "回测会基于历史收盘价做本地模拟，不会真实下单。";
  } else {
    notesTarget.className = "table-like";
    const timeline = generationSummaries.map(
      (item) => `
        <div class="row">
          <div><b>轮次</b><span>第 ${item.generation} 轮</span></div>
          <div><b>冠军</b><span>${item.winnerLabel}</span></div>
          <div><b>冠军收益</b><span>${formatPercentValue(item.winnerReturnPct)}</span></div>
          <div><b>冠军回撤</b><span>${formatPercentValue(item.winnerDrawdownPct)}</span></div>
          <div><b>均分</b><span>${Number(item.avgScore || 0).toFixed(2)}</span></div>
          <div><b>赛马数</b><span>${item.candidateCount || 0}</span></div>
        </div>
      `
    ).join("");
    const noteRows = notes
      .map(
        (note) => `
          <div class="row">
            <div><b>说明</b><span>${note}</span></div>
          </div>
        `
      )
      .join("");
    notesTarget.innerHTML = `${timeline}${noteRows}`;
  }
}

function renderBotMarket(targetId, market, watchlistEntries = [], scope = "spot") {
  const target = $(targetId);
  if (!market) {
    target.innerHTML = '<div class="empty">暂无数据</div>';
    return;
  }
  const rows = [
    { label: "标的", value: market.instId || "-" },
    { label: "信号", value: market.signal || "-" },
    { label: "趋势", value: market.trend || "-" },
    { label: "最新价", value: market.lastPrice || "-" },
    { label: "持仓方向", value: market.positionSide || "-" },
    { label: "持仓数量", value: market.positionSize || "-" },
    { label: "持仓名义", value: market.positionNotional || "-" },
    { label: "入场价", value: market.entryPrice || "-" },
    { label: "最近动作", value: market.lastAction || "-" },
    { label: "最近说明", value: market.lastMessage || "-" },
  ];
  target.className = "table-like";
  const watchlistMarkup = watchlistEntries.length > 1
    ? `
      <div class="row">
        <div><b>并行 watchlist</b><span>${watchlistEntries.length} 个标的</span></div>
      </div>
      ${watchlistEntries.map((entry) => {
        const current = entry?.[scope] || {};
        const entrySummary = entry?.summary || {};
        return `
          <div class="row">
            <div><b>${entry.symbol || "-"}</b><span>${current.instId || "-"} · ${current.signal || "-"} · ${current.positionSide || "flat"} · ${current.lastMessage || "-"}</span></div>
            <div><b>独立状态</b><span>${entrySummary.status || "观察中"} · ${entrySummary.riskLabel || current.riskLabel || "-"}</span></div>
            <div><b>当前收益</b><span>${formatSignedMoney(entrySummary.floatingPnl || current.floatingPnl || 0)} USDT · ${formatPercentValue(entrySummary.floatingPnlPct || current.floatingPnlPct || 0)}</span></div>
          </div>
        `;
      }).join("")}
    `
    : "";
  target.innerHTML = rows
    .map(
      (row) => `
        <div class="row">
          <div><b>${row.label}</b><span>${row.value}</span></div>
        </div>
      `
    )
    .join("") + watchlistMarkup;
}

function renderPortfolioWatchlist(entries = []) {
  const target = $("portfolio-watchlist");
  if (!target) return;
  if (!entries.length) {
    target.className = "portfolio-watchlist-grid empty";
    target.textContent = "保存配置后，这里会按每个币展示独立决策、独立风控和独立仓位摘要。";
    return;
  }

  target.className = "portfolio-watchlist-grid";
  target.innerHTML = entries.map((entry) => {
    const summary = entry.summary || {};
    const spot = entry.spot || {};
    const swap = entry.swap || {};
    const pnl = Number(summary.floatingPnl || 0);
    const pnlPct = Number(summary.floatingPnlPct || 0);
    const toneClass = pnl > 0 ? "positive" : pnl < 0 ? "negative" : "muted";
    const exposure = Number(summary.exposureTotal || 0);
    const spotState = spot.enabled
      ? `${spot.signal || "hold"} · ${spot.positionSide || "flat"} · ${spot.positionSize || "0"}`
      : "未启用";
    const swapState = swap.enabled
      ? `${swap.signal || "hold"} · ${swap.positionSide || "flat"} · ${swap.positionSize || "0"}`
      : "未启用";
    const strategyPill = "波段";
    return `
      <article class="portfolio-coin-card tone-${toneClass}">
        <div class="portfolio-coin-head">
          <div class="portfolio-coin-title">
            <strong>${entry.symbol || "--"}</strong>
            <span>${summary.status || "观察中"}</span>
          </div>
          <span class="pill portfolio-coin-pill">${strategyPill}${entry.overrideActive ? " · 独立参数" : ""}</span>
        </div>
        <div class="portfolio-coin-hero">
          <div class="portfolio-hero-block">
            <span>当前收益</span>
            <strong class="tone-${toneClass}">${formatSignedMoney(pnl)} USDT</strong>
            <small>${formatPercentValue(pnlPct)}</small>
          </div>
          <div class="portfolio-hero-block">
            <span>仓位名义</span>
            <strong>${formatMoney(exposure)} USDT</strong>
            <small>${summary.detail || "按每个币独立执行与监控"}</small>
          </div>
        </div>
        <div class="portfolio-coin-grid">
          <div>
            <b>现货线</b>
            <span>${spot.instId || `${entry.symbol || "--"}-USDT`}</span>
            <small>${spotState}</small>
          </div>
          <div>
            <b>永续线</b>
            <span>${swap.instId || `${entry.symbol || "--"}-USDT-SWAP`}</span>
            <small>${swapState}</small>
          </div>
          <div>
            <b>独立风控</b>
            <span>${summary.riskLabel || "等待风控拆分"}</span>
            <small>${entry.overrideActive ? `当前币已单独覆盖 ${((entry.allocation?.overrideKeys || []).length || 0)} 项参数` : "现货与永续各自按预算、上限、张数和杠杆拆分"}</small>
          </div>
          <div>
            <b>独立仓位摘要</b>
            <span>现货 ${formatMoney(spot.positionNotional || 0)}U · 永续 ${formatMoney(swap.positionNotional || 0)}U</span>
            <small>${spot.lastMessage || swap.lastMessage || "等待第一轮决策"}</small>
          </div>
        </div>
      </article>
    `;
  }).join("");
}

function renderStrategyPortfolio() {
  const automation = dashboardState.automation || {};
  const analysis = sanitizeAnalysisForSwingOnly(automation.analysis || {});
  const pipeline = automation.lastPipeline || {};
  const riskReport = automation.lastRiskReport || {};
  const executionJournal = automation.executionJournal || dashboardState.orderJournal || {};
  const saved = dashboardState.savedAutomationConfig;
  const draft = normalizeAutomationConfigForCompare(collectAutomationConfig());
  const dirty = isAutomationConfigDirty();
  const applyState = dashboardState.strategyApplyState || {};
  const watchlist = (automation.watchlist && automation.watchlist.length)
    ? automation.watchlist
    : buildDraftPortfolioEntries(draft);
  const startEq = Number(automation.sessionStartEq || 0);
  const currentEq = Number(automation.currentEq || 0);
  const pnlAmount = startEq > 0 ? currentEq - startEq : 0;
  const pnlPct = startEq > 0 ? (pnlAmount / startEq) * 100 : 0;
  const running = Boolean(automation.running);
  const riskChecks = Array.isArray(riskReport.checks) ? riskReport.checks : [];
  const activeConfig = dirty ? draft : (saved || draft);
  const activeSummary = buildStrategyFormSummary(activeConfig);
  let badge = "待同步";
  let statusTitle = "等待应用策略";
  let detail = "从研究榜单应用或保存配置后，这里会明确告诉你当前组合是否已经生效。";

  if (running) {
    badge = "运行中";
    statusTitle = applyState.title || "组合策略运行中";
    detail = `${activeSummary} · ${analysis.decisionLabel || automation.modeText || "正在轮询"}。`;
  } else if (dirty) {
    badge = "待保存";
    statusTitle = applyState.title || "参数已改动，待保存";
    detail = applyState.detail || `${activeSummary} · 当前只是草稿，保存后才会真正生效。`;
  } else if (saved) {
    badge = isSimulatedMode() ? "已生效" : "实盘就绪";
    statusTitle = applyState.title || "当前组合已生效";
    detail = applyState.detail || `${activeSummary} · ${analysis.selectedStrategyName || "当前执行参数"} 已同步。`;
  }

  $("strategy-application-status").textContent = statusTitle;
  $("strategy-application-detail").textContent = detail;
  $("strategy-application-badge").textContent = badge;
  $("portfolio-pnl-main").textContent = startEq > 0 ? `${formatPercentValue(pnlPct)}` : "--";
  $("portfolio-pnl-main").style.color =
    pnlAmount > 0 ? "var(--success)" : pnlAmount < 0 ? "var(--danger)" : "var(--text)";
  $("portfolio-pnl-sub").textContent = startEq > 0
    ? `约 ${formatSignedMoney(pnlAmount)} USDT · 从本次组合会话开始统计`
    : "收益会按组合会话持续更新，不再只靠订单详情猜测";
  $("portfolio-context-main").textContent = activeSummary;
  $("portfolio-context-sub").textContent = running
    ? `${analysis.selectedStrategyName || "波段"} · ${analysis.decisionLabel || "观察中"} · 回撤 ${analysis.pullbackPct || "--"}% · 反弹 ${analysis.reboundPct || "--"}%${analysis.liquidationBufferPct ? ` · 缓冲 ${analysis.liquidationBufferPct}%` : ""}`
    : `${watchlist.length} 个币各自独立决策、独立风控、独立仓位摘要`;
  if ($("portfolio-pipeline-main")) {
    $("portfolio-pipeline-main").textContent = pipeline.summary || (running ? "本轮执行中" : "等待下一轮编排");
  }
  if ($("portfolio-pipeline-sub")) {
    const pipelineFlags = [
      `信号 ${pipeline.signal || "idle"}`,
      `组合 ${pipeline.portfolio || "idle"}`,
      `风控 ${pipeline.risk || "idle"}`,
      `执行 ${pipeline.execution || "idle"}`,
    ];
    if (pipeline.targetCount) pipelineFlags.push(`${pipeline.targetCount} 币`);
    if (executionJournal.lastSource) pipelineFlags.push(`账本 ${executionJournal.lastSource}`);
    $("portfolio-pipeline-sub").textContent = pipelineFlags.join(" · ");
  }
  if ($("portfolio-risk-main")) {
    $("portfolio-risk-main").textContent = riskReport.stopReason || (riskReport.status === "ok" ? "护栏正常" : "等待风控检查");
  }
  if ($("portfolio-risk-sub")) {
    const topChecks = riskChecks.slice(0, 2).map((item) => {
      const marker = item?.passed ? "通过" : "拦截";
      return `${marker} · ${item?.detail || item?.name || "未命名检查"}`;
    });
    const fallbackRisk = [
      `活跃 ${riskReport.activeMarkets || 0} 市场`,
      `观察 ${riskReport.watchedSymbols || watchlist.length || 0} 币`,
      `回撤 ${analysis.pullbackPct || "--"}%`,
      `反弹 ${analysis.reboundPct || "--"}%`,
    ];
    if (analysis.liquidationBufferPct) fallbackRisk.push(`强平缓冲 ${analysis.liquidationBufferPct}%`);
    const journalBits = Number(executionJournal.totalOrders || 0)
      ? [`账本 ${executionJournal.totalOrders || 0} 单`, `成交 ${executionJournal.filledOrders || 0}`, `异常 ${(Number(executionJournal.canceledOrders || 0) + Number(executionJournal.rejectedOrders || 0))}`]
      : [];
    $("portfolio-risk-sub").textContent = [
      ...(topChecks.length ? topChecks : fallbackRisk),
      ...journalBits,
    ].join(" · ");
  }
  if ($("rail-strategy-apply")) {
    $("rail-strategy-apply").textContent = statusTitle;
  }
  if ($("rail-strategy-pnl")) {
    $("rail-strategy-pnl").textContent = startEq > 0
      ? `${formatPercentValue(pnlPct)} · ${formatSignedMoney(pnlAmount)}U`
      : "--";
    $("rail-strategy-pnl").style.color = pnlAmount > 0 ? "var(--success)" : pnlAmount < 0 ? "var(--danger)" : "var(--text)";
  }
  renderPortfolioWatchlist(watchlist);
}

function flashTicker(targetId, text) {
  const el = $(targetId);
  el.textContent = text;
  el.classList.add("flash");
  setTimeout(() => el.classList.remove("flash"), 220);
}

async function loadSavedConfig() {
  const data = await request("/api/config");
  const config = data.config || {};
  if (!Object.keys(config).length) {
    $("envPreset").value = "okx_main_demo";
    applyEnvironmentPreset("okx_main_demo");
    $("executionMode").value = "local";
    dashboardState.config = {
      envPreset: "okx_main_demo",
      baseUrl: ENV_PRESETS.okx_main_demo.baseUrl,
      simulated: true,
      executionMode: "local",
      remoteGatewayUrl: "",
    };
    return;
  }
  $("apiKey").value = "";
  $("secretKey").value = "";
  $("passphrase").value = "";
  $("remoteGatewayToken").value = "";
  $("apiKey").placeholder = config.apiKeyMask || "输入 OKX API Key";
  $("secretKey").placeholder = config.secretKeyMask || "已保存";
  $("passphrase").placeholder = config.passphraseMask || "已保存";
  $("remoteGatewayToken").placeholder = config.remoteGatewayTokenMask || "可选，远端节点鉴权令牌";
  $("executionMode").value = config.executionMode || "local";
  $("remoteGatewayUrl").value = config.remoteGatewayUrl || "";
  syncEnvironmentUi(
    {
      envPreset: config.envPreset || "custom",
      baseUrl: config.baseUrl || "https://www.okx.com",
      simulated: Boolean(config.simulated),
    },
    { preserveBaseUrl: true }
  );
  dashboardState.config = {
    envPreset: config.envPreset || "custom",
    baseUrl: config.baseUrl || "https://www.okx.com",
    simulated: Boolean(config.simulated),
    executionMode: config.executionMode || "local",
    remoteGatewayUrl: config.remoteGatewayUrl || "",
  };
  if (data.remoteConfigLoaded === false && data.remoteConfigError) {
    setMessage(`远端配置暂未读取成功：${data.remoteConfigError}`, "warn");
  }
}

function hasTradingEnvironmentChanged(previousConfig, nextConfig) {
  const prev = previousConfig || {};
  return (
    String(prev.envPreset || "") !== String(nextConfig.envPreset || "") ||
    String((prev.baseUrl || "").trim()) !== String((nextConfig.baseUrl || "").trim()) ||
    Boolean(prev.simulated) !== Boolean(nextConfig.simulated)
  );
}

async function loadAutomationConfig() {
  const data = await request("/api/automation/config");
  const config = data.config || {};
  dashboardState.savedAutomationConfig = normalizeAutomationConfigForCompare(config);
  fillAutomationForm(config);
  setStrategyApplyState("loaded", "当前组合参数已加载", buildStrategyFormSummary(config));
  if (data.remoteConfigLoaded === false && data.remoteConfigError) {
    setAutomationMessage(`远端策略配置暂未读取成功：${data.remoteConfigError}`, "warn");
  }
  renderDeskGuards();
}

async function loadMinerConfig() {
  const data = await request("/api/miner/config");
  fillMinerForm(data.config || {});
}

function renderAutomationState(state) {
  dashboardState.automation = sanitizeAutomationStateForSwingOnly(state || {});
  if (state?.executionJournal) {
    dashboardState.orderJournal = state.executionJournal;
    dashboardState.orderJournalSymbols = Array.isArray(state.executionJournal.symbols) ? state.executionJournal.symbols : [];
  }
  renderResearchState(state?.research || {});
  renderAnalysisState(state?.analysis || {});
  const running = Boolean(state?.running);
  $("bot-dot").style.background = running ? "var(--success)" : "#59636f";
  $("bot-status").textContent = state?.statusText || "未启动";
  $("bot-mode").textContent = state?.modeText || "等待配置";
  $("bot-last-cycle").textContent = state?.lastCycleAt || "-";
  $("bot-order-count").textContent = state?.orderCountToday ?? 0;
  $("bot-drawdown").textContent = `${state?.dailyDrawdownPct || "0"}%`;
  renderBotMarket("bot-spot-state", state?.markets?.spot, state?.watchlist || [], "spot");
  renderBotMarket("bot-swap-state", state?.markets?.swap, state?.watchlist || [], "swap");
  renderLogs(state?.logs || []);
  renderDeskOverview();
  renderRailStrategyControls();
  renderStrategyPortfolio();
}

async function saveConfig() {
  dashboardState.configSaving = true;
  syncConfigActionState();
  const payload = collectConfig();
  const envChanged = hasTradingEnvironmentChanged(dashboardState.config, payload);
  const targetEnv = ENV_PRESETS[inferEnvPreset(payload.envPreset, payload.baseUrl, payload.simulated)] || ENV_PRESETS.custom;
  setMessage(`正在切到 ${targetEnv.label}，请等远端确认并回填当前状态。`, "warn");

  try {
    if (envChanged) {
      try {
        await request("/api/automation/stop", { method: "POST" });
      } catch (_) {}
      $("autoAutostart").checked = false;
      $("autoAllowLiveManualOrders").checked = false;
      $("autoAllowLiveTrading").checked = false;
      $("autoAllowLiveAutostart").checked = false;
      syncRailAutomationToggles();
      renderRailStrategyControls();
      try {
        await saveAutomationConfig({ silent: true });
      } catch (_) {}
    }

    const data = await request("/api/config", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    const savedConfig = data.config || payload;
    $("apiKey").value = "";
    $("secretKey").value = "";
    $("passphrase").value = "";
    $("remoteGatewayToken").value = "";
    $("apiKey").placeholder = savedConfig.apiKeyMask || "输入 OKX API Key";
    $("secretKey").placeholder = savedConfig.secretKeyMask || "已保存";
    $("passphrase").placeholder = savedConfig.passphraseMask || "已保存";
    $("remoteGatewayToken").placeholder = savedConfig.remoteGatewayTokenMask || "可选，远端节点鉴权令牌";
    $("executionMode").value = savedConfig.executionMode || payload.executionMode || "local";
    $("remoteGatewayUrl").value = savedConfig.remoteGatewayUrl || payload.remoteGatewayUrl || "";
    syncEnvironmentUi(
      {
        envPreset: savedConfig.envPreset || payload.envPreset || "custom",
        baseUrl: savedConfig.baseUrl || payload.baseUrl || "https://www.okx.com",
        simulated: Object.prototype.hasOwnProperty.call(savedConfig, "simulated")
          ? Boolean(savedConfig.simulated)
          : Boolean(payload.simulated),
      },
      { preserveBaseUrl: true }
    );
    dashboardState.config = {
      envPreset: savedConfig.envPreset || payload.envPreset,
      baseUrl: savedConfig.baseUrl || payload.baseUrl,
      simulated: Object.prototype.hasOwnProperty.call(savedConfig, "simulated")
        ? Boolean(savedConfig.simulated)
        : Boolean(payload.simulated),
      executionMode: savedConfig.executionMode || payload.executionMode,
      remoteGatewayUrl: savedConfig.remoteGatewayUrl || payload.remoteGatewayUrl,
    };
    try {
      const health = await request("/api/health");
      applyRouteHealth(health?.okxRoute || null);
    } catch (_) {}
    await refreshSnapshot().catch(() => {});
    await refreshDeskState().catch(() => {});
    await refreshAutomationState().catch(() => {});
    if (envChanged) {
      setMessage(`${targetEnv.label} 已生效；策略已停止，自动启动与实盘权限已锁回。`, "ok");
      setAutomationMessage("切换真实/模拟盘时已自动停止策略，并锁回实盘手动/自动交易与自动启动。", "warn");
    } else {
      setMessage(`${targetEnv.label} 配置已更新并生效。`, "ok");
    }
  } finally {
    dashboardState.configSaving = false;
    syncConfigActionState();
  }
}

async function testConfig() {
  dashboardState.configTesting = true;
  syncConfigActionState();
  const payload = collectConfig();
  const targetEnv = ENV_PRESETS[inferEnvPreset(payload.envPreset, payload.baseUrl, payload.simulated)] || ENV_PRESETS.custom;
  setMessage(`正在校验 ${targetEnv.label} 链路，请稍等。`, "warn");
  try {
    const data = await request("/api/config/test", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    applyRouteHealth(data?.route || null);
    const remote = isRemoteExecutionMode() || data?.route?.executionMode === "remote";
    const routeLabel = data?.route?.healthy
      ? (remote
          ? "远端执行节点已连接"
          : `链路正常 · ${Math.round(Number(data?.route?.rest?.elapsedMs || 0))}ms`)
      : (remote
          ? `远端执行节点 · ${data?.route?.summary || data.message || "连接成功"}`
          : (data?.route?.summary || data.message || "连接成功"));
    $("health-text").textContent = routeLabel;
    $("health-text").title = data?.route?.technicalDetail || data?.route?.detail || "";
    setMessage(`${targetEnv.label} ${data.message || "连接成功"}`, data?.route?.healthy ? "ok" : "warn");
  } finally {
    dashboardState.configTesting = false;
    syncConfigActionState();
  }
}

async function saveAutomationConfig({ silent = false } = {}) {
  const payload = collectAutomationConfig();
  const overrideError = getWatchlistOverrideParseError(payload.watchlistOverrides);
  if (overrideError) {
    throw new Error(overrideError);
  }
  const data = await request("/api/automation/config", {
    method: "POST",
    body: JSON.stringify(payload),
  });
  dashboardState.savedAutomationConfig = normalizeAutomationConfigForCompare(data.config || payload);
  await loadAutomationConfig();
  setStrategyApplyState("saved", "组合策略已保存并生效", buildStrategyFormSummary(data.config || payload));
  if (!silent) {
    setAutomationMessage("策略参数已保存。每个币会按独立风控和仓位摘要继续执行。", "ok");
  }
  return data.config;
}

async function runBacktest() {
  const payload = { ...collectAutomationConfig(), ...collectResearchOptions() };
  const data = await request("/api/automation/research/backtest", {
    method: "POST",
    body: JSON.stringify(payload),
    timeoutMs: 45000,
  });
  renderResearchState(data.research || {});
  setAutomationMessage("历史回测已完成。", "ok");
}

async function runOptimizer() {
  const payload = { ...collectAutomationConfig(), ...collectResearchOptions() };
  const data = await request("/api/automation/research/optimize", {
    method: "POST",
    body: JSON.stringify(payload),
    timeoutMs: 90000,
  });
  renderResearchState(data.research || {});
  const pipeline = data.research?.pipeline || {};
  const capital = pipeline.capital || {};
  setAutomationMessage(
    `进化优化已完成：当前榜单 ${pipeline.raceSize || 10} 条，后台种群 ${pipeline.populationSize || pipeline.raceSize || 10} 条，${pipeline.evolutionLoops || 4} 轮循环${capital.horseSlots ? `，资金槽位 ${capital.horseSlots}` : ""}。`,
    "ok"
  );
}

async function runLiveAnalysis({ silent = false } = {}) {
  if (autoAnalysisInFlight) return null;
  autoAnalysisInFlight = true;
  try {
    const data = await request("/api/automation/analyze", {
      method: "POST",
      body: JSON.stringify({
        publicConfig: collectPublicConfigForAnalysis(),
        automation: collectAutomationConfig(),
      }),
      timeoutMs: 60000,
    });
    renderResearchState(data.research || {});
    renderAnalysisState(data.analysis || {});
    if (!silent) {
      setAutomationMessage(
        `联网预检已完成：${data.analysis?.selectedStrategyName || "已刷新"} · ${data.analysis?.decisionLabel || "待分析"}`,
        "ok"
      );
    }
    return data;
  } finally {
    autoAnalysisInFlight = false;
  }
}

function shouldAutoAnalyze(state = dashboardState.automation) {
  const analysis = state?.analysis || {};
  if (state?.running) return false;
  if (!analysis.lastAnalyzedAt) return true;
  const lastAt = Date.parse(analysis.lastAnalyzedAt);
  if (!Number.isFinite(lastAt)) return true;
  return (Date.now() - lastAt) >= AUTO_ANALYSIS_INTERVAL_MS;
}

function scheduleAutoAnalysis({ immediate = false, force = false } = {}) {
  if (autoAnalysisDebounceTimer) {
    clearTimeout(autoAnalysisDebounceTimer);
    autoAnalysisDebounceTimer = null;
  }
  const runner = async () => {
    const now = Date.now();
    if (autoAnalysisInFlight) return;
    if (!force && (now - autoAnalysisLastAttemptAt) < AUTO_ANALYSIS_INTERVAL_MS) return;
    if (!force && !shouldAutoAnalyze()) return;
    autoAnalysisLastAttemptAt = now;
    try {
      await runLiveAnalysis({ silent: true });
      await refreshAutomationState();
    } catch (_) {}
  };
  if (immediate) {
    void runner();
    return;
  }
  autoAnalysisDebounceTimer = setTimeout(() => {
    void runner();
  }, AUTO_ANALYSIS_DEBOUNCE_MS);
}

async function exportStrategies(index = null) {
  const payload = index === null ? {} : { index };
  const data = await request("/api/automation/research/export", {
    method: "POST",
    body: JSON.stringify(payload),
  });
  const exportInfo = data.export || {};
  const strategyLabel = exportInfo.strategyCount === 1
    ? `已导出 1 条策略到 ${exportInfo.folder}`
    : `已导出 ${exportInfo.strategyCount || 0} 条策略到 ${exportInfo.folder}`;
  setAutomationMessage(
    `${strategyLabel} · CSV: ${exportInfo.csvPath || "-"} · 清单: ${exportInfo.manifestPath || "-"}`,
    "ok"
  );
  return exportInfo;
}

function applyBestStrategy() {
  const best = dashboardState.research?.bestConfig;
  if (!best?.fastEma) {
    throw new Error("还没有可应用的最佳参数，请先运行自动优化");
  }
  const appliedConfig = { ...collectAutomationConfig(), ...best };
  fillAutomationForm(appliedConfig);
  setStrategyApplyState(
    "draft",
    "已应用最佳候选，待保存",
    `已回填 ${best.bar} · EMA ${best.fastEma}/${best.slowEma}，保存后会真正切到组合交易执行。`
  );
  setAutomationMessage(`已应用最佳参数: ${best.bar} · EMA ${best.fastEma}/${best.slowEma}`, "ok");
  scheduleAutoAnalysis({ force: true });
}

async function saveMinerConfig() {
  const payload = collectMinerConfig();
  const data = await request("/api/miner/config", {
    method: "POST",
    body: JSON.stringify(payload),
  });
  fillMinerForm(data.config || payload);
  setMinerMessage("矿机配置已保存。", "ok");
  startMinerPolling();
  return data.config;
}

async function refreshAutomationState() {
  return runSingleFlight("automationState", async () => {
    const data = await request("/api/automation/state");
    renderAutomationState(data.state || {});
    if (shouldAutoAnalyze(data.state || {})) {
      scheduleAutoAnalysis({ immediate: true });
    }
  });
}

async function startAutomation() {
  if (isLiveRouteBlocked()) {
    throw new Error(dashboardState.routeHealth?.summary || "当前实盘链路不可用");
  }
  await saveAutomationConfig();
  await request("/api/automation/start", { method: "POST" });
  await refreshAutomationState();
  setStrategyApplyState("running", "组合策略已启动", "每个币会独立决策、独立风控、独立仓位跟踪。");
  setAutomationMessage("自动量化已启动。", "ok");
}

async function stopAutomation() {
  await request("/api/automation/stop", { method: "POST" });
  await refreshAutomationState();
  setStrategyApplyState("stopped", "组合策略已停止", "当前组合已停机，仓位摘要与收益会保留展示。");
  setAutomationMessage("自动量化已停止。", "ok");
}

async function runAutomationOnce() {
  if (isLiveRouteBlocked()) {
    throw new Error(dashboardState.routeHealth?.summary || "当前实盘链路不可用");
  }
  await saveAutomationConfig();
  await request("/api/automation/run-once", { method: "POST" });
  await refreshAutomationState();
  setStrategyApplyState("checked", "已执行一轮组合检查", "当前组合的独立决策、风险和仓位摘要已经刷新。");
  setAutomationMessage("已执行一轮策略检查。", "ok");
}

async function refreshMinerOverview() {
  return runSingleFlight("minerOverview", async () => {
    const data = await request("/api/miner/overview");
    renderMinerOverview(data.overview || {});
  });
}

function buildAccountBalanceRows(account) {
  const fundingRows = (account.fundingBalances || []).map((row) => ({
    accountType: "资金",
    ccy: row.ccy || "-",
    availBal: row.availBal ?? row.bal ?? "-",
    cashBal: row.bal ?? row.cashBal ?? "-",
    eqUsd: row.usdEq ?? row.eqUsd ?? "-",
  }));
  const tradingRows = (account.balances || []).map((row) => ({
    accountType: "交易",
    ccy: row.ccy || "-",
    availBal: row.availBal ?? row.availEq ?? "-",
    cashBal: row.cashBal ?? row.bal ?? "-",
    eqUsd: row.eqUsd ?? row.usdEq ?? "-",
  }));
  return [...fundingRows, ...tradingRows];
}

function applyAccountOverview(account) {
  dashboardState.account = account;
  renderRows("balances", buildAccountBalanceRows(account).slice(0, 12), [
    { key: "accountType", label: "账户" },
    { key: "ccy", label: "币种" },
    { key: "availBal", label: "可用" },
    { key: "cashBal", label: "现金余额" },
    { key: "eqUsd", label: "折算 USD" },
  ]);
  renderRows("positions", (account.positions || []).slice(0, 8), [
    { key: "instId", label: "合约" },
    { key: "posSide", label: "方向" },
    { key: "pos", label: "持仓" },
    { key: "upl", label: "未实现 PnL" },
  ]);
  $("metric-balance-count").textContent = account.balanceCount ?? buildAccountBalanceRows(account).length;
  $("metric-position-count").textContent = account.positionCount ?? (account.positions || []).length;
  dashboardState.accountDetailsLoadedOnce = true;
  dashboardState.accountSummaryLoadedOnce = true;
  renderDeskOverview();
  rerenderSelectedOrderDetail();
}

function applyAccountSummary(account) {
  dashboardState.account = {
    ...(dashboardState.account || {}),
    ...account,
  };
  if (account.balanceCount != null) {
    $("metric-balance-count").textContent = account.balanceCount;
  }
  if (account.positionCount != null) {
    $("metric-position-count").textContent = account.positionCount;
  }
  dashboardState.accountSummaryLoadedOnce = true;
  renderDeskOverview();
  rerenderSelectedOrderDetail();
}

function applyRecentOrders(data) {
  renderOrderFeed(data);
}

function prependRecentOrder(order) {
  if (!order || !order.instId) return;
  const merged = [order, ...(dashboardState.recentOrdersAll || dashboardState.recentOrders || [])];
  const deduped = [];
  const seen = new Set();
  for (const item of merged) {
    const key = item.ordId || item.clOrdId || `${item.instId}-${item.side}-${item.px}-${item.sz}`;
    if (seen.has(key)) continue;
    seen.add(key);
    deduped.push(item);
    if (deduped.length >= 10) break;
  }
  applyRecentOrders({
    orders: deduped,
    source: dashboardState.orderFeedMeta?.source || "local_echo",
    stream: dashboardState.orderFeedMeta?.stream || null,
  });
}

async function runBitaxeAction(action) {
  const host = $("minerBitaxeActionHost").value.trim();
  const data = await request("/api/miner/bitaxe-action", {
    method: "POST",
    body: JSON.stringify({ host, action }),
  });
  setMinerMessage(`Bitaxe 动作已发送: ${action}`, "ok");
  await refreshMinerOverview().catch(() => {});
  return data.result;
}

async function startMacLotto() {
  await saveMinerConfig();
  const data = await request("/api/miner/mac-lotto/start", { method: "POST" });
  setMinerMessage("Mac 本机乐透机已启动。", "ok");
  await refreshMinerOverview().catch(() => {});
  return data.state;
}

async function stopMacLotto() {
  const data = await request("/api/miner/mac-lotto/stop", { method: "POST" });
  setMinerMessage("Mac 本机乐透机已停止。", "ok");
  await refreshMinerOverview().catch(() => {});
  return data.state;
}

async function refreshSnapshot() {
  return runSingleFlight("snapshot", async () => {
    const account = await request("/api/account/overview");
    applyAccountOverview(account);
  });
}

async function refreshOrders() {
  return runSingleFlight("orders", async () => {
    const data = await request("/api/orders/recent");
    applyRecentOrders(data);
  });
}

async function refreshDeskState() {
  return runSingleFlight("deskState", async () => {
    const data = await request("/api/focus-snapshot", { timeoutMs: 30000 });
    if (data.account) applyAccountSummary(data.account);
    if (data.automationState) renderAutomationState(data.automationState);
    if (data.minerOverview) renderMinerOverview(data.minerOverview);
    return data;
  });
}

async function refreshMarket() {
  return runSingleFlight("market", async () => {
    const spot = $("spotInstId").value.trim();
    const swap = $("swapInstId").value.trim();

    const [spotData, swapData] = await Promise.all([
      request(`/api/market/ticker?instId=${encodeURIComponent(spot)}`),
      request(`/api/market/ticker?instId=${encodeURIComponent(swap)}`),
      loadHistoricalCandles(),
    ]);

    const spotTicker = spotData.ticker?.[0];
    const swapTicker = swapData.ticker?.[0];
    if (spotTicker?.instId) liveMarketState.tickers[spotTicker.instId] = spotTicker;
    if (swapTicker?.instId) liveMarketState.tickers[swapTicker.instId] = swapTicker;
    renderSelectedTickers();
    renderMainstreamBoard();
    rerenderSelectedOrderDetail();
  });
}

function buildOrderPayload(form) {
  const formData = new FormData(form);
  const payload = {};
  for (const [key, value] of formData.entries()) {
    if (!value) continue;
    payload[key] = value;
  }
  form.querySelectorAll("[data-source]").forEach((node) => {
    payload[node.name] = $(node.dataset.source).value.trim();
  });
  if (form.id === "swap-order-form") {
    payload.reduceOnly = form.querySelector('input[name="reduceOnly"]').checked;
  }
  return payload;
}

async function submitOrder(event) {
  event.preventDefault();
  if (isLiveRouteBlocked()) {
    throw new Error(dashboardState.routeHealth?.summary || "当前实盘链路不可用");
  }
  if (!isSimulatedMode() && !$("autoAllowLiveManualOrders").checked) {
    throw new Error("当前是实盘，未开启“允许手动实盘下单”");
  }
  const form = event.currentTarget;
  const payload = buildOrderPayload(form);
  const result = await request("/api/order/place", {
    method: "POST",
    body: JSON.stringify(payload),
  });
  const elapsedMs = result.elapsedMs || result.result?._clientElapsedMs;
  setMessage(
    `下单请求已发送${elapsedMs ? ` · ${elapsedMs} ms` : ""}: ${JSON.stringify(result.result.data?.[0] || result.result)}`,
    "ok"
  );
  prependRecentOrder(result.result?.data?.[0] || {});
  window.setTimeout(() => {
    refreshOrders().catch(() => {});
  }, 600);
  window.setTimeout(() => {
    refreshOrders().catch(() => {});
  }, 1800);
  window.setTimeout(() => {
    refreshOrders().catch(() => {});
  }, 4500);
}

function startAutomationPolling() {
  if (automationPollTimer) {
    clearInterval(automationPollTimer);
  }
  automationPollTimer = setInterval(() => {
    refreshAutomationState().catch(() => {});
  }, 5000);
}

function startSnapshotPolling() {
  if (snapshotPollTimer) {
    clearInterval(snapshotPollTimer);
  }
  snapshotPollTimer = setInterval(() => {
    refreshDeskState().catch(() => {});
  }, 15000);
}

function syncOrderPolling() {
  if (orderPollTimer) {
    clearInterval(orderPollTimer);
    orderPollTimer = null;
  }
  if (dashboardState.currentView !== "orders") {
    return;
  }
  orderPollTimer = setInterval(() => {
    refreshOrders().catch(() => {});
  }, 3000);
}

function startMinerPolling() {
  if (minerPollTimer) {
    clearInterval(minerPollTimer);
  }
  const seconds = Math.max(1, Number($("minerRefreshSeconds").value || 20));
  minerPollTimer = setInterval(() => {
    refreshMinerOverview().catch(() => {});
  }, seconds * 1000);
}

async function boot() {
  let initialView = "focus";
  try {
    initialView = window.localStorage.getItem("okx-desk-view") || "focus";
  } catch (_) {}
  setWorkspaceView(initialView, { persist: false, scroll: false });
  setupTunnelBackground();
  setPendingEnvironmentUi();

  try {
    const health = await request("/api/health");
    const route = health?.okxRoute || {};
    applyRouteHealth(route);
    if (route?.detail) {
      const remote = route?.executionMode === "remote" || isRemoteExecutionMode();
      $("health-text").textContent = route.healthy
        ? (remote ? `远端执行节点已连接 · ${route.summary || route.detail}` : `本地服务已启动 · ${route.summary || route.detail}`)
        : (remote ? `远端执行节点 · ${route.summary || route.detail}` : (route.summary || route.detail));
    } else {
      $("health-text").textContent = isRemoteExecutionMode() ? "远端执行节点已配置" : "本地服务已启动";
    }
  } catch (err) {
    $("health-text").textContent = "服务不可用";
  }

  try {
    await loadSavedConfig();
    await loadAutomationConfig();
    await loadMinerConfig();
    await refreshDeskState();
  } catch (err) {
    setMessage(err.message, "err");
    setAutomationMessage(err.message, "err");
    setMinerMessage(err.message, "err");
  }

  $("envPreset").addEventListener("change", () => {
    applyEnvironmentPreset($("envPreset").value);
    restartLiveFeedSoon();
    scheduleAutoAnalysis({ force: true });
  });

  $("baseUrl").addEventListener("input", () => {
    updateEndpointCards($("envPreset").value);
    updateQuickState();
    restartLiveFeedSoon();
    scheduleAutoAnalysis({ force: true });
  });

  document.querySelectorAll("[data-env-preset]").forEach((button) => {
    button.addEventListener("click", () => {
      $("envPreset").value = button.dataset.envPreset;
      applyEnvironmentPreset(button.dataset.envPreset);
      restartLiveFeedSoon();
      scheduleAutoAnalysis({ force: true });
    });
  });

  document.querySelectorAll("[data-view]").forEach((button) => {
    button.addEventListener("click", () => {
      setWorkspaceView(button.dataset.view);
    });
  });

  document.querySelectorAll("[data-pair-preset]").forEach((button) => {
    button.addEventListener("click", () => {
      setPairPreset(button.dataset.pairPreset, { refresh: true });
      scheduleAutoAnalysis({ force: true });
    });
  });

  document.querySelectorAll("[data-strategy-preset]").forEach((button) => {
    button.addEventListener("click", () => {
      applyStrategyPreset(button.dataset.strategyPreset, { refresh: true });
      scheduleAutoAnalysis({ force: true });
    });
  });

  $("spotInstId").addEventListener("input", updateQuickState);
  $("swapInstId").addEventListener("input", updateQuickState);
  $("autoWatchlistSymbols").addEventListener("input", updateQuickState);
  $("autoWatchlistSymbols").addEventListener("input", () => {
    renderWatchlistOverrideEditor($("autoWatchlistOverrides").value);
    renderStrategyPortfolio();
  });
  if ($("watchlistOverrideEditor")) {
    $("watchlistOverrideEditor").addEventListener("input", (event) => {
      if (!event.target.closest("[data-override-field]")) return;
      syncWatchlistOverridesValueFromEditor();
      renderStrategyPortfolio();
    });
    $("watchlistOverrideEditor").addEventListener("change", (event) => {
      if (!event.target.closest("[data-override-field]")) return;
      syncWatchlistOverridesValueFromEditor();
      renderDeskGuards();
      renderDeskOverview();
      scheduleAutoAnalysis({ force: true });
    });
  }
  if ($("watchlistOverrideReset")) {
    $("watchlistOverrideReset").addEventListener("click", () => {
      $("autoWatchlistOverrides").value = "";
      renderWatchlistOverrideEditor("");
      renderDeskGuards();
      renderDeskOverview();
      renderStrategyPortfolio();
      scheduleAutoAnalysis({ force: true });
    });
  }
  if ($("orderSpotInstMirror")) {
    $("orderSpotInstMirror").addEventListener("input", () => {
      $("spotInstId").value = $("orderSpotInstMirror").value.trim().toUpperCase();
      updateQuickState();
    });
    $("orderSpotInstMirror").addEventListener("change", () => {
      $("spotInstId").value = $("orderSpotInstMirror").value.trim().toUpperCase();
      updateQuickState();
      restartLiveFeedSoon();
      scheduleAutoAnalysis({ force: true });
    });
  }
  if ($("orderSwapInstMirror")) {
    $("orderSwapInstMirror").addEventListener("input", () => {
      $("swapInstId").value = $("orderSwapInstMirror").value.trim().toUpperCase();
      updateQuickState();
    });
    $("orderSwapInstMirror").addEventListener("change", () => {
      $("swapInstId").value = $("orderSwapInstMirror").value.trim().toUpperCase();
      updateQuickState();
      restartLiveFeedSoon();
      scheduleAutoAnalysis({ force: true });
    });
  }
  $("spotInstId").addEventListener("change", () => {
    restartLiveFeedSoon();
    scheduleAutoAnalysis({ force: true });
  });
  $("swapInstId").addEventListener("change", () => {
    restartLiveFeedSoon();
    scheduleAutoAnalysis({ force: true });
  });
  $("autoWatchlistSymbols").addEventListener("change", () => {
    renderWatchlistOverrideEditor($("autoWatchlistOverrides").value);
    scheduleAutoAnalysis({ force: true });
  });
  $("marketBar").addEventListener("change", restartLiveFeedSoon);

  [
    "executionMode",
    "remoteGatewayUrl",
    "apiKey",
    "secretKey",
    "passphrase",
    "remoteGatewayToken",
    "persist",
  ].forEach((id) => {
    const node = $(id);
    if (!node) return;
    const syncDraft = () => syncConfigActionState();
    node.addEventListener("change", syncDraft);
    if (node.tagName === "INPUT" && node.type !== "checkbox") {
      node.addEventListener("input", syncDraft);
    }
  });

  [
    "simulated",
    "autoBar",
    "autoFastEma",
    "autoSlowEma",
    "autoPollSeconds",
    "autoCooldownSeconds",
    "autoMaxOrdersPerDay",
    "autoSpotEnabled",
    "autoWatchlistOverrides",
    "autoSpotQuoteBudget",
    "autoSpotMaxExposure",
    "autoSwapEnabled",
    "autoSwapContracts",
    "autoSwapTdMode",
    "autoSwapStrategyMode",
    "autoSwapLeverage",
    "autoStopLossPct",
    "autoTakeProfitPct",
    "autoMaxDailyLossPct",
    "autoAutostart",
    "autoAllowLiveManualOrders",
    "autoAllowLiveTrading",
    "autoAllowLiveAutostart",
    "researchHistoryLimit",
    "researchRaceSize",
    "researchEvolutionLoops",
    "researchDepth",
    "researchIncludeAltBars",
    "researchEnableHybrid",
    "researchEnableFineTune",
  ].forEach((id) => {
    const syncAnalysis = () => {
      renderDeskGuards();
      renderDeskOverview();
      applyRouteHealth(dashboardState.routeHealth, { preserveMessage: true });
      scheduleAutoAnalysis({ force: true });
    };
    $(id).addEventListener("change", syncAnalysis);
    if ($(id).tagName === "INPUT" && $(id).type !== "checkbox") {
      $(id).addEventListener("input", syncAnalysis);
    }
  });

  RAIL_AUTOMATION_TOGGLES.forEach(([formId, railId]) => {
    const form = $(formId);
    const rail = $(railId);
    if (!form || !rail) return;
    form.addEventListener("change", () => {
      rail.checked = form.checked;
      renderRailStrategyControls();
    });
    rail.addEventListener("change", () => {
      form.checked = rail.checked;
      form.dispatchEvent(new Event("change", { bubbles: true }));
    });
  });

  $("save-config").addEventListener("click", async () => {
    try {
      await saveConfig();
    } catch (err) {
      setMessage(err.message, "err");
    }
  });

  $("test-config").addEventListener("click", async () => {
    try {
      await testConfig();
    } catch (err) {
      $("health-dot").style.background = "var(--danger)";
      $("health-text").textContent = "连接失败";
      setMessage(err.message, "err");
    }
  });

  $("refresh-all").addEventListener("click", async () => {
    try {
      await Promise.all([
        refreshDeskState(),
        refreshOrders(),
        refreshMarket(),
      ]);
      setMessage("快照已更新。", "ok");
    } catch (err) {
      setMessage(err.message, "err");
    }
  });

  $("refresh-market").addEventListener("click", async () => {
    try {
      await refreshMarket();
      await startLiveFeeds();
    } catch (err) {
      setMessage(err.message, "err");
    }
  });

  $("restart-live-feed").addEventListener("click", async () => {
    try {
      await startLiveFeeds();
      setMessage("实时行情已重新连接。", "ok");
    } catch (err) {
      setMessage(err.message, "err");
    }
  });

  $("refresh-orders").addEventListener("click", async () => {
    try {
      await refreshOrders();
    } catch (err) {
      setMessage(err.message, "err");
    }
  });

  $("save-automation").addEventListener("click", async () => {
    try {
      await saveAutomationConfig();
    } catch (err) {
      setAutomationMessage(err.message, "err");
    }
  });

  $("rail-save-automation").addEventListener("click", async () => {
    try {
      await saveAutomationConfig();
    } catch (err) {
      setAutomationMessage(err.message, "err");
    }
  });

  $("save-miner-config").addEventListener("click", async () => {
    try {
      await saveMinerConfig();
    } catch (err) {
      setMinerMessage(err.message, "err");
    }
  });

  $("refresh-miner").addEventListener("click", async () => {
    try {
      await refreshMinerOverview();
      setMinerMessage("矿机概览已刷新。", "ok");
    } catch (err) {
      setMinerMessage(err.message, "err");
    }
  });

  $("miner-mac-start").addEventListener("click", async () => {
    try {
      await startMacLotto();
    } catch (err) {
      setMinerMessage(err.message, "err");
    }
  });

  $("miner-mac-stop").addEventListener("click", async () => {
    try {
      await stopMacLotto();
    } catch (err) {
      setMinerMessage(err.message, "err");
    }
  });

  if ($("miner-identify")) {
    $("miner-identify").addEventListener("click", async () => {
      try {
        await runBitaxeAction("identify");
      } catch (err) {
        setMinerMessage(err.message, "err");
      }
    });
  }

  if ($("miner-pause")) {
    $("miner-pause").addEventListener("click", async () => {
      try {
        await runBitaxeAction("pause");
      } catch (err) {
        setMinerMessage(err.message, "err");
      }
    });
  }

  if ($("miner-resume")) {
    $("miner-resume").addEventListener("click", async () => {
      try {
        await runBitaxeAction("resume");
      } catch (err) {
        setMinerMessage(err.message, "err");
      }
    });
  }

  if ($("miner-restart")) {
    $("miner-restart").addEventListener("click", async () => {
      try {
        await runBitaxeAction("restart");
      } catch (err) {
        setMinerMessage(err.message, "err");
      }
    });
  }

  $("run-automation-once").addEventListener("click", async () => {
    try {
      await runAutomationOnce();
    } catch (err) {
      setAutomationMessage(err.message, "err");
      await refreshAutomationState().catch(() => {});
    }
  });

  $("rail-run-automation-once").addEventListener("click", async () => {
    try {
      await runAutomationOnce();
    } catch (err) {
      setAutomationMessage(err.message, "err");
      await refreshAutomationState().catch(() => {});
    }
  });

  $("start-automation").addEventListener("click", async () => {
    try {
      await startAutomation();
    } catch (err) {
      setAutomationMessage(err.message, "err");
      await refreshAutomationState().catch(() => {});
    }
  });

  $("rail-start-automation").addEventListener("click", async () => {
    try {
      await startAutomation();
    } catch (err) {
      setAutomationMessage(err.message, "err");
      await refreshAutomationState().catch(() => {});
    }
  });

  $("rail-strategy-toggle")?.addEventListener("click", async () => {
    try {
      if (dashboardState.automation?.running) {
        await stopAutomation();
      } else {
        await startAutomation();
      }
    } catch (err) {
      setAutomationMessage(err.message, "err");
      await refreshAutomationState().catch(() => {});
    }
  });

  $("stop-automation").addEventListener("click", async () => {
    try {
      await stopAutomation();
    } catch (err) {
      setAutomationMessage(err.message, "err");
    }
  });

  $("rail-stop-automation").addEventListener("click", async () => {
    try {
      await stopAutomation();
    } catch (err) {
      setAutomationMessage(err.message, "err");
    }
  });

  $("run-backtest").addEventListener("click", async () => {
    try {
      setAutomationMessage("正在跑历史回测...", "");
      await runBacktest();
    } catch (err) {
      setAutomationMessage(err.message, "err");
    }
  });

  $("run-optimizer").addEventListener("click", async () => {
    try {
      const options = collectResearchOptions();
      setAutomationMessage(
        `正在跑持续赛马池：榜单前 ${options.raceSize}，后台种群 ${Math.max(options.raceSize * 4, 24)}，准备做 ${options.evolutionLoops} 轮杂交、微调和进化...`,
        ""
      );
      await runOptimizer();
    } catch (err) {
      setAutomationMessage(err.message, "err");
    }
  });

  $("apply-best-strategy").addEventListener("click", () => {
    try {
      applyBestStrategy();
    } catch (err) {
      setAutomationMessage(err.message, "err");
    }
  });

  $("export-all-strategies").addEventListener("click", async () => {
    try {
      setAutomationMessage("正在导出当前榜单策略包...", "");
      await exportStrategies();
    } catch (err) {
      setAutomationMessage(err.message, "err");
    }
  });

  $("cockpit-refresh").addEventListener("click", async () => {
    try {
      await Promise.all([
        refreshDeskState(),
        refreshOrders(),
        refreshMarket(),
      ]);
      setMessage("驾驶舱和订单、行情都已刷新。", "ok");
    } catch (err) {
      setMessage(err.message, "err");
    }
  });

  $("cockpit-analyze").addEventListener("click", async () => {
    try {
      setAutomationMessage("正在立即联网预检并刷新本轮最优策略...", "");
      await runLiveAnalysis({ silent: false });
      await refreshAutomationState().catch(() => {});
    } catch (err) {
      setAutomationMessage(err.message, "err");
    }
  });

  $("cockpit-run-once").addEventListener("click", async () => {
    try {
      await runAutomationOnce();
    } catch (err) {
      setAutomationMessage(err.message, "err");
    }
  });

  $("cockpit-stop").addEventListener("click", async () => {
    try {
      await stopAutomation();
      setAutomationMessage("已执行一键停机。", "ok");
    } catch (err) {
      setAutomationMessage(err.message, "err");
    }
  });

  $("spot-order-form").addEventListener("submit", async (event) => {
    try {
      await submitOrder(event);
    } catch (err) {
      setMessage(err.message, "err");
    }
  });

  $("swap-order-form").addEventListener("submit", async (event) => {
    try {
      await submitOrder(event);
    } catch (err) {
      setMessage(err.message, "err");
    }
  });

  startAutomationPolling();
  startSnapshotPolling();
  startMinerPolling();
  syncOrderPolling();
  updateQuickState();
  renderDeskOverview();
  renderRailStrategyControls();
  renderMainstreamBoard();
  renderMarketChart();
  await Promise.all([
    refreshDeskState().catch(() => {}),
    refreshMarket().catch(() => {}),
  ]);
  scheduleAutoAnalysis({ immediate: true, force: true });
  await startLiveFeeds().catch((err) => {
    setLiveFeedStatus("err", "实时行情启动失败", err.message);
  });
  window.addEventListener("beforeunload", () => {
    if (automationPollTimer) clearInterval(automationPollTimer);
    if (snapshotPollTimer) clearInterval(snapshotPollTimer);
    if (minerPollTimer) clearInterval(minerPollTimer);
    if (orderPollTimer) clearInterval(orderPollTimer);
    if (autoAnalysisDebounceTimer) clearTimeout(autoAnalysisDebounceTimer);
    clearLiveFeedTimers();
    closeSocket(liveMarketState.tickerSocket);
    closeSocket(liveMarketState.candleSocket);
    if (tunnelBgState) {
      tunnelBgState.cleanup();
      tunnelBgState = null;
    }
  });
}

boot();
