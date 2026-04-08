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
};

const STRATEGY_PRESETS = {
  dual_engine: {
    label: "标准双引擎",
    description: "均衡版现货+永续策略，适合常规 BTC / ETH / SOL 轮询。",
    config: {
      strategyPreset: "dual_engine",
      bar: "5m",
      fastEma: 9,
      slowEma: 21,
      pollSeconds: 20,
      cooldownSeconds: 180,
      maxOrdersPerDay: 20,
      spotEnabled: true,
      spotQuoteBudget: "100",
      spotMaxExposure: "300",
      swapEnabled: true,
      swapContracts: "1",
      swapTdMode: "cross",
      swapStrategyMode: "trend_follow",
      swapLeverage: "5",
      stopLossPct: "1.2",
      takeProfitPct: "2.4",
      maxDailyLossPct: "3.0",
      autostart: false,
      allowLiveTrading: false,
      allowLiveAutostart: false,
      enforceNetMode: true,
    },
  },
  btc_lotto: {
    label: "BTC 乐透机",
    description: "BTC 专用的小仓位高频模式，更快节奏、更紧风控，适合先在模拟盘跑波动试验。",
    config: {
      strategyPreset: "btc_lotto",
      spotInstId: "BTC-USDT",
      swapInstId: "BTC-USDT-SWAP",
      bar: "1m",
      fastEma: 4,
      slowEma: 11,
      pollSeconds: 8,
      cooldownSeconds: 30,
      maxOrdersPerDay: 80,
      spotEnabled: true,
      spotQuoteBudget: "30",
      spotMaxExposure: "120",
      swapEnabled: true,
      swapContracts: "1",
      swapTdMode: "cross",
      swapStrategyMode: "trend_follow",
      swapLeverage: "5",
      stopLossPct: "0.7",
      takeProfitPct: "1.4",
      maxDailyLossPct: "2.0",
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
    label: "交易执行",
    chip: "Trade",
    description: "连接配置、账户快照、订单和手动下单。",
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
  miner: null,
  research: null,
  routeHealth: null,
  config: null,
  currentView: "focus",
  recentOrders: [],
  recentOrdersAll: [],
  orderFeedMeta: null,
  selectedOrderKey: null,
  accountDetailsLoadedOnce: false,
  accountSummaryLoadedOnce: false,
  ordersLoadedOnce: false,
};

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
  $("active-pair-label").textContent = "正在同步组合";
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
    $("spot-order-form")?.querySelector('button[type="submit"]'),
    $("swap-order-form")?.querySelector('button[type="submit"]'),
  ].forEach((node) => {
    if (!node) return;
    node.disabled = disabled;
    node.title = disabled ? disabledTitle : "";
  });
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
  if (key === "trade" && !dashboardState.ordersLoadedOnce) {
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
      svg.innerHTML = emptyMarkup;
    }
    if (dockSvg) {
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

  const width = 960;
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
    const dockBottom = dockHeight - dockPaddingY;
    const dockStep = samples.length > 1 ? (width - dockPaddingX * 2) / (samples.length - 1) : 0;
    const dockPoints = samples.map((point, index) => {
      const x = dockPaddingX + dockStep * index;
      const y = dockBottom - ((point.eq - min) / span) * (dockHeight - dockPaddingY * 2);
      return [x, y];
    });
    const dockLinePath = buildSmoothPath(dockPoints);
    const dockAreaPath = buildAreaPath(dockPoints, dockBottom);
    const dockLatestPoint = dockPoints[dockPoints.length - 1];
    const dockGridMarkup = buildGridMarkup(width, dockHeight, dockPaddingX, width - dockPaddingX, dockPaddingY, dockBottom, min, max, formatMoney, false);
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
      <rect x="0" y="0" width="960" height="${dockHeight}" rx="14" fill="rgba(255,255,255,0.015)"></rect>
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

function renderAnalysisState(analysis) {
  const data = analysis || {};
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
      ? `${data.marketRegime} · 现货 ${data.spotTrend || "--"} · 永续 ${data.swapTrend || "--"}`
      : "等待波动、价差、资金费和趋势分析";
  $("analysis-refresh").textContent =
    data.lastAnalyzedAt
      ? `${data.lastAnalyzedAt} · 波动 ${data.volatilityPct || "--"}% · 价差 ${data.spreadPct || "--"}% · 资金费 ${data.fundingRatePct || "--"}%`
      : "等待最新分析时间";
  const reasonBits = [];
  if (data.summary) reasonBits.push(data.summary);
  if (data.blockers?.length) reasonBits.push(`阻断: ${data.blockers.join("；")}`);
  else if (data.warnings?.length) reasonBits.push(`提醒: ${data.warnings.join("；")}`);
  $("analysis-reason").textContent =
    reasonBits.join(" | ") || "这层会决定是允许开新仓、只观察，还是直接跳过。";

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
    if (data.fundingRatePct) dockSummaryBits.push(`资金费 ${data.fundingRatePct}%`);
    if (data.selectedReturnPct) dockSummaryBits.push(`收益 ${formatPercentValue(data.selectedReturnPct)}`);
    if (data.blockers?.length) dockSummaryBits.push(`阻断 ${data.blockers[0]}`);
    else if (data.warnings?.length) dockSummaryBits.push(`提醒 ${data.warnings[0]}`);
    dockSub.textContent = dockSummaryBits.join(" · ") || "顶部固定显示当前策略、联网判断和执行环境。";
  }
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
  const totalEq = Number(summary.displayTotalEq || summary.totalEq || automation.currentEq || 0);
  const currentEq = Number(automation.currentEq || summary.displayTotalEq || summary.totalEq || 0);
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
  const dockPnlMain = $("dock-pnl-main");
  const dockPnlSub = $("dock-pnl-sub");
  const minerDailyUsd = Number(minerProgress.dailyUsd || 0);
  const railBalanceMain = $("rail-balance-main");
  const railBalanceSub = $("rail-balance-sub");
  const railMinerMain = $("rail-miner-main");
  const railMinerSub = $("rail-miner-sub");
  const balanceBreakdown = summary.displayBreakdown
    || (fundingTotalEq > 0
      ? `资金账户 ${formatMoney(fundingTotalEq)} USDT · 交易账户 ${formatMoney(tradingTotalEq)} USDT`
      : (tradingTotalEq > 0 ? `交易账户 ${formatMoney(tradingTotalEq)} USDT` : ""));

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

  $("desk-mode").textContent = `${modeText} · ${automation.statusText || "未启动"}`;
  $("desk-mode-sub").textContent = automation.modeText || "模拟 / 实盘 + 策略状态";

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
  if (railMinerMain) {
    railMinerMain.textContent = formatMoney(minerDailyUsd || 0);
  }
  if (railMinerSub) {
    railMinerSub.textContent = "USDT / 天";
  }

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

  $("active-pair-label").textContent = pairMatch
    ? `${pairMatch[1].spot} / ${pairMatch[1].swap}`
    : `${spot || "-"} / ${swap || "-"}`;
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
  return {
    strategyPreset: $("autoStrategyPreset").value || "dual_engine",
    spotInstId: $("spotInstId").value.trim(),
    swapInstId: $("swapInstId").value.trim(),
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

function setStrategyPresetUi(presetKey) {
  const preset = STRATEGY_PRESETS[presetKey] || STRATEGY_PRESETS.dual_engine;
  $("autoStrategyPreset").value = presetKey in STRATEGY_PRESETS ? presetKey : "dual_engine";
  $("active-strategy-label").textContent = preset.label;
  $("active-strategy-description").textContent = preset.description;
  document.querySelectorAll("[data-strategy-preset]").forEach((button) => {
    button.classList.toggle("active", button.dataset.strategyPreset === $("autoStrategyPreset").value);
  });
}

function fillAutomationForm(config) {
  $("autoStrategyPreset").value = config.strategyPreset || "dual_engine";
  $("spotInstId").value = config.spotInstId || "BTC-USDT";
  $("swapInstId").value = config.swapInstId || "BTC-USDT-SWAP";
  $("autoBar").value = config.bar || "5m";
  $("autoFastEma").value = config.fastEma ?? 9;
  $("autoSlowEma").value = config.slowEma ?? 21;
  $("autoPollSeconds").value = config.pollSeconds ?? 20;
  $("autoCooldownSeconds").value = config.cooldownSeconds ?? 180;
  $("autoMaxOrdersPerDay").value = config.maxOrdersPerDay ?? 20;
  $("autoSpotEnabled").checked = Boolean(config.spotEnabled);
  $("autoSpotQuoteBudget").value = config.spotQuoteBudget ?? "100";
  $("autoSpotMaxExposure").value = config.spotMaxExposure ?? "300";
  $("autoSwapEnabled").checked = Boolean(config.swapEnabled);
  $("autoSwapContracts").value = config.swapContracts ?? "1";
  $("autoSwapTdMode").value = config.swapTdMode || "cross";
  $("autoSwapStrategyMode").value = config.swapStrategyMode || "trend_follow";
  $("autoSwapLeverage").value = config.swapLeverage ?? "5";
  $("autoStopLossPct").value = config.stopLossPct ?? "1.2";
  $("autoTakeProfitPct").value = config.takeProfitPct ?? "2.4";
  $("autoMaxDailyLossPct").value = config.maxDailyLossPct ?? "3.0";
  $("autoAutostart").checked = Boolean(config.autostart);
  $("autoAllowLiveManualOrders").checked = Boolean(config.allowLiveManualOrders);
  $("autoAllowLiveTrading").checked = Boolean(config.allowLiveTrading);
  $("autoAllowLiveAutostart").checked = Boolean(config.allowLiveAutostart);
  $("autoEnforceNetMode").checked = config.enforceNetMode !== false;
  setStrategyPresetUi(config.strategyPreset || "dual_engine");
  updateQuickState();
  renderDeskGuards();
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
  const preset = STRATEGY_PRESETS[presetKey];
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
  const working = orders.filter((item) => ["live", "effective", "partially_filled"].includes(item.state)).length;
  const filled = orders.filter((item) => item.state === "filled").length;
  $("order-summary-count").textContent = String((data?.orders || []).length || 0);
  $("order-summary-working").textContent = String(working);
  $("order-summary-filled").textContent = String(filled);
  $("order-summary-source").textContent = source;
}

function renderOrderDetail(order, meta = {}) {
  const target = $("orderDetail");
  if (!order) {
    target.className = "order-detail empty";
    target.innerHTML = `
      <div class="order-detail-empty">
        <strong>选中一笔订单</strong>
        <span>这里会显示状态、成交、价格、时间、订单号和执行标记。</span>
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
    </div>
    <div class="order-detail-grid">
      <div><b>委托数量</b><span>${escapeHtml(requestSize)}</span></div>
      <div><b>保证金 / 模式</b><span>${escapeHtml(order.tdMode || "--")}</span></div>
      <div><b>订单号</b><span>${escapeHtml(order.ordId || "--")}</span></div>
      <div><b>Client ID</b><span>${escapeHtml(order.clOrdId || "--")}</span></div>
      <div><b>创建时间</b><span>${escapeHtml(createdAt)}</span></div>
      <div><b>最近更新时间</b><span>${escapeHtml(updatedAt)}</span></div>
      <div><b>手续费</b><span>${escapeHtml(formatOrderValue(order.fee))}</span></div>
      <div><b>订单标签</b><span>${escapeHtml(order.tag || "--")}</span></div>
      <div><b>仅减仓</b><span>${order.reduceOnly ? "是" : "否"}</span></div>
      <div><b>成交来源</b><span>${escapeHtml(meta?.stream?.connected ? "实时推送" : source)}</span></div>
    </div>
    <div class="order-detail-note">
      最近一笔会优先高亮；如果下单后私有 WS 还没回报，这里会先显示本地回执，再自动切换成交易所最新状态。
    </div>
  `;
}

function renderOrderFeed(data) {
  const allOrders = data?.orders || [];
  const orders = allOrders.slice(0, 10);
  const target = $("recentOrders");
  dashboardState.recentOrdersAll = allOrders;
  dashboardState.recentOrders = orders;
  dashboardState.orderFeedMeta = { source: data?.source || "rest", stream: data?.stream || null };
  renderOrderSummary(data, allOrders);

  if (!orders.length) {
    target.className = "orders-feed empty";
    target.innerHTML = '<div class="empty">暂无订单数据</div>';
    dashboardState.selectedOrderKey = null;
    renderOrderDetail(null);
    $("metric-order-count").textContent = String(allOrders.length || 0);
    dashboardState.ordersLoadedOnce = true;
    return;
  }

  if (!dashboardState.selectedOrderKey || !orders.some((item) => getOrderKey(item) === dashboardState.selectedOrderKey)) {
    dashboardState.selectedOrderKey = getOrderKey(orders[0]);
  }

  target.className = "orders-feed";
  target.innerHTML = orders.map((order) => {
    const key = getOrderKey(order);
    const tone = getOrderTone(order.state);
    const sideClass = order.side === "sell" ? "down" : "up";
    const activeClass = key === dashboardState.selectedOrderKey ? "active" : "";
    const targetPrice = order.ordType === "market" ? "市价" : formatOrderValue(order.px);
    return `
      <button type="button" class="order-card ${sideClass} ${activeClass}" data-order-key="${escapeHtml(key)}">
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
        <div class="order-card-foot">
          <span>${escapeHtml(formatOrderTime(order.cTime))}</span>
          <span>${escapeHtml(order.clOrdId || order.ordId || "--")}</span>
        </div>
      </button>
    `;
  }).join("");

  target.querySelectorAll(".order-card").forEach((button) => {
    button.addEventListener("click", () => {
      dashboardState.selectedOrderKey = button.dataset.orderKey;
      renderOrderFeed({
        orders: dashboardState.recentOrdersAll,
        source: dashboardState.orderFeedMeta?.source,
        stream: dashboardState.orderFeedMeta?.stream,
      });
    });
  });

  const selectedOrder = orders.find((item) => getOrderKey(item) === dashboardState.selectedOrderKey) || orders[0];
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
        fillAutomationForm({ ...collectAutomationConfig(), ...(item.fullConfig || item.config) });
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

function renderBotMarket(targetId, market) {
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
  target.innerHTML = rows
    .map(
      (row) => `
        <div class="row">
          <div><b>${row.label}</b><span>${row.value}</span></div>
        </div>
      `
    )
    .join("");
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
  fillAutomationForm(config);
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
  dashboardState.automation = state || {};
  renderResearchState(state?.research || {});
  renderAnalysisState(state?.analysis || {});
  const running = Boolean(state?.running);
  $("bot-dot").style.background = running ? "var(--success)" : "#59636f";
  $("bot-status").textContent = state?.statusText || "未启动";
  $("bot-mode").textContent = state?.modeText || "等待配置";
  $("bot-last-cycle").textContent = state?.lastCycleAt || "-";
  $("bot-order-count").textContent = state?.orderCountToday ?? 0;
  $("bot-drawdown").textContent = `${state?.dailyDrawdownPct || "0"}%`;
  renderBotMarket("bot-spot-state", state?.markets?.spot);
  renderBotMarket("bot-swap-state", state?.markets?.swap);
  renderLogs(state?.logs || []);
  renderDeskOverview();
}

async function saveConfig() {
  const payload = collectConfig();
  const envChanged = hasTradingEnvironmentChanged(dashboardState.config, payload);

  if (envChanged) {
    try {
      await request("/api/automation/stop", { method: "POST" });
    } catch (_) {}
    $("autoAutostart").checked = false;
    $("autoAllowLiveManualOrders").checked = false;
    $("autoAllowLiveTrading").checked = false;
    $("autoAllowLiveAutostart").checked = false;
    try {
      await saveAutomationConfig({ silent: true });
    } catch (_) {}
  }

  const data = await request("/api/config", {
    method: "POST",
    body: JSON.stringify(payload),
  });
  dashboardState.config = {
    envPreset: payload.envPreset,
    baseUrl: payload.baseUrl,
    simulated: Boolean(payload.simulated),
    executionMode: payload.executionMode,
    remoteGatewayUrl: payload.remoteGatewayUrl,
  };
  await refreshSnapshot().catch(() => {});
  await refreshDeskState().catch(() => {});
  await refreshAutomationState().catch(() => {});
  if (envChanged) {
    setMessage(
      `${data.persisted ? "配置已保存到本地" : "配置已载入到当前会话"}；已停止策略并关闭自动启动`,
      "ok"
    );
    setAutomationMessage("切换真实/模拟盘时已自动停止策略，并锁回实盘手动/自动交易与自动启动。", "warn");
  } else {
    setMessage(data.persisted ? "配置已保存到本地" : "配置已载入到当前会话", "ok");
  }
}

async function testConfig() {
  const payload = collectConfig();
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
  setMessage("连接通过，可以开始刷新账户和下单。", "ok");
}

async function saveAutomationConfig({ silent = false } = {}) {
  const payload = collectAutomationConfig();
  const data = await request("/api/automation/config", {
    method: "POST",
    body: JSON.stringify(payload),
  });
  await loadAutomationConfig();
  if (!silent) {
    setAutomationMessage("策略参数已保存。", "ok");
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
  fillAutomationForm({ ...collectAutomationConfig(), ...best });
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
  setAutomationMessage("自动量化已启动。", "ok");
}

async function stopAutomation() {
  await request("/api/automation/stop", { method: "POST" });
  await refreshAutomationState();
  setAutomationMessage("自动量化已停止。", "ok");
}

async function runAutomationOnce() {
  if (isLiveRouteBlocked()) {
    throw new Error(dashboardState.routeHealth?.summary || "当前实盘链路不可用");
  }
  await saveAutomationConfig();
  await request("/api/automation/run-once", { method: "POST" });
  await refreshAutomationState();
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
  if (dashboardState.currentView !== "trade") {
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
  $("spotInstId").addEventListener("change", () => {
    restartLiveFeedSoon();
    scheduleAutoAnalysis({ force: true });
  });
  $("swapInstId").addEventListener("change", () => {
    restartLiveFeedSoon();
    scheduleAutoAnalysis({ force: true });
  });
  $("marketBar").addEventListener("change", restartLiveFeedSoon);

  [
    "simulated",
    "autoBar",
    "autoFastEma",
    "autoSlowEma",
    "autoPollSeconds",
    "autoCooldownSeconds",
    "autoMaxOrdersPerDay",
    "autoSpotEnabled",
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

  $("start-automation").addEventListener("click", async () => {
    try {
      await startAutomation();
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
