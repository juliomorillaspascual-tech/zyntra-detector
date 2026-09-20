import WebSocket from "ws";

const ALERT_URL = process.env.ALERT_URL;
const ALERT_KEY = process.env.ZYNTRA_ALERT_KEY;

if (!ALERT_URL || !ALERT_KEY) {
  console.error("Faltan ALERT_URL o ZYNTRA_ALERT_KEY");
  process.exit(1);
}

const WS_URL = "wss://stream.binance.com:9443/ws/!ticker@arr";
const REST = "https://api.binance.com";

const MIN_VOLUME_24H = 8_000_000;
const ANALYSIS_COOLDOWN = 30_000;
const ALERT_COOLDOWN = 45 * 60 * 1000;
const MAX_CONCURRENT = 4;

const lastAnalysis = new Map();
const lastAlert = new Map();

let running = 0;
const queue = [];

function pct(a, b) {
  if (!a || !b) return 0;
  return ((b - a) / a) * 100;
}

function average(arr) {
  if (!arr.length) return 0;
  return arr.reduce((a, b) => a + b, 0) / arr.length;
}

function ema(values, period) {
  if (values.length < period) return null;

  const k = 2 / (period + 1);
  let value = average(values.slice(0, period));

  for (let i = period; i < values.length; i++) {
    value = values[i] * k + value * (1 - k);
  }

  return value;
}

function rsi(values, period = 14) {
  if (values.length <= period) return null;

  let gains = 0;
  let losses = 0;

  for (let i = values.length - period; i < values.length; i++) {
    const change = values[i] - values[i - 1];

    if (change > 0) gains += change;
    else losses += Math.abs(change);
  }

  if (losses === 0) return 100;

  const rs = gains / losses;
  return 100 - 100 / (1 + rs);
}

function atr(candles, period = 14) {
  if (candles.length <= period) return null;

  const trs = [];

  for (let i = 1; i < candles.length; i++) {
    const high = candles[i].high;
    const low = candles[i].low;
    const prevClose = candles[i - 1].close;

    trs.push(
      Math.max(
        high - low,
        Math.abs(high - prevClose),
        Math.abs(low - prevClose)
      )
    );
  }

  return average(trs.slice(-period));
}

async function getKlines(symbol, interval, limit = 100) {
  const url =
    `${REST}/api/v3/klines?symbol=${symbol}` +
    `&interval=${interval}&limit=${limit}`;

  const r = await fetch(url);

  if (!r.ok) {
    throw new Error("Klines " + r.status);
  }

  const data = await r.json();

  return data.map(x => ({
    open: Number(x[1]),
    high: Number(x[2]),
    low: Number(x[3]),
    close: Number(x[4]),
    volume: Number(x[5]),
    quoteVolume: Number(x[7])
  }));
}

function analyseFrame(candles) {
  const closes = candles.map(c => c.close);
  const volumes = candles.map(c => c.quoteVolume);

  const price = closes.at(-1);

  const ema9 = ema(closes, 9);
  const ema21 = ema(closes, 21);
  const ema50 = ema(closes, 50);

  const currentRSI = rsi(closes, 14);
  const currentATR = atr(candles, 14);

  const recentVolumes = volumes.slice(-21, -1);
  const avgVolume = average(recentVolumes);
  const currentVolume = volumes.at(-1);

  const volumeRatio =
    avgVolume > 0 ? currentVolume / avgVolume : 0;

  const previous20 = candles.slice(-21, -1);

  const resistance = Math.max(
    ...previous20.map(c => c.high)
  );

  const support = Math.min(
    ...previous20.map(c => c.low)
  );

  const distanceResistance =
    pct(price, resistance);

  const move5 =
    closes.length >= 6
      ? pct(closes.at(-6), price)
      : 0;

  return {
    price,
    ema9,
    ema21,
    ema50,
    rsi: currentRSI,
    atr: currentATR,
    volumeRatio,
    resistance,
    support,
    distanceResistance,
    move5
  };
}

function scoreSetup(m1, m5, m15) {
  let score = 0;
  const reasons = [];

  // Tendencia corta
  if (m1.ema9 > m1.ema21) {
    score += 7;
    reasons.push("EMA 9 > EMA 21 en 1m");
  }

  if (m5.ema9 > m5.ema21) {
    score += 10;
    reasons.push("tendencia positiva en 5m");
  }

  if (
    m15.ema9 > m15.ema21 &&
    m15.ema21 > m15.ema50
  ) {
    score += 14;
    reasons.push("estructura alcista en 15m");
  }

  // Volumen
  if (m1.volumeRatio >= 1.5) {
    score += 7;
    reasons.push("volumen 1m por encima de la media");
  }

  if (m5.volumeRatio >= 1.4) {
    score += 10;
    reasons.push("volumen relativo fuerte");
  }

  if (m5.volumeRatio >= 2.2) {
    score += 5;
    reasons.push("expansión clara de volumen");
  }

  // RSI: momentum sin estar excesivamente extendido
  if (m5.rsi >= 52 && m5.rsi <= 68) {
    score += 10;
    reasons.push("RSI con momentum saludable");
  }

  if (m15.rsi >= 50 && m15.rsi <= 67) {
    score += 8;
    reasons.push("RSI 15m favorable");
  }

  // Cercanía a ruptura
  if (
    m5.distanceResistance >= -0.8 &&
    m5.distanceResistance <= 0.4
  ) {
    score += 10;
    reasons.push("precio cerca de resistencia relevante");
  }

  if (m5.price > m5.resistance) {
    score += 9;
    reasons.push("ruptura reciente de resistencia");
  }

  // Confirmación temporal
  if (
    m1.price > m1.ema21 &&
    m5.price > m5.ema21 &&
    m15.price > m15.ema21
  ) {
    score += 10;
    reasons.push("confirmación multi-timeframe");
  }

  // Penalizaciones anti-pump
  if (m1.rsi > 82) {
    score -= 18;
    reasons.push("RSI 1m excesivamente alto");
  }

  if (m5.rsi > 78) {
    score -= 20;
    reasons.push("RSI 5m sobreextendido");
  }

  if (m5.move5 > 7) {
    score -= 30;
    reasons.push("movimiento reciente demasiado vertical");
  }

  if (m15.move5 > 12) {
    score -= 35;
    reasons.push("pump demasiado extendido");
  }

  return {
    score,
    reasons
  };
}

function classify(score, m5) {
  const atrPct =
    m5.atr && m5.price
      ? (m5.atr / m5.price) * 100
      : 0;

  if (score >= 88 && atrPct <= 4.5) {
    return {
      level: "PEZ GORDO",
      upside: "+7% a +15% (escenario estimado)",
      risk: "medio relativo"
    };
  }

  if (score >= 76 && atrPct <= 4) {
    return {
      level: "OPORTUNIDAD BUENA",
      upside: "+4% a +8% (escenario estimado)",
      risk: "medio-bajo relativo"
    };
  }

  if (score >= 66 && atrPct <= 3.5) {
    return {
      level: "OPORTUNIDAD NORMAL",
      upside: "+3% a +5% (escenario estimado)",
      risk: "medio relativo"
    };
  }

  return null;
}

async function deepAnalyse(symbol) {
  try {
    const [c1, c5, c15] = await Promise.all([
      getKlines(symbol, "1m", 100),
      getKlines(symbol, "5m", 100),
      getKlines(symbol, "15m", 100)
    ]);

    const m1 = analyseFrame(c1);
    const m5 = analyseFrame(c5);
    const m15 = analyseFrame(c15);

    const result = scoreSetup(m1, m5, m15);
    const classification =
      classify(result.score, m5);

    if (!classification) {
      console.log(
        "Sin señal:",
        symbol,
        "score",
        result.score
      );

      return;
    }

    const alertKey =
      symbol + ":" + classification.level;

    const previous =
      lastAlert.get(alertKey) || 0;

    if (
      Date.now() - previous <
      ALERT_COOLDOWN
    ) {
      return;
    }

    const signal = {
      symbol,
      level: classification.level,
      price: m5.price,
      upside: classification.upside,
      risk: classification.risk,
      score: result.score,

      reason:
        result.reasons.join(", "),

      startWindow:
        "si mantiene volumen y confirma la estructura en las próximas velas",

      targetZone:
        "escenario calculado según estructura, volatilidad y resistencias",

      invalidation:
        "pérdida del soporte reciente, deterioro de volumen o ruptura bajista de la estructura"
    };

    await sendAlert(signal);

    lastAlert.set(
      alertKey,
      Date.now()
    );

  } catch (error) {
    console.error(
      "Análisis profundo",
      symbol,
      error.message
    );
  }
}

async function sendAlert(signal) {
  try {
    const r = await fetch(ALERT_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-zyntra-key": ALERT_KEY
      },
      body: JSON.stringify(signal)
    });

    const response = await r.text();

    if (r.ok) {
      console.log(
        "🚨 ALERTA ZYNTRA:",
        signal.level,
        signal.symbol,
        "score:",
        signal.score
      );
    } else {
      console.error(
        "Worker rechazó alerta:",
        r.status,
        response
      );
    }

  } catch (error) {
    console.error(
      "Error enviando alerta:",
      error.message
    );
  }
}

function enqueue(symbol) {
  queue.push(symbol);
  processQueue();
}

async function processQueue() {
  if (
    running >= MAX_CONCURRENT ||
    queue.length === 0
  ) {
    return;
  }

  const symbol = queue.shift();
  running++;

  try {
    await deepAnalyse(symbol);
  } finally {
    running--;
    processQueue();
  }
}

function quickFilter(ticker) {
  const symbol = ticker.s;
  const price = Number(ticker.c);
  const quoteVolume = Number(ticker.q);
  const change24h = Number(ticker.P);

  if (!symbol.endsWith("USDT")) {
    return false;
  }

  // Excluir productos apalancados típicos
  if (
    symbol.includes("UPUSDT") ||
    symbol.includes("DOWNUSDT") ||
    symbol.includes("BULLUSDT") ||
    symbol.includes("BEARUSDT")
  ) {
    return false;
  }

  if (
    !Number.isFinite(price) ||
    !Number.isFinite(quoteVolume)
  ) {
    return false;
  }

  if (quoteVolume < MIN_VOLUME_24H) {
    return false;
  }

  // Evitar activos con pumps diarios extremos
  if (change24h > 35) {
    return false;
  }

  // No analizar continuamente cada moneda
  const previous =
    lastAnalysis.get(symbol) || 0;

  if (
    Date.now() - previous <
    ANALYSIS_COOLDOWN
  ) {
    return false;
  }

  // Solo activar el análisis pesado cuando
  // existe cierta actividad positiva.
  if (change24h < -6) {
    return false;
  }

  lastAnalysis.set(
    symbol,
    Date.now()
  );

  return true;
}

function connect() {
  console.log(
    "Conectando Zyntra al mercado..."
  );

  const ws =
    new WebSocket(WS_URL);

  ws.on("open", () => {
    console.log(
      "Zyntra conectado a Binance ✅"
    );
  });

  ws.on("message", raw => {
    try {
      const tickers =
        JSON.parse(raw.toString());

      if (!Array.isArray(tickers)) {
        return;
      }

      for (const ticker of tickers) {
        if (quickFilter(ticker)) {
          enqueue(ticker.s);
        }
      }

    } catch (error) {
      console.error(
        "Error procesando stream:",
        error.message
      );
    }
  });

  ws.on("close", () => {
    console.log(
      "Mercado desconectado. Reconectando..."
    );

    setTimeout(
      connect,
      2000
    );
  });

  ws.on("error", error => {
    console.error(
      "WebSocket:",
      error.message
    );

    ws.close();
  });

  const heartbeat =
    setInterval(() => {
      if (
        ws.readyState ===
        WebSocket.OPEN
      ) {
        ws.ping();
      }
    }, 30000);

  ws.on("close", () => {
    clearInterval(heartbeat);
  });
}

console.log("================================");
console.log("ZYNTRA DETECTOR PRO");
console.log("Mercado: tiempo real");
console.log("Análisis: multi-timeframe");
console.log("Alertas: Normal / Buena / Pez Gordo");
console.log("================================");

connect();
