import WebSocket from "ws";

const ALERT_URL = process.env.ALERT_URL;
const ALERT_KEY = process.env.ZYNTRA_ALERT_KEY;

if (!ALERT_URL || !ALERT_KEY) {
  console.error("Faltan ALERT_URL o ZYNTRA_ALERT_KEY");
  process.exit(1);
}

const WS_URL =
  "wss://stream.binance.com:9443/ws/!ticker@arr";

const coins = new Map();
const cooldowns = new Map();

const MIN_QUOTE_VOLUME = 5_000_000;
const ALERT_COOLDOWN = 30 * 60 * 1000;

function now() {
  return Date.now();
}

function percent(a, b) {
  if (!a || !b) return 0;
  return ((b - a) / a) * 100;
}

function historyFor(symbol) {
  if (!coins.has(symbol)) {
    coins.set(symbol, []);
  }

  return coins.get(symbol);
}

function pushPrice(symbol, price, quoteVolume) {
  const history = historyFor(symbol);

  history.push({
    time: now(),
    price,
    quoteVolume
  });

  const cutoff = now() - 10 * 60 * 1000;

  while (history.length && history[0].time < cutoff) {
    history.shift();
  }
}

function nearest(history, millisecondsAgo) {
  const target = now() - millisecondsAgo;

  let best = null;

  for (const item of history) {
    if (!best) {
      best = item;
      continue;
    }

    if (
      Math.abs(item.time - target) <
      Math.abs(best.time - target)
    ) {
      best = item;
    }
  }

  return best;
}

function analyse(symbol, ticker) {
  const history = historyFor(symbol);

  if (history.length < 10) return null;

  const price = Number(ticker.c);
  const quoteVolume = Number(ticker.q);

  if (
    !Number.isFinite(price) ||
    !Number.isFinite(quoteVolume)
  ) {
    return null;
  }

  if (quoteVolume < MIN_QUOTE_VOLUME) {
    return null;
  }

  const p30s = nearest(history, 30_000);
  const p1m = nearest(history, 60_000);
  const p3m = nearest(history, 180_000);
  const p5m = nearest(history, 300_000);

  if (!p30s || !p1m || !p3m || !p5m) {
    return null;
  }

  const move30s = percent(p30s.price, price);
  const move1m = percent(p1m.price, price);
  const move3m = percent(p3m.price, price);
  const move5m = percent(p5m.price, price);

  let score = 0;
  const reasons = [];

  if (move30s > 0.15) {
    score += 10;
    reasons.push("momentum corto positivo");
  }

  if (move1m > 0.25) {
    score += 15;
    reasons.push("aceleración 1m");
  }

  if (move3m > 0.45) {
    score += 18;
    reasons.push("tendencia 3m");
  }

  if (move5m > 0.65) {
    score += 18;
    reasons.push("tendencia 5m");
  }

  if (quoteVolume > 25_000_000) {
    score += 12;
    reasons.push("liquidez alta");
  }

  if (quoteVolume > 100_000_000) {
    score += 8;
    reasons.push("volumen muy alto");
  }

  const accelerating =
    move30s > 0 &&
    move1m > move30s &&
    move3m > move1m;

  if (accelerating) {
    score += 15;
    reasons.push("aceleración consistente");
  }

  // Evita perseguir pumps extremos.
  if (move5m > 8) {
    score -= 35;
    reasons.push("movimiento demasiado extendido");
  }

  let level = null;
  let upside = null;
  let risk = null;

  if (score >= 88) {
    level = "PEZ GORDO";
    upside = "+8% a +15% (escenario estimado)";
    risk = "medio";
  } else if (score >= 72) {
    level = "OPORTUNIDAD BUENA";
    upside = "+4% a +8% (escenario estimado)";
    risk = "medio-bajo relativo";
  } else if (score >= 60) {
    level = "OPORTUNIDAD NORMAL";
    upside = "+3% a +5% (escenario estimado)";
    risk = "medio";
  }

  if (!level) return null;

  return {
    symbol,
    level,
    score,
    price,
    upside,
    risk,
    reason: reasons.join(", "),
    invalidation:
      "pérdida del momentum, caída fuerte del volumen o ruptura del soporte reciente"
  };
}

async function sendAlert(signal) {
  const cooldownKey =
    signal.symbol + ":" + signal.level;

  const previous = cooldowns.get(cooldownKey);

  if (
    previous &&
    now() - previous < ALERT_COOLDOWN
  ) {
    return;
  }

  try {
    const response = await fetch(ALERT_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-zyntra-key": ALERT_KEY
      },
      body: JSON.stringify(signal)
    });

    const text = await response.text();

    if (response.ok) {
      cooldowns.set(cooldownKey, now());

      console.log(
        "ALERTA:",
        signal.level,
        signal.symbol,
        signal.score,
        text
      );
    } else {
      console.error(
        "Worker rechazó alerta:",
        response.status,
        text
      );
    }
  } catch (error) {
    console.error(
      "Error enviando alerta:",
      error.message
    );
  }
}

function connect() {
  console.log("Conectando al mercado...");

  const ws = new WebSocket(WS_URL);

  ws.on("open", () => {
    console.log("Zyntra conectado a Binance ✅");
  });

  ws.on("message", async raw => {
    try {
      const tickers = JSON.parse(raw.toString());

      if (!Array.isArray(tickers)) return;

      for (const ticker of tickers) {
        const symbol = ticker.s;

        if (!symbol?.endsWith("USDT")) {
          continue;
        }

        const price = Number(ticker.c);
        const quoteVolume = Number(ticker.q);

        if (!price || !quoteVolume) {
          continue;
        }

        pushPrice(
          symbol,
          price,
          quoteVolume
        );

        const signal =
          analyse(symbol, ticker);

        if (signal) {
          await sendAlert(signal);
        }
      }
    } catch (error) {
      console.error(
        "Error procesando mercado:",
        error.message
      );
    }
  });

  ws.on("close", () => {
    console.log(
      "WebSocket cerrado. Reconectando..."
    );

    setTimeout(connect, 2000);
  });

  ws.on("error", error => {
    console.error(
      "WebSocket:",
      error.message
    );

    ws.close();
  });

  setInterval(() => {
    if (ws.readyState === WebSocket.OPEN) {
      ws.ping();
    }
  }, 30000);
}

console.log("ZYNTRA DETECTOR");
console.log("Vigilancia continua iniciada");

connect();
