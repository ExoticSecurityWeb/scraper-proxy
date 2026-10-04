// Scrape + test + notif Discord (webhook) + surveillance par réactions. Node 20, zéro dépendance.
const net = require("net");
const dns = require("dns").promises;
const fs = require("fs");

const SOURCES = {
  http: [
    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
    "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/http.txt",
    "https://api.proxyscrape.com/v2/?request=getproxies&protocol=http&timeout=10000&country=all",
  ],
  socks4: [
    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks4.txt",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks4.txt",
  ],
  socks5: [
    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks5.txt",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt",
  ],
};

const TIMEOUT = 6000;
const CONCURRENCY = 300;
const MAX_NEW = 8; // nb max de nouvelles notifs par run
const STATE_FILE = "proxies/state.json";
const RE = /\b(\d{1,3}(?:\.\d{1,3}){3}):(\d{2,5})\b/g;

const { WEBHOOK_URL, BOT_TOKEN } = process.env; // BOT_TOKEN = optionnel (lecture des réactions)
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ---------- Scrape ----------
async function scrape(type) {
  const res = await Promise.allSettled(
    SOURCES[type].map((u) =>
      fetch(u, { signal: AbortSignal.timeout(15000) }).then((r) => r.text())
    )
  );
  const set = new Set();
  for (const r of res) {
    if (r.status !== "fulfilled") continue;
    for (const m of r.value.matchAll(RE)) {
      if (m[1].split(".").every((n) => +n <= 255) && +m[2] > 0 && +m[2] < 65536)
        set.add(`${m[1]}:${m[2]}`);
    }
  }
  return [...set];
}

// ---------- Test ----------
function test(type, proxy, targetIp) {
  return new Promise((resolve) => {
    const [host, port] = proxy.split(":");
    const start = Date.now();
    const s = net.connect({ host, port: +port });
    let done = false;
    let buf = Buffer.alloc(0);
    let stage = type === "http" ? 2 : 0;
    const req = "GET / HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\n\r\n";

    const end = (ok) => {
      if (done) return;
      done = true;
      s.destroy();
      resolve(ok ? Date.now() - start : null);
    };

    s.setTimeout(TIMEOUT, () => end(false));
    s.on("error", () => end(false));
    s.on("close", () => end(false));

    s.on("connect", () => {
      if (type === "http") {
        s.write("GET http://example.com/ HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\n\r\n");
      } else if (type === "socks5") {
        s.write(Buffer.from([5, 1, 0]));
      } else {
        s.write(Buffer.from([4, 1, 0, 80, ...targetIp.split(".").map(Number), 0]));
      }
    });

    s.on("data", (d) => {
      buf = Buffer.concat([buf, d]);
      if (stage === 0 && type === "socks5") {
        if (buf.length < 2) return;
        if (buf[0] !== 5 || buf[1] !== 0) return end(false);
        buf = buf.subarray(2);
        stage = 1;
        const dom = Buffer.from("example.com");
        s.write(Buffer.concat([Buffer.from([5, 1, 0, 3, dom.length]), dom, Buffer.from([0, 80])]));
        return;
      }
      if (stage === 1) {
        if (buf.length < 10) return;
        if (buf[1] !== 0 || buf[3] !== 1) return end(false);
        buf = buf.subarray(10);
        stage = 2;
        s.write(req);
      }
      if (stage === 0 && type === "socks4") {
        if (buf.length < 8) return;
        if (buf[1] !== 90) return end(false);
        buf = buf.subarray(8);
        stage = 2;
        s.write(req);
      }
      if (stage === 2) {
        if (buf.toString("latin1").includes("Example Domain")) end(true);
        else if (buf.length > 30000) end(false);
      }
    });
  });
}

async function runPool(list, worker) {
  const out = [];
  let i = 0;
  await Promise.all(
    Array.from({ length: CONCURRENCY }, async () => {
      while (i < list.length) {
        const item = list[i++];
        const r = await worker(item);
        if (r !== null) out.push({ proxy: item, ms: r });
      }
    })
  );
  return out.sort((a, b) => a.ms - b.ms);
}

// ---------- Géoloc (ip-api, batch de 100 max) ----------
async function geo(ips) {
  const map = {};
  if (!ips.length) return map;
  try {
    const r = await fetch("http://ip-api.com/batch?fields=status,country,countryCode,query", {
      method: "POST",
      body: JSON.stringify(ips),
      signal: AbortSignal.timeout(15000),
    });
    for (const g of await r.json()) if (g.status === "success") map[g.query] = g;
  } catch (e) {
    console.log("geo KO:", e.message);
  }
  return map;
}

const flag = (cc) =>
  cc ? String.fromCodePoint(...[...cc.toUpperCase()].map((c) => 127397 + c.charCodeAt(0))) : "🏳️";

// ---------- Discord (webhook) ----------
async function hook(payload, { method = "POST", mid } = {}) {
  const url = method === "POST" ? `${WEBHOOK_URL}?wait=true` : `${WEBHOOK_URL}/messages/${mid}`;
  for (let t = 0; t < 3; t++) {
    const r = await fetch(url, {
      method,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (r.status === 429) {
      const j = await r.json();
      await sleep((j.retry_after || 2) * 1000 + 200);
      continue;
    }
    if (!r.ok) {
      console.log("webhook KO", r.status, await r.text());
      return null;
    }
    return r.json();
  }
  return null;
}

function embed(p, footer, color = 0xff4fa3) {
  return {
    title: "📡 On a reçu un proxy !",
    color,
    description: footer ? undefined : "Réagis avec 👀 pour le **garder en surveillance** ou ❌ pour l'**oublier**.",
    fields: [
      { name: "Proxy", value: `\`${p.proxy}\``, inline: true },
      { name: "Type", value: p.type.toUpperCase(), inline: true },
      { name: "Pays", value: p.country, inline: true },
      { name: "Latence", value: `${p.ms} ms`, inline: true },
    ],
    footer: footer ? { text: footer } : undefined,
    timestamp: new Date().toISOString(),
  };
}

async function reactedByHuman(channelId, mid, emoji) {
  const r = await fetch(
    `https://discord.com/api/v10/channels/${channelId}/messages/${mid}/reactions/${encodeURIComponent(emoji)}?limit=25`,
    { headers: { Authorization: `Bot ${BOT_TOKEN}` } }
  );
  if (!r.ok) return false;
  const users = await r.json();
  return users.some((u) => !u.bot);
}

// ---------- State ----------
function loadState() {
  try {
    const s = JSON.parse(fs.readFileSync(STATE_FILE, "utf8"));
    return { seen: s.seen || {}, pending: s.pending || {}, watch: s.watch || {} };
  } catch {
    return { seen: {}, pending: {}, watch: {} };
  }
}

// ---------- Main ----------
(async () => {
  fs.mkdirSync("proxies", { recursive: true });
  const { address: targetIp } = await dns.lookup("example.com", { family: 4 });

  const all = {};
  const aliveMs = {}; // "type|ip:port" -> ms
  for (const type of Object.keys(SOURCES)) {
    const list = await scrape(type);
    console.log(`[${type}] ${list.length} scrapés, test en cours...`);
    const alive = await runPool(list, (p) => test(type, p, targetIp));
    console.log(`[${type}] ${alive.length} vivants`);
    fs.writeFileSync(`proxies/${type}.txt`, alive.map((a) => a.proxy).join("\n") + "\n");
    all[type] = alive;
    for (const a of alive) aliveMs[`${type}|${a.proxy}`] = a.ms;
  }

  fs.writeFileSync(
    "proxies/all.txt",
    Object.entries(all).flatMap(([t, l]) => l.map((a) => `${t}://${a.proxy}`)).join("\n") + "\n"
  );
  fs.writeFileSync(
    "proxies/proxies.json",
    JSON.stringify({ updated: new Date().toISOString(), ...all }, null, 2)
  );

  const state = loadState();
  const now = Date.now();

  if (!WEBHOOK_URL) {
    console.log("Pas de WEBHOOK_URL, pas de notif Discord.");
  } else {
    let channelId = null;
    if (BOT_TOKEN) {
      try {
        channelId = (await (await fetch(WEBHOOK_URL)).json()).channel_id;
      } catch {}
    }

    // 1) Réactions sur les messages en attente de décision
    for (const [key, p] of Object.entries(state.pending)) {
      if (now - p.at > 24 * 3600 * 1000) { delete state.pending[key]; continue; } // expiré
      if (!channelId) break;
      const keep = await reactedByHuman(channelId, p.mid, "👀");
      const drop = !keep && (await reactedByHuman(channelId, p.mid, "❌"));
      if (!keep && !drop) continue;
      if (keep) state.watch[key] = { type: p.type, proxy: p.proxy };
      delete state.pending[key];
      await hook(
        { embeds: [embed(p, keep ? "👀 En surveillance" : "❌ Oublié", keep ? 0x57f287 : 0x808080)] },
        { method: "PATCH", mid: p.mid }
      );
      await sleep(600);
    }

    // 2) Surveillance : retest des proxys gardés
    for (const [key, w] of Object.entries(state.watch)) {
      let ms = aliveMs[key];
      if (ms === undefined) ms = await test(w.type, w.proxy, targetIp);
      if (ms === null) {
        delete state.watch[key];
        await hook({
          embeds: [{
            title: "💀 Un proxy surveillé est mort",
            color: 0xed4245,
            description: `\`${w.proxy}\` (${w.type.toUpperCase()}) ne répond plus, retiré de la surveillance.`,
          }],
        });
        await sleep(1200);
      }
    }

    // 3) Nouveaux proxys à notifier
    const fresh = Object.entries(all)
      .flatMap(([type, l]) => l.map((a) => ({ type, proxy: a.proxy, ms: a.ms })))
      .filter((p) => {
        const k = `${p.type}|${p.proxy}`;
        return !state.seen[k] && !state.watch[k] && !state.pending[k];
      })
      .sort((a, b) => a.ms - b.ms)
      .slice(0, MAX_NEW);

    const g = await geo([...new Set(fresh.map((p) => p.proxy.split(":")[0]))]);

    for (const p of fresh) {
      const key = `${p.type}|${p.proxy}`;
      const gi = g[p.proxy.split(":")[0]];
      p.country = gi ? `${flag(gi.countryCode)} ${gi.country}` : "❓ Inconnu";
      const msg = await hook({ embeds: [embed(p)] });
      if (!msg) continue;
      state.seen[key] = now;
      if (BOT_TOKEN) state.pending[key] = { ...p, mid: msg.id, at: now };
      await sleep(1200);
    }
  }

  // Nettoyage des "seen" de plus de 7 jours
  for (const [k, t] of Object.entries(state.seen)) if (now - t > 7 * 24 * 3600 * 1000) delete state.seen[k];
  fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2));
})();
