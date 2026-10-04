// MiaStar — bot Discord (Worker + Durable Object, connecté en permanence)
// + API du Navigateur de Proxy : seulement les proxys mis en surveillance (👀).
import { DurableObject } from "cloudflare:workers";
import { connect } from "cloudflare:sockets";

const API = "https://discord.com/api/v10";
const LIST_URL = "https://raw.githubusercontent.com/ExoticSecurityWeb/scraper-proxy/main/proxies/proxies.json";
const TICK_MS = 60_000;
const FETCH_EVERY = 5 * 60_000;
const NOTIF_PER_HOUR = 6;
const MAX_PER_TICK = 3;
const DEAD_AFTER = 2;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const concat = (a, b) => { const o = new Uint8Array(a.length + b.length); o.set(a); o.set(b, a.length); return o; };
const flag = (cc) =>
  cc ? String.fromCodePoint(...[...cc.toUpperCase()].map((c) => 127397 + c.charCodeAt(0))) : "🏳️";

// ---------- Géoloc ----------
async function geo(ips) {
  const map = {};
  if (!ips.length) return map;
  try {
    const r = await fetch("http://ip-api.com/batch?fields=status,country,countryCode,query", {
      method: "POST",
      body: JSON.stringify(ips),
    });
    for (const g of await r.json()) if (g.status === "success") map[g.query] = g;
  } catch {}
  return map;
}

// ---------- Embeds / boutons ----------
const proxyEmbed = (p) => ({
  title: "📡 On a reçu un proxy !",
  color: 0xff4fa3,
  description: "Tu veux le garder en surveillance ou l'oublier ?",
  fields: [
    { name: "Proxy", value: `\`${p.proxy}\``, inline: true },
    { name: "Type", value: p.type.toUpperCase(), inline: true },
    { name: "Pays", value: p.country, inline: true },
    { name: "Latence", value: `${p.ms} ms`, inline: true },
  ],
  timestamp: new Date().toISOString(),
});

const choiceRow = (type, proxy) => ({
  type: 1,
  components: [
    { type: 2, style: 3, label: "Garder en surveillance", emoji: { name: "👀" }, custom_id: `keep|${type}|${proxy}` },
    { type: 2, style: 4, label: "Oublier", emoji: { name: "🗑️" }, custom_id: `forget|${type}|${proxy}` },
  ],
});

const stopRow = (type, proxy) => ({
  type: 1,
  components: [
    { type: 2, style: 2, label: "Ne plus surveiller", emoji: { name: "🗑️" }, custom_id: `forget|${type}|${proxy}` },
  ],
});

// ---------- Durable Object : MiaStar ----------
export class MiaStar extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.ws = null;
    this.seq = null;
    this.hb = null;
    this.busy = false;
    this.s = null;
    this.registered = false;
  }

  async load() {
    if (!this.s) {
      this.s = (await this.ctx.storage.get("s")) || {
        alive: [], seen: {}, watch: {}, notifs: [], lastFetch: 0, updated: null, total: 0,
      };
    }
    return this.s;
  }
  async save() { await this.ctx.storage.put("s", this.s); }

  async fetch(req) {
    const s = await this.load();
    // Liste des proxys en surveillance (utilisée par le Navigateur de Proxy)
    if (req && new URL(req.url).pathname === "/watchlist") {
      return Response.json(Object.values(s.watch).map((w) => ({ type: w.type, proxy: w.proxy, ms: w.ms ?? null })));
    }
    await this.wake();
    return Response.json({
      miastar: "ok",
      gateway: !!this.ws,
      liste_du_repo: s.updated,
      proxys_dans_la_liste: s.total,
      en_attente_de_notif: s.alive.length,
      en_surveillance: Object.keys(s.watch).length,
    });
  }

  async wake() {
    if (!(await this.ctx.storage.getAlarm())) await this.ctx.storage.setAlarm(Date.now() + 3000);
    await this.connectGateway().catch((e) => console.log("gateway:", e.message));
  }

  async alarm() {
    try {
      await this.connectGateway();
      await this.tick();
    } catch (e) {
      console.log("alarm:", e.message);
    } finally {
      await this.ctx.storage.setAlarm(Date.now() + TICK_MS);
    }
  }

  // ----- Gateway Discord (la pastille verte) -----
  async connectGateway() {
    if (this.ws && this.ws.readyState === 1) return;
    try { this.ws?.close(); } catch {}
    clearInterval(this.hb);
    const r = await fetch("https://gateway.discord.gg/?v=10&encoding=json", { headers: { Upgrade: "websocket" } });
    const ws = r.webSocket;
    if (!ws) throw new Error("pas de websocket");
    ws.accept();
    this.ws = ws;
    ws.addEventListener("message", (e) => this.onGateway(JSON.parse(e.data)).catch((x) => console.log("msg:", x.message)));
    const down = () => {
      if (this.ws !== ws) return;
      this.ws = null;
      clearInterval(this.hb);
      setTimeout(() => this.connectGateway().catch(() => {}), 5000);
    };
    ws.addEventListener("close", down);
    ws.addEventListener("error", down);
  }

  send(o) { try { this.ws.send(JSON.stringify(o)); } catch {} }

  async onGateway(p) {
    if (p.s != null) this.seq = p.s;
    if (p.op === 10) {
      clearInterval(this.hb);
      this.hb = setInterval(() => this.send({ op: 1, d: this.seq }), p.d.heartbeat_interval);
      this.send({
        op: 2,
        d: {
          token: this.env.TOKEN,
          intents: 1,
          properties: { os: "linux", browser: "miastar", device: "miastar" },
          presence: { status: "online", afk: false, since: null, activities: [{ name: "les proxys 📡", type: 3 }] },
        },
      });
    } else if (p.op === 1) {
      this.send({ op: 1, d: this.seq });
    } else if (p.op === 7 || p.op === 9) {
      try { this.ws?.close(); } catch {}
    } else if (p.op === 0) {
      if (p.t === "READY") await this.registerCommands(p.d.application.id);
      if (p.t === "INTERACTION_CREATE") await this.onInteraction(p.d);
    }
  }

  async registerCommands(appId) {
    if (this.registered) return;
    this.registered = true;
    await fetch(`${API}/applications/${appId}/commands`, {
      method: "PUT",
      headers: { Authorization: `Bot ${this.env.TOKEN}`, "Content-Type": "application/json" },
      body: JSON.stringify([
        { name: "watchlist", description: "Voir les proxys en surveillance" },
        { name: "scan", description: "Relire la liste du repo maintenant" },
      ]),
    });
  }

  async onInteraction(i) {
    const cb = (body) =>
      fetch(`${API}/interactions/${i.id}/${i.token}/callback`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
    const s = await this.load();

    if (i.type === 2) {
      if (i.data.name === "watchlist") {
        const l = Object.values(s.watch);
        await cb({
          type: 4,
          data: {
            flags: 64,
            content: l.length
              ? l.map((w) => `👀 \`${w.proxy}\` (${w.type.toUpperCase()})`).join("\n")
              : "Rien en surveillance pour l'instant.",
          },
        });
      } else if (i.data.name === "scan") {
        await cb({ type: 4, data: { flags: 64, content: "Je relis la liste du repo 📡" } });
        s.lastFetch = 0;
        this.tick(true).catch((e) => console.log("scan:", e.message));
      }
      return;
    }

    if (i.type === 3) {
      const [action, type, proxy] = i.data.custom_id.split("|");
      const key = `${type}|${proxy}`;
      const user = i.member?.user?.username || i.user?.username || "?";
      const embed = i.message.embeds[0];
      delete embed.description;

      if (action === "keep") {
        s.watch[key] = { type, proxy, by: user, at: Date.now(), fails: 0 };
        await this.save();
        embed.color = 0x57f287;
        embed.footer = { text: `👀 En surveillance (par ${user})` };
        await cb({ type: 7, data: { embeds: [embed], components: [stopRow(type, proxy)] } });
      } else {
        delete s.watch[key];
        await this.save();
        embed.color = 0x808080;
        embed.footer = { text: `🗑️ Oublié (par ${user})` };
        await cb({ type: 7, data: { embeds: [embed], components: [] } });
      }
    }
  }

  async post(payload) {
    for (let t = 0; t < 2; t++) {
      const r = await fetch(`${API}/channels/${this.env.CHANNEL_ID}/messages`, {
        method: "POST",
        headers: { Authorization: `Bot ${this.env.TOKEN}`, "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (r.status === 429) {
        const j = await r.json();
        await sleep(((j.retry_after || 1) + 0.2) * 1000);
        continue;
      }
      return r.ok;
    }
    return false;
  }

  // ----- Un passage : relit le repo, surveille, notifie -----
  async tick(force = false) {
    if (this.busy) return;
    this.busy = true;
    try {
      const s = await this.load();
      const now = Date.now();

      if (now - s.lastFetch > FETCH_EVERY) {
        s.lastFetch = now;
        let json = null;
        try {
          const r = await fetch(`${LIST_URL}?t=${now}`);
          if (r.ok) json = await r.json();
        } catch {}

        if (json && json.updated !== s.updated) {
          s.updated = json.updated;
          const current = new Map();
          for (const type of ["http", "socks4", "socks5"]) {
            for (const a of json[type] || []) current.set(`${type}|${a.proxy}`, a.ms);
          }
          s.total = current.size;

          for (const [key, w] of Object.entries(s.watch)) {
            if (current.has(key)) { w.fails = 0; w.ms = current.get(key); continue; }
            w.fails = (w.fails || 0) + 1;
            if (w.fails >= DEAD_AFTER) {
              delete s.watch[key];
              await this.post({
                embeds: [{
                  title: "💀 Un proxy surveillé est mort",
                  color: 0xed4245,
                  description: `\`${w.proxy}\` (${w.type.toUpperCase()}) n'est plus dans la liste, retiré de la surveillance.`,
                }],
              });
            }
          }

          s.alive = s.alive.filter((a) => current.has(`${a.type}|${a.proxy}`));
          for (const [key, ms] of current) {
            if (s.seen[key] || s.watch[key]) continue;
            const [type, proxy] = key.split("|");
            if (s.alive.some((a) => a.type === type && a.proxy === proxy)) continue;
            s.alive.push({ type, proxy, ms });
          }
          s.alive.sort((a, b) => a.ms - b.ms);
          s.alive = s.alive.slice(0, 100);
        }
      }

      s.notifs = s.notifs.filter((t) => now - t < 3600_000);
      let allowed = Math.max(0, NOTIF_PER_HOUR - s.notifs.length);
      if (force) allowed = Math.max(allowed, 1);
      const toSend = s.alive.splice(0, Math.min(allowed, MAX_PER_TICK));
      if (toSend.length) {
        const g = await geo([...new Set(toSend.map((p) => p.proxy.split(":")[0]))]);
        for (const p of toSend) {
          const gi = g[p.proxy.split(":")[0]];
          p.country = gi ? `${flag(gi.countryCode)} ${gi.country}` : "❓ Inconnu";
          const ok = await this.post({ embeds: [proxyEmbed(p)], components: [choiceRow(p.type, p.proxy)] });
          if (ok) {
            s.seen[`${p.type}|${p.proxy}`] = now;
            s.notifs.push(now);
          }
        }
      }

      for (const [k, t] of Object.entries(s.seen)) if (now - t > 3 * 24 * 3600_000) delete s.seen[k];
      await this.save();
    } finally {
      this.busy = false;
    }
  }
}

// =====================================================================
// API du Navigateur de Proxy : seulement les proxys en surveillance (👀),
// sur n'importe quel site (http/https).
// Option : si tu crées un secret NAV_KEY, il faut ajouter #LACLE au lien.
// =====================================================================
const CORS = { "Access-Control-Allow-Origin": "*", "Access-Control-Allow-Headers": "*" };
const MAX_BODY = 600_000;

async function resolve4(host) {
  if (/^\d+\.\d+\.\d+\.\d+$/.test(host)) return host;
  const j = await (await fetch(`https://cloudflare-dns.com/dns-query?name=${encodeURIComponent(host)}&type=A`, {
    headers: { accept: "application/dns-json" },
  })).json();
  const ip = j.Answer?.find((a) => a.type === 1)?.data;
  if (!ip) throw new Error("DNS introuvable");
  return ip;
}

function dechunk(b) {
  const parts = [];
  const dec = new TextDecoder();
  let i = 0;
  while (i < b.length) {
    let j = i;
    while (j < b.length - 1 && !(b[j] === 13 && b[j + 1] === 10)) j++;
    const size = parseInt(dec.decode(b.slice(i, j)).split(";")[0].trim(), 16);
    if (!size) break;
    i = j + 2;
    parts.push(b.slice(i, i + size));
    i += size + 2;
  }
  return parts.reduce((acc, p) => concat(acc, p), new Uint8Array(0));
}

// Fait un GET de `target` à travers le proxy (http, socks4 ou socks5)
async function viaProxy(type, proxy, target) {
  const u = new URL(target);
  const https = u.protocol === "https:";
  const host = u.hostname;
  const port = u.port ? +u.port : https ? 443 : 80;
  const [ph, pp] = proxy.split(":");
  const enc = new TextEncoder();
  const dec = new TextDecoder();
  const socket = connect({ hostname: ph, port: +pp }, { secureTransport: https ? "starttls" : "off" });
  const timer = new Promise((_, rej) => setTimeout(() => rej(new Error("timeout")), 12_000));

  const job = (async () => {
    let w = socket.writable.getWriter();
    let r = socket.readable.getReader();
    let buf = new Uint8Array(0);
    const fill = async () => {
      const { value, done } = await r.read();
      if (done) return false;
      buf = concat(buf, value);
      return true;
    };
    const need = async (n) => { while (buf.length < n) if (!(await fill())) throw new Error("connexion fermée"); };
    const take = (n) => { const o = buf.slice(0, n); buf = buf.slice(n); return o; };
    const headEnd = (b) => {
      for (let i = 0; i < b.length - 3; i++) if (b[i] === 13 && b[i + 1] === 10 && b[i + 2] === 13 && b[i + 3] === 10) return i + 4;
      return -1;
    };

    // 1) Tunnel via le proxy
    if (type === "http") {
      if (https) {
        await w.write(enc.encode(`CONNECT ${host}:${port} HTTP/1.1\r\nHost: ${host}:${port}\r\n\r\n`));
        let he;
        while ((he = headEnd(buf)) < 0) if (!(await fill())) throw new Error("connexion fermée");
        if (!/^HTTP\/1\.[01] 200/.test(dec.decode(take(he)))) throw new Error("ce proxy refuse le HTTPS (CONNECT)");
      }
    } else if (type === "socks5") {
      await w.write(new Uint8Array([5, 1, 0]));
      await need(2);
      const a = take(2);
      if (a[0] !== 5 || a[1] !== 0) throw new Error("socks5 refusé");
      const dom = enc.encode(host);
      await w.write(new Uint8Array([5, 1, 0, 3, dom.length, ...dom, (port >> 8) & 255, port & 255]));
      await need(4);
      const h = take(4);
      if (h[1] !== 0) throw new Error("socks5 : connexion impossible");
      if (h[3] === 3) { await need(1); const l = take(1)[0]; await need(l + 2); take(l + 2); }
      else { const n = h[3] === 4 ? 16 : 4; await need(n + 2); take(n + 2); }
    } else {
      const ip = await resolve4(host);
      await w.write(new Uint8Array([4, 1, (port >> 8) & 255, port & 255, ...ip.split(".").map(Number), 0]));
      await need(8);
      if (take(8)[1] !== 90) throw new Error("socks4 : connexion impossible");
    }

    // 2) TLS par-dessus le tunnel si HTTPS
    if (https) {
      w.releaseLock();
      r.releaseLock();
      const tls = socket.startTls({ expectedServerHostname: host });
      w = tls.writable.getWriter();
      r = tls.readable.getReader();
      buf = new Uint8Array(0);
    }

    // 3) La requête
    const line = type === "http" && !https ? u.href : u.pathname + u.search;
    await w.write(enc.encode(
      `GET ${line} HTTP/1.1\r\nHost: ${u.host}\r\nUser-Agent: Mozilla/5.0 (compatible; MiaStar)\r\n` +
      `Accept: text/html,*/*\r\nAccept-Encoding: identity\r\nConnection: close\r\n\r\n`
    ));

    // 4) La réponse
    let he;
    while ((he = headEnd(buf)) < 0) {
      if (!(await fill())) throw new Error("réponse vide");
      if (buf.length > 65_536 && headEnd(buf) < 0) throw new Error("réponse invalide");
    }
    const head = dec.decode(buf.slice(0, he));
    const m = head.match(/^HTTP\/1\.[01] (\d{3})/);
    if (!m) throw new Error("ce n'est pas du HTTP");
    const headers = {};
    for (const l of head.split("\r\n").slice(1)) {
      const k = l.indexOf(":");
      if (k > 0) headers[l.slice(0, k).trim().toLowerCase()] = l.slice(k + 1).trim();
    }
    buf = buf.slice(he);
    while (buf.length < MAX_BODY && (await fill())) {}
    const body = /chunked/i.test(headers["transfer-encoding"] || "") ? dechunk(buf) : buf;
    return { status: +m[1], headers, body };
  })();

  try {
    return await Promise.race([job, timer]);
  } finally {
    try { socket.close(); } catch {}
  }
}

async function navApi(url, env) {
  const json = (o, s = 200) => Response.json(o, { status: s, headers: CORS });

  if (env.NAV_KEY && url.searchParams.get("key") !== env.NAV_KEY) {
    return json({ ok: false, error: "Clé manquante : ajoute #LACLE à la fin du lien de la page" }, 403);
  }

  const watch = await (await star(env).fetch("https://miastar/watchlist")).json();
  if (url.pathname === "/api/proxies") return json(watch);

  const type = url.searchParams.get("type");
  const proxy = url.searchParams.get("proxy");
  let u;
  try { u = new URL(url.searchParams.get("url")); } catch { return json({ ok: false, error: "URL invalide" }, 400); }
  if (u.protocol !== "http:" && u.protocol !== "https:") return json({ ok: false, error: "Seulement http ou https" }, 400);
  if (u.port && u.port !== "80" && u.port !== "443") return json({ ok: false, error: "Seulement les ports 80 et 443" }, 400);
  if (!watch.some((w) => w.type === type && w.proxy === proxy)) {
    return json({ ok: false, error: "Ce proxy n'est pas en surveillance (👀)" }, 403);
  }

  const start = Date.now();
  try {
    const r = await viaProxy(type, proxy, u.href);
    const ms = Date.now() - start;
    const ct = r.headers["content-type"] || "";
    const text = /text\/|json|xml/.test(ct) ? new TextDecoder().decode(r.body) : "";
    if (url.pathname === "/api/test") {
      const title = (text.match(/<title[^>]*>([^<]{0,120})/i) || [])[1]?.trim() || null;
      return json({ ok: true, status: r.status, ms, title, bytes: r.body.length, contentType: ct });
    }
    return json({
      ok: true,
      status: r.status,
      ms,
      url: u.href,
      location: r.headers.location || null,
      contentType: ct,
      html: /text\/html/.test(ct) ? text : null,
    });
  } catch (e) {
    return json({ ok: false, error: e.message, ms: Date.now() - start });
  }
}

// ---------- Worker ----------
const star = (env) => env.STAR.get(env.STAR.idFromName("main"));

export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    if (req.method === "OPTIONS") return new Response(null, { headers: CORS });
    if (url.pathname === "/api/proxies" || url.pathname === "/api/test" || url.pathname === "/api/open") {
      return navApi(url, env);
    }
    return star(env).fetch(req);
  },
  scheduled: (_e, env, ctx) => ctx.waitUntil(star(env).fetch("https://miastar/wake")),
};
