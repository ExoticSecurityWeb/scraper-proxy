// MiaStar — bot Discord (Worker + Durable Object, connecté en permanence).
// Lit les proxys déjà testés par le repo ExoticSecurityWeb/scraper-proxy, notifie, boutons, surveillance.
import { DurableObject } from "cloudflare:workers";

const API = "https://discord.com/api/v10";
const LIST_URL = "https://raw.githubusercontent.com/ExoticSecurityWeb/scraper-proxy/main/proxies/proxies.json";
const TICK_MS = 60_000;
const FETCH_EVERY = 5 * 60_000;   // relit ton repo toutes les 5 min
const NOTIF_PER_HOUR = 6;
const MAX_PER_TICK = 3;
const DEAD_AFTER = 2;             // absent de 2 listes de suite = mort

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
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

  async fetch() {
    await this.wake();
    const s = await this.load();
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

      // 1) Relire proxies.json du repo (si pas déjà fait récemment)
      if (now - s.lastFetch > FETCH_EVERY) {
        s.lastFetch = now;
        let json = null;
        try {
          const r = await fetch(`${LIST_URL}?t=${now}`);
          if (r.ok) json = await r.json();
        } catch {}

        if (json && json.updated !== s.updated) {
          s.updated = json.updated;
          const current = new Map(); // "type|ip:port" -> ms
          for (const type of ["http", "socks4", "socks5"]) {
            for (const a of json[type] || []) current.set(`${type}|${a.proxy}`, a.ms);
          }
          s.total = current.size;

          // Surveillance : absent de la liste = probablement mort
          for (const [key, w] of Object.entries(s.watch)) {
            if (current.has(key)) { w.fails = 0; continue; }
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

          // Nouveaux proxys à proposer
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

      // 2) Notifs (6 max par heure)
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

// ---------- Worker : sert juste à réveiller MiaStar ----------
const star = (env) => env.STAR.get(env.STAR.idFromName("main"));

export default {
  fetch: (req, env) => star(env).fetch(req),
  scheduled: (_e, env, ctx) => ctx.waitUntil(star(env).fetch("https://miastar/wake")),
};