// Scrape + test de proxys gratuits (Node 20, zéro dépendance)
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
const RE = /\b(\d{1,3}(?:\.\d{1,3}){3}):(\d{2,5})\b/g;

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

(async () => {
  fs.mkdirSync("proxies", { recursive: true });
  const { address: targetIp } = await dns.lookup("example.com", { family: 4 });
  const all = {};
  const full = {};

  for (const type of Object.keys(SOURCES)) {
    const list = await scrape(type);
    console.log(`[${type}] ${list.length} scrapés, test en cours...`);
    const alive = await runPool(list, (p) => test(type, p, targetIp));
    console.log(`[${type}] ${alive.length} vivants`);
    fs.writeFileSync(`proxies/${type}.txt`, alive.map((a) => a.proxy).join("\n") + "\n");
    all[type] = alive.map((a) => a.proxy);
    full[type] = alive;
  }

  fs.writeFileSync(
    "proxies/all.txt",
    Object.entries(all).flatMap(([t, l]) => l.map((p) => `${t}://${p}`)).join("\n") + "\n"
  );
  fs.writeFileSync(
    "proxies/proxies.json",
    JSON.stringify({ updated: new Date().toISOString(), ...full }, null, 2)
  );
})();
