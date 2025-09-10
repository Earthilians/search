// crawler/generate_index.js
// Rewritten crawler using native fetch (Node 18+), csv-parse, jsdom, fast-xml-parser, p-limit, p-retry

const fs = require('fs');
const path = require('path');
const { parse } = require('csv-parse/sync');
const { JSDOM } = require('jsdom');
const zlib = require('zlib');
const { XMLParser } = require('fast-xml-parser');
const pLimit = require('p-limit');
const pRetry = require('p-retry');
const { URL } = require('url');

// files
const DOMAINS_CSV = path.join(__dirname, 'domains.csv');
const OUT = path.join(__dirname, '..', 'site', 'index.json');
const LAST_INDEX_FILE = path.join(__dirname, '..', 'site', 'last_indexed.json');
const LOG_FILE = path.join(__dirname, '..', 'site', 'crawler.log');

// CONFIG - environment overrides allowed
const CONFIG = {
  USER_AGENT: process.env.USER_AGENT || 'EarthiliansCrawler/1.0 (+mailto:you@example.com)',
  concurrency: Number(process.env.CRAWL_CONCURRENCY || 5),
  rateMs: Number(process.env.CRAWL_RATE_MS || 200),
  fetchTimeoutMs: Number(process.env.CRAWL_FETCH_TIMEOUT_MS || 12000),
  maxPagesPerDomain: Number(process.env.CRAWL_PAGES_PER_DOMAIN || 10),
  maxDomainsPerRun: Number(process.env.CRAWL_DOMAINS_PER_RUN || 3000),
  maxTotalPages: Number(process.env.CRAWL_MAX_TOTAL_PAGES || 3000 * 10),
  maxTextChars: Number(process.env.CRAWL_MAX_TEXT_CHARS || 120000),
  retries: Number(process.env.CRAWL_RETRIES || 2),
  retryBackoffMs: Number(process.env.CRAWL_RETRY_BACKOFF_MS || 600),
  daysNoCheck: Number(process.env.CRAWL_DAYS_NO_CHECK || 4),
  batchSize: Number(process.env.CRAWL_BATCH_SIZE || 3000) // safety cap for domains processed
};

function log(...args) {
  const line = new Date().toISOString() + ' ' + args.map(a => (typeof a === 'string' ? a : JSON.stringify(a))).join(' ');
  console.log(line);
  try { fs.appendFileSync(LOG_FILE, line + '\n'); } catch (e) { /* ignore log errors */ }
}

function nowMs() { return Date.now(); }
function daysToMs(d) { return d * 24 * 60 * 60 * 1000; }

// ensure CSV exists
if (!fs.existsSync(DOMAINS_CSV)) {
  log('ERROR: domains.csv missing; create one with CSV and put domain in second column');
  process.exit(1);
}

// CSV loader: takes second column if present; falls back to first column
function loadDomainsFromCsv(p) {
  const raw = fs.readFileSync(p, 'utf8');
  const records = parse(raw, { skip_empty_lines: true });
  const out = [];
  for (const r of records) {
    const val = (r[1] && String(r[1]).trim()) || (r[0] && String(r[0]).trim());
    if (!val) continue;
    let candidate = val.trim();
    if (!/^https?:\/\//i.test(candidate)) candidate = 'https://' + candidate.replace(/\/+$/, '');
    try {
      const u = new URL(candidate);
      out.push({ base: u.origin, host: u.host });
    } catch (e) {
      // skip invalid
    }
  }
  return out;
}

// fetch with timeout using global fetch (Node 18+). If you run older node, you must install node-fetch and adjust.
async function fetchWithTimeout(url, opts = {}, timeout = CONFIG.fetchTimeoutMs) {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), timeout);
  try {
    const res = await globalThis.fetch(url, { ...opts, signal: controller.signal });
    clearTimeout(id);
    return res;
  } catch (e) {
    clearTimeout(id);
    throw e;
  }
}

// fetch sitemap text, support gz
async function fetchSitemapText(url) {
  return await pRetry(async () => {
    const res = await fetchWithTimeout(url, { headers: { 'User-Agent': CONFIG.USER_AGENT } }, CONFIG.fetchTimeoutMs);
    if (!res.ok) throw new Error('HTTP ' + res.status);
    const buf = Buffer.from(await res.arrayBuffer());
    const ct = (res.headers.get('content-type') || '').toLowerCase();
    if (url.toLowerCase().endsWith('.gz') || ct.includes('gzip')) {
      try { return zlib.gunzipSync(buf).toString('utf8'); } catch (e) { return buf.toString('utf8'); }
    }
    return buf.toString('utf8');
  }, { retries: 1 });
}

function parseSitemapXmlText(xml) {
  try {
    const parser = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: '@_' });
    const obj = parser.parse(xml);
    const out = [];
    if (obj.sitemapindex && obj.sitemapindex.sitemap) {
      const s = obj.sitemapindex.sitemap; const arr = Array.isArray(s) ? s : [s];
      for (const it of arr) if (it.loc) out.push(String(it.loc));
    }
    if (obj.urlset && obj.urlset.url) {
      const u = obj.urlset.url; const arr = Array.isArray(u) ? u : [u];
      for (const it of arr) if (it.loc) out.push(String(it.loc));
    }
    if (out.length === 0) for (const m of xml.matchAll(/<loc>([^<]+)<\/loc>/gi)) out.push(m[1].trim());
    return out;
  } catch (e) {
    const out = []; for (const m of xml.matchAll(/<loc>([^<]+)<\/loc>/gi)) out.push(m[1].trim()); return out;
  }
}

// expand sitemap (follows sitemapindex entries), returns pages (not xml files)
async function expandSitemap(sitemapUrl, seen, domainAllowed, maxPagesPerDomain) {
  const q = [sitemapUrl];
  const pages = [];
  while (q.length) {
    const s = q.shift();
    if (!s || seen.has(s)) continue;
    seen.add(s);
    try {
      const xml = await fetchSitemapText(s);
      const locs = parseSitemapXmlText(xml);
      for (const l of locs) {
        let norm;
        try { norm = new URL(l).toString(); } catch (e) { try { norm = new URL(l, s).toString(); } catch (e2) { continue; } }
        if (norm.toLowerCase().endsWith('.xml') || norm.toLowerCase().endsWith('.xml.gz')) q.push(norm);
        else if (domainAllowed(norm)) pages.push(norm);
      }
    } catch (e) {
      // ignore sitemap fetch errors per domain
    }
    await sleep(CONFIG.rateMs);
    if (pages.length >= maxPagesPerDomain) break;
  }
  return pages;
}

// fetch page HTML and extract title, meta description, paragraphs
async function fetchHtml(url) {
  return await pRetry(async () => {
    const res = await fetchWithTimeout(url, { headers: { 'User-Agent': CONFIG.USER_AGENT } }, CONFIG.fetchTimeoutMs);
    if (!res.ok) throw new Error('HTTP ' + res.status);
    const ct = (res.headers.get('content-type') || '').toLowerCase();
    if (!ct.includes('html')) throw new Error('non-html ' + ct);
    const html = await res.text();
    const dom = new JSDOM(html);
    const doc = dom.window.document;
    const title = doc.querySelector('title')?.textContent?.trim() || '';
    const rawMeta = doc.querySelector('meta[name="description"]')?.getAttribute('content')?.trim() || '';
    const metaDesc = rawMeta.slice(0, 200);
    const ps = Array.from(doc.querySelectorAll('p')).map(p => p.textContent.trim()).filter(Boolean);
    const text = ps.join('\n').slice(0, CONFIG.maxTextChars);
    return { title, description: metaDesc, text };
  }, { retries: CONFIG.retries });
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function loadJsonSafe(p) { try { return JSON.parse(fs.readFileSync(p, 'utf8')); } catch (e) { return null; } }

// main
(async function main() {
  log('Starting crawler run');
  const domains = loadDomainsFromCsv(DOMAINS_CSV);
  log('Loaded domains count=', domains.length);

  const lastIndex = loadJsonSafe(LAST_INDEX_FILE) || {};
  const existingOut = loadJsonSafe(OUT) || [];

  const seenPages = new Set(existingOut.map(x => x.url));
  const seenSitemaps = new Set();
  const out = [];
  let total = 0;

  function recentlyIndexed(host) {
    const rec = lastIndex[host];
    if (!rec || !rec.lastAt) return false;
    return (nowMs() - rec.lastAt) < daysToMs(CONFIG.daysNoCheck);
  }

  function domainAllowedFactory(domainsList) {
    const hostList = domainsList.map(d => d.host);
    return (url) => {
      try {
        const h = new URL(url).host;
        return hostList.some(hd => h === hd || h.endsWith('.' + hd));
      } catch (e) {
        return false;
      }
    };
  }

  const domainAllowed = domainAllowedFactory(domains);

  // decide which domains to process this run (batch)
  const toProcess = [];
  for (const d of domains) {
    if (toProcess.length >= Math.min(CONFIG.batchSize, CONFIG.maxDomainsPerRun)) break;
    if (recentlyIndexed(d.host)) continue;
    toProcess.push(d);
  }

  log('Domains to process this run=', toProcess.length);

  const limit = pLimit(CONFIG.concurrency);

  for (const d of toProcess) {
    if (total >= CONFIG.maxTotalPages) break;
    log('Processing domain=', d.base);

    const domainSitemaps = new Set();

    try {
      const robots = new URL('/robots.txt', d.base).toString();
      const r = await fetchWithTimeout(robots, { headers: { 'User-Agent': CONFIG.USER_AGENT } }, 6000);
      if (r && r.ok) {
        const txt = await r.text();
        for (const line of txt.split(/\r?\n/)) {
          if (line.toLowerCase().startsWith('sitemap:')) {
            const sm = line.split(':').slice(1).join(':').trim();
            try { domainSitemaps.add(new URL(sm, d.base).toString()); } catch (_) { domainSitemaps.add(sm); }
          }
        }
      }
    } catch (e) {
      log('[WARN] robots parse error for', d.base, e && e.message ? e.message : e);
    }

    // fallback and collected sitemaps
    domainSitemaps.add(new URL('/sitemap.xml', d.base).toString());

    // expand sitemaps
    let pageUrls = [];
    for (const sm of domainSitemaps) {
      try {
        const pages = await expandSitemap(sm, seenSitemaps, domainAllowed, CONFIG.maxPagesPerDomain);
        for (const p of pages) {
          if (!seenPages.has(p)) { pageUrls.push(p); seenPages.add(p); }
          if (pageUrls.length >= 1000) break;
        }
      } catch (e) {
        log('[WARN] sitemap fallback error for', d.base, e && e.message ? e.message : e);
      }
      if (pageUrls.length >= 1000) break;
    }

    // ensure homepage
    const homepage = new URL('/', d.base).toString();
    if (!pageUrls.includes(homepage) && !seenPages.has(homepage)) pageUrls.unshift(homepage);

    log(' Domain', d.base, 'collected urls=', pageUrls.length);

    const hostRecord = lastIndex[d.host] || { lastAt: 0, urls: [] };
    const alreadyIndexedSet = new Set(hostRecord.urls || []);
    const candidates = pageUrls.filter(u => !alreadyIndexedSet.has(u));

    if (candidates.length === 0) {
      log(' No new pages for', d.host);
      lastIndex[d.host] = { lastAt: nowMs(), urls: hostRecord.urls || [] };
      continue;
    }

    const candidatesLimited = candidates.slice(0, CONFIG.maxPagesPerDomain);
    const newUrlsForHost = [];

    const tasks = candidatesLimited.map(url => limit(async () => {
      await sleep(CONFIG.rateMs);
      try {
        const info = await fetchHtml(url);
        if (!info) return;
        out.push({ id: url, url, title: info.title, description: info.description, text: info.text });
        newUrlsForHost.push(url);
        total++;
        log('[KEEP]', url, 'title-len=', (info.title || '').length, 'meta-len=', (info.description || '').length);
      } catch (e) {
        log('[ERR FETCH]', url, e && e.message ? e.message : e);
      }
    }));

    await Promise.all(tasks);

    lastIndex[d.host] = { lastAt: nowMs(), urls: Array.from(new Set([...(hostRecord.urls || []), ...newUrlsForHost])) };
    log(' Added', newUrlsForHost.length, 'new pages for', d.host);
  }

  // merge with existingOut, dedupe
  const merged = [];
  const seen = new Set();
  for (const e of existingOut) {
    if (!e || !e.url) continue;
    if (seen.has(e.url)) continue;
    seen.add(e.url);
    merged.push(e);
  }
  for (const r of out) {
    if (!r || !r.url) continue;
    if (seen.has(r.url)) continue;
    seen.add(r.url);
    merged.push(r);
  }

  if (!fs.existsSync(path.dirname(OUT))) fs.mkdirSync(path.dirname(OUT), { recursive: true });
  fs.writeFileSync(OUT, JSON.stringify(merged, null, 2), 'utf8');
  log('Wrote', OUT, '->', merged.length, 'pages total (merged).');

  fs.writeFileSync(LAST_INDEX_FILE, JSON.stringify(lastIndex, null, 2), 'utf8');
  log('Updated', LAST_INDEX_FILE);
  log('Crawler run completed — total new pages this run=', out.length);
})();

// crawler/generate_index_shard.js

// Shard-aware crawler worker: reads crawler/domains.csv (uses SECOND column per line; falls back to first),
// partitioned deterministically by hash(host) % shardCount.
// Writes per-shard outputs: shard-output-<shardIndex>.json and shard-last-<shardIndex>.json

// Example (Actions job):
// node crawler/generate_index_shard.js --shardIndex=0 --shardCount=50 --domains=./crawler/domains.csv \
//   --out=shard-output-0.json --last=shard-last-0.json --globalLast=./site/last_indexed.json \
//   --maxDomainsPerShard=2000 --concurrency=12 --maxPages=10 --daysNoCheck=4 --verbose

// crawler/generate_index_shard.js
// crawler/generate_index_shard.js
// Defensive shard crawler with per-domain timeout and robust fetch cleanup.

// const fs = require('fs');
// const path = require('path');
// const http = require('http');
// const https = require('https');
// const { JSDOM } = require('jsdom');
// const zlib = require('zlib');
// const { XMLParser } = require('fast-xml-parser');
// const fetch = require('node-fetch');
// const { URL } = require('url');

// //
// // CLI helpers
// //
// function parseArgInt(name, def) {
//   const p = process.argv.find(a => a.startsWith(`--${name}=`));
//   if (!p) return def;
//   const v = Number(p.split('=')[1]);
//   return Number.isFinite(v) ? v : def;
// }
// function parseArg(name, def) {
//   const p = process.argv.find(a => a.startsWith(`--${name}=`));
//   if (!p) return def;
//   return p.split('=')[1];
// }
// function hasFlag(name) { return process.argv.includes(`--${name}`); }

// const REPO_ROOT = process.cwd();
// const DOMAINS_CSV = parseArg('domains', path.join(REPO_ROOT, 'crawler', 'domains.csv'));
// const SHARD_INDEX = parseArgInt('shardIndex', 0);
// const SHARD_COUNT = parseArgInt('shardCount', 1);
// const OUT_FILE = parseArg('out', path.join(REPO_ROOT, `crawler`, `shard-output-${SHARD_INDEX}.json`));
// const LAST_OUT_FILE = parseArg('last', path.join(REPO_ROOT, `crawler`, `shard-last-${SHARD_INDEX}.json`));
// const GLOBAL_LAST_FILE = parseArg('globalLast', path.join(REPO_ROOT, 'site', 'last_indexed.json'));

// const CONFIG = {
//   concurrency: parseArgInt('concurrency', 10),
//   rateMs: parseArgInt('rateMs', 150),
//   fetchTimeoutMs: parseArgInt('fetchTimeoutMs', 12000),
//   maxPages: parseArgInt('maxPages', 10),
//   maxDomainsPerShard: parseArgInt('maxDomainsPerShard', 2000),
//   retries: parseArgInt('retries', 2),
//   retryBackoffMs: parseArgInt('retryBackoffMs', 600),
//   daysNoCheck: parseArgInt('daysNoCheck', 4),
//   maxUrlsPerHost: parseArgInt('maxUrlsPerHost', 10000),
//   domainTimeoutMs: parseArgInt('domainTimeoutMs', 30 * 1000) // per-domain timeout default 30 seconds
// };

// const VERBOSE = hasFlag('verbose');
// const FORCE = hasFlag('force');
// const USER_AGENT = 'EarthiliansCrawler/1.0 (+mailto:you@example.com)';

// function log(...args){ if (VERBOSE) console.log(...args); }
// function nowMs(){ return Date.now(); }
// function daysToMs(d){ return d * 24 * 60 * 60 * 1000; }

// // global progress watchdog
// let lastProgressAt = nowMs();
// function touchProgress(){ lastProgressAt = nowMs(); }

// // basic process-level handlers
// process.on('unhandledRejection', (reason) => {
//   console.error('[UNHANDLED REJECTION]', reason && reason.stack ? reason.stack : reason);
// });
// process.on('uncaughtException', err => {
//   console.error('[UNCAUGHT EXCEPTION]', err && err.stack ? err.stack : err);
// });

// const httpAgent = new http.Agent({ keepAlive: true, maxSockets: CONFIG.concurrency });
// const httpsAgent = new https.Agent({ keepAlive: true, maxSockets: CONFIG.concurrency });

// // global watchdog to avoid infinite runs (30 minutes default, configurable via env WATCHDOG_MINUTES)
// const WATCHDOG_MINUTES = Number(process.env.WATCHDOG_MINUTES || 30);
// const WATCHDOG_MS = WATCHDOG_MINUTES * 60 * 1000;
// setInterval(() => {
//   if (nowMs() - lastProgressAt > WATCHDOG_MS) {
//     console.error(`[FATAL] No progress for ${WATCHDOG_MINUTES} minutes — exiting`);
//     try { httpAgent.destroy(); } catch (e) {}
//     try { httpsAgent.destroy(); } catch (e) {}
//     process.exit(1);
//   }
// }, 60 * 1000);

// async function fetchWithTimeout(url, opts = {}, timeout = CONFIG.fetchTimeoutMs) {
//   const controller = new AbortController();
//   const id = setTimeout(() => {
//     try { controller.abort(); } catch (e) {}
//   }, timeout);

//   try {
//     const u = new URL(url);
//     const agent = u.protocol === 'http:' ? httpAgent : httpsAgent;
//     const res = await fetch(url, { ...opts, agent, signal: controller.signal });
//     return res;
//   } finally {
//     clearTimeout(id);
//   }
// }

// async function fetchSitemapText(url) {
//   const res = await fetchWithTimeout(url, { headers: { 'User-Agent': USER_AGENT } }, CONFIG.fetchTimeoutMs);
//   if (!res || !res.ok) throw new Error('HTTP ' + (res ? res.status : 'NORES'));
//   const buf = Buffer.from(await res.arrayBuffer());
//   const ct = (res.headers.get('content-type') || '').toLowerCase();
//   if (url.toLowerCase().endsWith('.gz') || ct.includes('gzip')) {
//     try { return zlib.gunzipSync(buf).toString('utf8'); } catch (e) { return buf.toString('utf8'); }
//   }
//   return buf.toString('utf8');
// }

// function parseSitemapXmlText(xml) {
//   try {
//     const parser = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: '@_' });
//     const obj = parser.parse(xml);
//     const out = [];
//     if (obj.sitemapindex && obj.sitemapindex.sitemap) {
//       const s = obj.sitemapindex.sitemap; const arr = Array.isArray(s) ? s : [s];
//       for (const it of arr) if (it.loc) out.push(String(it.loc));
//     }
//     if (obj.urlset && obj.urlset.url) {
//       const u = obj.urlset.url; const arr = Array.isArray(u) ? u : [u];
//       for (const it of arr) if (it.loc) out.push(String(it.loc));
//     }
//     if (out.length === 0) for (const m of xml.matchAll(/<loc>([^<]+)<\/loc>/gi)) out.push(m[1].trim());
//     return out;
//   } catch (e) {
//     const out = []; for (const m of xml.matchAll(/<loc>([^<]+)<\/loc>/gi)) out.push(m[1].trim()); return out;
//   }
// }

// //
// // Robust URL helpers
// //
// function safeNewUrl(input, base) {
//   try {
//     if (base) return new URL(input, base);
//     return new URL(input);
//   } catch (e) {
//     return null;
//   }
// }

// function normalizeDomainCandidate(raw) {
//   if (!raw) return null;
//   const s = raw.trim();
//   if (!s) return null;
//   if (/^https?:\/\//i.test(s)) {
//     const u = safeNewUrl(s);
//     if (!u) return null;
//     return u.origin;
//   }
//   const hostOnly = s.replace(/^\/+|\/+$/g, '').split(/\s+/)[0];
//   if (!hostOnly) return null;
//   const cand = 'https://' + hostOnly;
//   const u = safeNewUrl(cand);
//   return u ? u.origin : null;
// }

// function hashHost(s) {
//   let h = 5381;
//   for (let i = 0; i < s.length; i++) h = ((h << 5) + h) + s.charCodeAt(i) | 0;
//   return Math.abs(h) >>> 0;
// }

// function loadJsonSafe(p) { try { return JSON.parse(fs.readFileSync(p, 'utf8')); } catch (e) { return null; } }
// function atomicWrite(p, obj) {
//   const tmp = p + '.tmp.' + process.pid;
//   const dir = path.dirname(p);
//   if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
//   fs.writeFileSync(tmp, JSON.stringify(obj, null, 2), 'utf8');
//   fs.renameSync(tmp, p);
// }

// //
// // SITEMAP expand — using safe URL resolution
// //
// async function expandSitemap(sitemapUrl, seen) {
//   const q = [sitemapUrl]; const pages = [];
//   while (q.length) {
//     const s = q.shift(); if (!s || seen.has(s)) continue; seen.add(s);
//     try {
//       const xml = await fetchSitemapText(s);
//       const locs = parseSitemapXmlText(xml);
//       for (const l of locs) {
//         let normObj = safeNewUrl(l);
//         if (!normObj) normObj = safeNewUrl(l, s);
//         if (!normObj) continue;
//         const norm = normObj.toString();
//         if (norm.toLowerCase().endsWith('.xml') || norm.toLowerCase().endsWith('.xml.gz')) q.push(norm);
//         else pages.push(norm);
//       }
//     } catch (e) {
//       log('[WARN] sitemap fetch fail', sitemapUrl, e && e.message ? e.message : e);
//     }
//     await new Promise(r => setTimeout(r, CONFIG.rateMs));
//     if (pages.length >= CONFIG.maxPages) break;
//   }
//   return pages.slice(0, CONFIG.maxPages);
// }

// //
// // Strict fetchHtml: use JSDOM and require title + meta + paragraphs; skip otherwise.
// // Any parse/fetch error => return null (skip).
// //
// async function fetchHtml(url) {
//   let attempt = 0;
//   while (true) {
//     try {
//       const res = await fetchWithTimeout(url, { headers: { 'User-Agent': USER_AGENT } }, CONFIG.fetchTimeoutMs);
//       if (!res || !res.ok) {
//         log('[SKIP NON-OK]', url, 'status=', res ? res.status : 'NORES');
//         return null;
//       }

//       const ct = (res.headers.get('content-type') || '').toLowerCase();
//       if (!ct.includes('html')) {
//         log('[SKIP NON-HTML]', url, 'content-type=', ct);
//         return null;
//       }

//       let html = await res.text();

//       // remove stylesheets and inline styles to avoid CSS parser crashes in JSDOM
//       html = html.replace(/<style[\s\S]*?>[\s\S]*?<\/style\s*>/gi, '');
//       html = html.replace(/<link[^>]+rel=(["']?)stylesheet\1[^>]*>/gi, '');
//       html = html.replace(/\sstyle=(["'])(.*?)\1/gi, '');

//       let dom;
//       try {
//         dom = new JSDOM(html);
//       } catch (jsErr) {
//         log('[SKIP JSDOM-ERR]', url, jsErr && jsErr.message ? jsErr.message : jsErr);
//         return null;
//       }

//       const doc = dom.window.document;
//       // get raw values
//       let title = doc.querySelector('title')?.textContent?.trim() || '';
//       let metaDesc = doc.querySelector('meta[name="description"]')?.getAttribute('content')?.trim() || '';
//       const ps = Array.from(doc.querySelectorAll('p')).map(p => p.textContent.trim()).filter(Boolean);
//       let text = ps.join('\n').slice(0, CONFIG.maxTextChars);

//       // enforce presence
//       if (!title)      { log('[SKIP MISSING TITLE]', url); try { dom.window.close(); } catch(e){}; return null; }
//       if (!metaDesc)   { log('[SKIP MISSING META]', url); try { dom.window.close(); } catch(e){}; return null; }
//       if (!text)       { log('[SKIP MISSING TEXT]', url); try { dom.window.close(); } catch(e){}; return null; }

//       // TRIM ALL TO FIRST 100 CHARACTERS (per your request)
//       title = title.slice(0, 100);
//       metaDesc = metaDesc.slice(0, 100);
//       text = text.slice(0, 100);

//       // cleanup dom to free memory
//       try { if (dom && dom.window && dom.window.close) dom.window.close(); } catch (e) {}
//       dom = null;
//       html = null;

//       touchProgress();
//       return { title, description: metaDesc, text };
//     } catch (e) {
//       attempt++;
//       if (attempt > CONFIG.retries) {
//         log('[ERR FETCH GIVING UP]', url, e && e.message ? e.message : e);
//         return null;
//       }
//       await new Promise(r => setTimeout(r, CONFIG.retryBackoffMs * Math.pow(2, attempt - 1)));
//     }
//   }
// }

// //
// // MAIN
// //
// (async function main() {
//   console.log('Shard', SHARD_INDEX, 'start: domainsCsv=', DOMAINS_CSV, 'shardCount=', SHARD_COUNT, 'maxDomainsPerShard=', CONFIG.maxDomainsPerShard, 'domainTimeoutMs=', CONFIG.domainTimeoutMs);

//   if (!fs.existsSync(DOMAINS_CSV)) {
//     console.error('domains CSV missing:', DOMAINS_CSV);
//     process.exit(1);
//   }

//   const globalLast = loadJsonSafe(GLOBAL_LAST_FILE) || {};

//   // safe CSV streaming & shard selection
//   const selected = [];
//   const rs = fs.createReadStream(DOMAINS_CSV, { encoding: 'utf8' });
//   let buf = '';
//   let totalSeen = 0;
//   for await (const chunk of rs) {
//     buf += chunk;
//     let idx;
//     while ((idx = buf.indexOf('\n')) >= 0) {
//       const line = buf.slice(0, idx).trim();
//       buf = buf.slice(idx + 1);
//       if (!line) continue;
//       totalSeen++;

//       const cols = line.split(',');
//       const domainCandidate = (cols[1] && cols[1].trim()) || (cols[0] && cols[0].trim());
//       if (!domainCandidate) continue;

//       const normalizedBase = normalizeDomainCandidate(domainCandidate);
//       if (!normalizedBase) {
//         log('[SKIP INVALID DOMAIN LINE]', domainCandidate);
//         continue;
//       }

//       const host = safeNewUrl(normalizedBase)?.host;
//       if (!host) {
//         log('[SKIP BAD HOST]', normalizedBase);
//         continue;
//       }

//       const h = hashHost(host);
//       if ((h % SHARD_COUNT) === SHARD_INDEX) {
//         selected.push({ raw: normalizedBase, host });
//         if (CONFIG.maxDomainsPerShard > 0 && selected.length >= CONFIG.maxDomainsPerShard) break;
//       }
//     }
//     if (CONFIG.maxDomainsPerShard > 0 && selected.length >= CONFIG.maxDomainsPerShard) break;
//   }
//   // leftover last line
//   if ((CONFIG.maxDomainsPerShard === 0 || selected.length < CONFIG.maxDomainsPerShard) && buf.trim()) {
//     const cols = buf.trim().split(',');
//     const domainCandidate = (cols[1] && cols[1].trim()) || (cols[0] && cols[0].trim());
//     if (domainCandidate) {
//       const normalizedBase = normalizeDomainCandidate(domainCandidate);
//       if (normalizedBase) {
//         const host = safeNewUrl(normalizedBase)?.host;
//         if (host) {
//           const h = hashHost(host);
//           if ((h % SHARD_COUNT) === SHARD_INDEX) selected.push({ raw: normalizedBase, host });
//         }
//       } else {
//         log('[SKIP INVALID DOMAIN LINE]', domainCandidate);
//       }
//     }
//   }

//   console.log('Shard', SHARD_INDEX, 'collected', selected.length, 'domains (file seen=', totalSeen, ')');

//   let idx = 0;
//   const outPages = [];
//   const shardLast = {};

//   async function processDomainItem(item) {
//     const host = item.host;
//     const rec = globalLast[host] || { lastAt: 0, urls: [] };
//     const recently = !FORCE && rec.lastAt && ((nowMs() - rec.lastAt) < daysToMs(CONFIG.daysNoCheck));
//     if (recently) {
//       log('[SKIP-RECENT]', host);
//       shardLast[host] = { lastAt: rec.lastAt, urls: Array.from(new Set(rec.urls || [])) };
//       return;
//     }

//     // guarded robots + fallback sitemap collection
//     const domainSitemaps = new Set();
//     try {
//       const baseOrigin = item.raw;
//       const robotsUrlObj = safeNewUrl('/robots.txt', baseOrigin);
//       if (robotsUrlObj) {
//         const r = await fetchWithTimeout(robotsUrlObj.toString(), { headers: { 'User-Agent': USER_AGENT } }, 6000).catch(() => null);
//         if (r && r.ok) {
//           const txt = await r.text().catch(() => '');
//           for (const line of txt.split(/\r?\n/)) {
//             if (line.toLowerCase().startsWith('sitemap:')) {
//               const smRaw = line.split(':').slice(1).join(':').trim();
//               const smObj = safeNewUrl(smRaw, robotsUrlObj.toString());
//               if (smObj) domainSitemaps.add(smObj.toString());
//               else log('[WARN] ignored invalid sitemap URL in robots:', smRaw, 'for', host);
//             }
//           }
//         }
//       }
//     } catch (e) {
//       log('[WARN] robots handling failed', item.host, e && e.message ? e.message : e);
//     }

//     // always add fallback sitemap
//     const fallbackSitemap = safeNewUrl('/sitemap.xml', item.raw);
//     if (fallbackSitemap) domainSitemaps.add(fallbackSitemap.toString());

//     // expand sitemaps
//     let pageUrls = [];
//     const seenSitemaps = new Set();
//     for (const sm of domainSitemaps) {
//       try {
//         const pages = await expandSitemap(sm, seenSitemaps);
//         for (const p of pages) if (!pageUrls.includes(p)) pageUrls.push(p);
//       } catch (e) { log('[WARN] sitemap expand fail', item.host, e && e.message ? e.message : e); }
//       if (pageUrls.length >= 1000) break;
//     }

//     // ensure homepage is first
//     const homepage = safeNewUrl('/', item.raw)?.toString() || (item.raw.endsWith('/') ? item.raw : item.raw + '/');
//     if (!pageUrls.includes(homepage)) pageUrls.unshift(homepage);

//     const already = new Set(rec.urls || []);
//     const candidates = pageUrls.filter(u => !already.has(u)).slice(0, CONFIG.maxPages);

//     if (candidates.length === 0) {
//       shardLast[item.host] = { lastAt: nowMs(), urls: Array.from(new Set(rec.urls || [])) };
//       log('[NO-NEW]', item.host);
//       touchProgress();
//       return;
//     }

//     const got = [];
//     for (const url of candidates) {
//       await new Promise(r => setTimeout(r, CONFIG.rateMs));
//       const info = await fetchHtml(url).catch(e => { log('[ERR FETCH]', url, e && e.message ? e.message : e); return null; });
//       if (!info) continue;
//       // ensure stored values are trimmed
//       const outTitle = (info.title || '').slice(0, 100);
//       const outDesc = (info.description || '').slice(0, 100);
//       const outText = (info.text || '').slice(0, 100);
//       outPages.push({ id: url, url, title: outTitle, description: outDesc, text: outText });
//       got.push(url);
//       touchProgress();
//       if (got.length >= CONFIG.maxPages) break;
//     }

//     const mergedUrls = Array.from(new Set([...(rec.urls || []), ...got]));
//     if (mergedUrls.length > CONFIG.maxUrlsPerHost) mergedUrls.splice(CONFIG.maxUrlsPerHost);

//     shardLast[item.host] = { lastAt: nowMs(), urls: mergedUrls };
//     log('[DONE]', item.host, 'added', got.length);
//     touchProgress();
//   }

//   async function worker() {
//     while (true) {
//       const i = idx++;
//       if (i >= selected.length) break;
//       const item = selected[i];

//       try {
//         // run domain processing with timeout
//         const domainPromise = processDomainItem(item);
//         const timeoutMs = CONFIG.domainTimeoutMs;
//         const timeoutPromise = new Promise((_, rej) =>
//           setTimeout(() => rej(new Error('DOMAIN_TIMEOUT')), timeoutMs)
//         );

//         try {
//           await Promise.race([domainPromise, timeoutPromise]);
//         } catch (e) {
//           if (e && e.message === 'DOMAIN_TIMEOUT') {
//             console.warn('[TIMEOUT] domain processing exceeded', timeoutMs, 'ms for', item.host, '- skipping');
//             try {
//               shardLast[item.host] = { lastAt: nowMs(), urls: (shardLast[item.host] && shardLast[item.host].urls) || [] };
//             } catch (ex) {}
//             touchProgress();
//             continue;
//           } else {
//             // other domain-level error
//             console.error('[ERROR] domain worker error for', item && item.host ? item.host : item, e && e.stack ? e.stack : e);
//             try {
//               shardLast[item.host || ('bad-host-' + i)] = { lastAt: nowMs(), urls: (shardLast[item.host] && shardLast[item.host].urls) || [] };
//             } catch (ee) {}
//             touchProgress();
//             continue;
//           }
//         }

//       } catch (domainErr) {
//         console.error('[ERROR] unexpected domain worker error for', item && item.host ? item.host : item, domainErr && domainErr.stack ? domainErr.stack : domainErr);
//         try {
//           shardLast[item.host || ('bad-host-' + i)] = { lastAt: nowMs(), urls: (shardLast[item.host] && shardLast[item.host].urls) || [] };
//         } catch (e) {}
//         touchProgress();
//         continue;
//       }
//     }
//   }

//   const pool = [];
//   for (let w = 0; w < CONFIG.concurrency; w++) pool.push(worker());
//   await Promise.all(pool);

//   atomicWrite(OUT_FILE, outPages);
//   atomicWrite(LAST_OUT_FILE, shardLast);
//   console.log('Shard', SHARD_INDEX, 'wrote', OUT_FILE, 'pages=', outPages.length, 'hosts=', Object.keys(shardLast).length);

//   try { httpAgent.destroy(); } catch (e) {}
//   try { httpsAgent.destroy(); } catch (e) {}

//   // small grace then exit
//   setTimeout(() => {
//     console.log('Shard exiting cleanly.');
//     process.exit(0);
//   }, 800);
// })().catch(err => {
//   console.error('FATAL', err && err.stack ? err.stack : err);
//   try { httpAgent.destroy(); } catch (e) {}
//   try { httpsAgent.destroy(); } catch (e) {}
//   process.exit(1);
// });
