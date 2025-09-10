// // crawler/generate_index.js
// const fs = require('fs');
// const path = require('path');
// const fetch = require('node-fetch');
// const { JSDOM } = require('jsdom');
// const zlib = require('zlib');
// const { XMLParser } = require('fast-xml-parser');
// const { URL } = require('url');

// const DOMAINS_FILE = path.join(__dirname, 'domains.txt');
// const OUT = path.join(__dirname, '..', 'site', 'index.json');
// const LAST_INDEX_FILE =path.join(__dirname, '..', 'site', 'last_indexed.json')

// const USER_AGENT = 'EarthiliansCrawler/1.0 (+mailto:you@example.com)';

// const CONFIG = {
//   concurrency: 5,
//   rateMs: 200,
//   fetchTimeoutMs: 12000,
//   // homepage + up to 9 new pages per run
//   maxPagesPerDomain: 10,
//   maxTotalPages: 1000,
//   maxTextChars: 120000,
//   retries: 2,
//   retryBackoffMs: 600,
//   // days to wait before reindexing same host
//   daysNoCheck: 15
// };

// function nowMs() { return Date.now(); }
// function daysToMs(d) { return d * 24 * 60 * 60 * 1000; }

// if (!fs.existsSync(DOMAINS_FILE)) {
//   console.error('domains.txt missing; add one domain per line (e.g. https://example.com)');
//   process.exit(1);
// }

// const raw = fs.readFileSync(DOMAINS_FILE, 'utf8')
//   .split(/\r?\n/)
//   .map(s => s.trim()).filter(Boolean);
// if (raw.length === 0) { console.error('domains.txt empty'); process.exit(1); }

// const domains = raw.map(d => {
//   try { const u = new URL(d); return { base: u.origin, host: u.host }; }
//   catch (e) { const host = d.replace(/^https?:\/\//, '').replace(/\/+$/, ''); return { base: 'https://' + host, host }; }
// });

// function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
// function domainAllowed(url) { try { const h = new URL(url).host; return domains.some(d => h === d.host || h.endsWith('.' + d.host)); } catch (e) { return false; } }

// async function fetchWithTimeout(url, opts = {}, timeout = CONFIG.fetchTimeoutMs) {
//   const controller = new AbortController();
//   const id = setTimeout(() => controller.abort(), timeout);
//   try { const res = await fetch(url, { ...opts, signal: controller.signal }); clearTimeout(id); return res; }
//   catch (e) { clearTimeout(id); throw e; }
// }

// async function fetchSitemapText(url) {
//   const res = await fetchWithTimeout(url, { headers: { 'User-Agent': USER_AGENT } }, CONFIG.fetchTimeoutMs);
//   if (!res.ok) throw new Error('HTTP ' + res.status);
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

// async function expandSitemap(sitemapUrl, seen) {
//   const q = [sitemapUrl]; const pages = [];
//   while (q.length) {
//     const s = q.shift(); if (!s || seen.has(s)) continue; seen.add(s);
//     try {
//       const xml = await fetchSitemapText(s);
//       const locs = parseSitemapXmlText(xml);
//       for (const l of locs) {
//         let norm;
//         try { norm = new URL(l).toString(); } catch (e) { try { norm = new URL(l, s).toString(); } catch (e2) { continue; } }
//         if (norm.toLowerCase().endsWith('.xml') || norm.toLowerCase().endsWith('.xml.gz')) q.push(norm);
//         else if (domainAllowed(norm)) pages.push(norm);
//       }
//     } catch (e) {
//       // ignore sitemap fetch errors
//     }
//     await sleep(CONFIG.rateMs);
//     if (pages.length >= CONFIG.maxPagesPerDomain) break;
//   }
//   return pages;
// }

// async function fetchHtml(url) {
//   let attempt = 0;
//   while (true) {
//     try {
//       const res = await fetchWithTimeout(url, { headers: { 'User-Agent': USER_AGENT } }, CONFIG.fetchTimeoutMs);
//       if (!res.ok) throw new Error('HTTP ' + res.status);
//       const ct = (res.headers.get('content-type') || '').toLowerCase();
//       if (!ct.includes('html')) throw new Error('non-html ' + ct);
//       const html = await res.text();
//       const dom = new JSDOM(html);
//       const doc = dom.window.document;
//       const title = doc.querySelector('title')?.textContent?.trim() || '';
//       const rawMeta = doc.querySelector('meta[name="description"]')?.getAttribute('content')?.trim() || '';
//       const metaDesc = rawMeta.slice(0, 100);
//       const ps = Array.from(doc.querySelectorAll('p')).map(p => p.textContent.trim()).filter(Boolean);
//       const text = ps.join('\n').slice(0, CONFIG.maxTextChars);
//       return { title, description: metaDesc, text };
//     } catch (e) {
//       attempt++;
//       if (attempt > CONFIG.retries) return null;
//       await sleep(CONFIG.retryBackoffMs * Math.pow(2, attempt - 1));
//     }
//   }
// }

// function loadJsonSafe(p) { try { return JSON.parse(fs.readFileSync(p, 'utf8')); } catch (e) { return null; } }
// const lastIndex = loadJsonSafe(LAST_INDEX_FILE) || {}; // shape: { host: { lastAt: number, urls: [ ... ] } }
// const existingOut = loadJsonSafe(OUT) || [];

// function recentlyIndexed(host) {
//   const rec = lastIndex[host]; if (!rec || !rec.lastAt) return false; return (nowMs() - rec.lastAt) < daysToMs(CONFIG.daysNoCheck);
// }

// (async function main() {
//   console.log('Starting crawl for', domains.length, 'domains');
//   const seenPages = new Set(existingOut.map(x => x.url));
//   const seenSitemaps = new Set();
//   const out = [];
//   let total = 0;

//   for (const d of domains) {
//     if (recentlyIndexed(d.host)) {
//       console.log('Skipping', d.base, '— indexed recently');
//       continue;
//     }
//     if (total >= CONFIG.maxTotalPages) break;
//     console.log('Processing', d.base);

//     const domainSitemaps = new Set();
//     try {
//       const robots = new URL('/robots.txt', d.base).toString();
//       const r = await fetchWithTimeout(robots, { headers: { 'User-Agent': USER_AGENT } }, 6000);
//       if (r && r.ok) {
//         const txt = await r.text();
//         for (const line of txt.split(/\r?\n/)) {
//           if (line.toLowerCase().startsWith('sitemap:')) {
//             const sm = line.split(':').slice(1).join(':').trim();
//             try { domainSitemaps.add(new URL(sm, d.base).toString()); } catch (_) { domainSitemaps.add(sm); }
//           }
//         }
//       }
//     } catch (e) {
//       console.log('[WARN] robots parse error for', d.base, e && e.message ? e.message : e);
//     }

//     // fallback sitemap
//     domainSitemaps.add(new URL('/sitemap.xml', d.base).toString());

//     // expand sitemaps
//     let pageUrls = [];
//     for (const sm of domainSitemaps) {
//       try {
//         const pages = await expandSitemap(sm, seenSitemaps);
//         for (const p of pages) {
//           if (!seenPages.has(p)) { pageUrls.push(p); seenPages.add(p); }
//           if (pageUrls.length >= 1000) break;
//         }
//       } catch (e) {
//         console.log('[WARN] sitemap fallback error for', d.base, e && e.message ? e.message : e);
//       }
//       if (pageUrls.length >= 1000) break;
//     }

//     // ensure homepage first
//     const homepage = new URL('/', d.base).toString();
//     if (!pageUrls.includes(homepage)) pageUrls.unshift(homepage);

//     console.log(' Domain', d.base, '-> collected so far:', pageUrls.length);

//     // filter out urls already indexed for this host (persisted in lastIndex)
//     const hostRecord = lastIndex[d.host] || { lastAt: 0, urls: [] };
//     const alreadyIndexedSet = new Set(hostRecord.urls || []);
//     const candidates = pageUrls.filter(u => !alreadyIndexedSet.has(u));

//     if (candidates.length === 0) {
//       console.log(' No new pages found for', d.host);
//       lastIndex[d.host] = { lastAt: nowMs(), urls: hostRecord.urls || [] };
//       continue;
//     }

//     // fetch up to maxPagesPerDomain new pages
//     const newUrlsForHost = [];
//     let idx = 0;

//     async function worker() {
//       while (true) {
//         const i = idx++;
//         if (i >= candidates.length) break;
//         if (newUrlsForHost.length >= CONFIG.maxPagesPerDomain) break;
//         const url = candidates[i];
//         await sleep(CONFIG.rateMs);
//         const info = await fetchHtml(url).catch((e) => {
//           console.log('[ERR FETCH]', url, e && e.message ? e.message : e);
//           return null;
//         });
//         if (!info) continue;
//         out.push({ id: url, url, title: info.title, description: info.description, text: info.text });
//         newUrlsForHost.push(url);
//         total++;
//         console.log('[KEEP]', url, 'title-len=', (info.title || '').length, 'meta-len=', (info.description || '').length);
//       }
//     }

//     const tasks = [];
//     for (let w = 0; w < CONFIG.concurrency; w++) tasks.push(worker());
//     await Promise.all(tasks);

//     lastIndex[d.host] = { lastAt: nowMs(), urls: Array.from(new Set([...(hostRecord.urls || []), ...newUrlsForHost])) };

//     console.log(' Added', newUrlsForHost.length, 'new pages for', d.host);
//   }

//   // merge with existingOut, dedupe
//   const merged = [];
//   const seen = new Set();
//   for (const e of existingOut) {
//     if (!e || !e.url) continue;
//     if (seen.has(e.url)) continue;
//     seen.add(e.url);
//     merged.push(e);
//   }
//   for (const r of out) {
//     if (!r || !r.url) continue;
//     if (seen.has(r.url)) continue;
//     seen.add(r.url);
//     merged.push(r);
//   }

//   if (!fs.existsSync(path.dirname(OUT))) fs.mkdirSync(path.dirname(OUT), { recursive: true });
//   fs.writeFileSync(OUT, JSON.stringify(merged, null, 2), 'utf8');
//   console.log('Wrote', OUT, '->', merged.length, 'pages total (merged).');

//   fs.writeFileSync(LAST_INDEX_FILE, JSON.stringify(lastIndex, null, 2), 'utf8');
//   console.log('Updated', LAST_INDEX_FILE);
// })();


// crawler/generate_index_shard.js
//
// Shard-aware crawler worker: reads crawler/domains.csv (uses SECOND column per line; falls back to first),
// partitioned deterministically by hash(host) % shardCount.
// Writes per-shard outputs: shard-output-<shardIndex>.json and shard-last-<shardIndex>.json
//
// Example (Actions job):
// node crawler/generate_index_shard.js --shardIndex=0 --shardCount=50 --domains=./crawler/domains.csv \
//   --out=shard-output-0.json --last=shard-last-0.json --globalLast=./site/last_indexed.json \
//   --maxDomainsPerShard=2000 --concurrency=12 --maxPages=10 --daysNoCheck=4 --verbose

// crawler/generate_index_shard.js
const fs = require('fs');
const path = require('path');
const http = require('http');
const https = require('https');
const { JSDOM } = require('jsdom');
const zlib = require('zlib');
const { XMLParser } = require('fast-xml-parser');
const fetch = require('node-fetch');
const { URL } = require('url');

//
// CLI helpers
//
function parseArgInt(name, def) {
  const p = process.argv.find(a => a.startsWith(`--${name}=`));
  if (!p) return def;
  const v = Number(p.split('=')[1]);
  return Number.isFinite(v) ? v : def;
}
function parseArg(name, def) {
  const p = process.argv.find(a => a.startsWith(`--${name}=`));
  if (!p) return def;
  return p.split('=')[1];
}
function hasFlag(name) { return process.argv.includes(`--${name}`); }

const REPO_ROOT = process.cwd();
const DOMAINS_CSV = parseArg('domains', path.join(REPO_ROOT, 'crawler', 'domains.csv'));
const SHARD_INDEX = parseArgInt('shardIndex', 0);
const SHARD_COUNT = parseArgInt('shardCount', 1);
const OUT_FILE = parseArg('out', path.join(REPO_ROOT, `crawler`, `shard-output-${SHARD_INDEX}.json`));
const LAST_OUT_FILE = parseArg('last', path.join(REPO_ROOT, `crawler`, `shard-last-${SHARD_INDEX}.json`));
const GLOBAL_LAST_FILE = parseArg('globalLast', path.join(REPO_ROOT, 'site', 'last_indexed.json'));

const CONFIG = {
  concurrency: parseArgInt('concurrency', 10),
  rateMs: parseArgInt('rateMs', 150),
  fetchTimeoutMs: parseArgInt('fetchTimeoutMs', 12000),
  maxPages: parseArgInt('maxPages', 10),
  maxDomainsPerShard: parseArgInt('maxDomainsPerShard', 2000),
  retries: parseArgInt('retries', 2),
  retryBackoffMs: parseArgInt('retryBackoffMs', 600),
  daysNoCheck: parseArgInt('daysNoCheck', 4),
  maxUrlsPerHost: parseArgInt('maxUrlsPerHost', 10000)
};

const VERBOSE = hasFlag('verbose');
const FORCE = hasFlag('force');
const USER_AGENT = 'EarthiliansCrawler/1.0 (+mailto:you@example.com)';

function log(...args){ if (VERBOSE) console.log(...args); }
function nowMs(){ return Date.now(); }
function daysToMs(d){ return d * 24 * 60 * 60 * 1000; }

process.on('unhandledRejection', (reason, p) => {
  console.error('[UNHANDLED REJECTION]', reason && reason.stack ? reason.stack : reason);
});
process.on('uncaughtException', err => {
  console.error('[UNCAUGHT EXCEPTION]', err && err.stack ? err.stack : err);
});

const httpAgent = new http.Agent({ keepAlive: true, maxSockets: CONFIG.concurrency });
const httpsAgent = new https.Agent({ keepAlive: true, maxSockets: CONFIG.concurrency });

async function fetchWithTimeout(url, opts = {}, timeout = CONFIG.fetchTimeoutMs) {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), timeout);
  try {
    const u = new URL(url);
    const agent = u.protocol === 'http:' ? httpAgent : httpsAgent;
    const res = await fetch(url, { ...opts, agent, signal: controller.signal });
    clearTimeout(id);
    return res;
  } catch (e) {
    clearTimeout(id);
    throw e;
  }
}

async function fetchSitemapText(url) {
  const res = await fetchWithTimeout(url, { headers: { 'User-Agent': USER_AGENT } }, CONFIG.fetchTimeoutMs);
  if (!res.ok) throw new Error('HTTP ' + res.status);
  const buf = Buffer.from(await res.arrayBuffer());
  const ct = (res.headers.get('content-type') || '').toLowerCase();
  if (url.toLowerCase().endsWith('.gz') || ct.includes('gzip')) {
    try { return zlib.gunzipSync(buf).toString('utf8'); } catch (e) { return buf.toString('utf8'); }
  }
  return buf.toString('utf8');
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

//
// Robust URL helpers
//
function safeNewUrl(input, base) {
  try {
    if (base) return new URL(input, base);
    return new URL(input);
  } catch (e) {
    return null;
  }
}

function normalizeDomainCandidate(raw) {
  if (!raw) return null;
  const s = raw.trim();
  if (!s) return null;
  if (/^https?:\/\//i.test(s)) {
    const u = safeNewUrl(s);
    if (!u) return null;
    return u.origin;
  }
  const hostOnly = s.replace(/^\/+|\/+$/g, '').split(/\s+/)[0];
  if (!hostOnly) return null;
  const cand = 'https://' + hostOnly;
  const u = safeNewUrl(cand);
  return u ? u.origin : null;
}

function hashHost(s) {
  let h = 5381;
  for (let i = 0; i < s.length; i++) h = ((h << 5) + h) + s.charCodeAt(i) | 0;
  return Math.abs(h) >>> 0;
}

function loadJsonSafe(p) { try { return JSON.parse(fs.readFileSync(p, 'utf8')); } catch (e) { return null; } }
function atomicWrite(p, obj) {
  const tmp = p + '.tmp.' + process.pid;
  const dir = path.dirname(p);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(tmp, JSON.stringify(obj, null, 2), 'utf8');
  fs.renameSync(tmp, p);
}

//
// SITEMAP expand — using safe URL resolution
//
async function expandSitemap(sitemapUrl, seen) {
  const q = [sitemapUrl]; const pages = [];
  while (q.length) {
    const s = q.shift(); if (!s || seen.has(s)) continue; seen.add(s);
    try {
      const xml = await fetchSitemapText(s);
      const locs = parseSitemapXmlText(xml);
      for (const l of locs) {
        let normObj = safeNewUrl(l);
        if (!normObj) normObj = safeNewUrl(l, s);
        if (!normObj) continue;
        const norm = normObj.toString();
        if (norm.toLowerCase().endsWith('.xml') || norm.toLowerCase().endsWith('.xml.gz')) q.push(norm);
        else pages.push(norm);
      }
    } catch (e) {
      log('[WARN] sitemap fetch fail', sitemapUrl, e && e.message ? e.message : e);
    }
    await new Promise(r => setTimeout(r, CONFIG.rateMs));
    if (pages.length >= CONFIG.maxPages) break;
  }
  return pages.slice(0, CONFIG.maxPages);
}

//
// Strict fetchHtml: use JSDOM and require title + meta + paragraphs; skip otherwise.
// Any parse/fetch error => return null (skip).
//
async function fetchHtml(url) {
  let attempt = 0;
  while (true) {
    try {
      const res = await fetchWithTimeout(url, { headers: { 'User-Agent': USER_AGENT } }, CONFIG.fetchTimeoutMs);
      if (!res.ok) {
        log('[SKIP NON-OK]', url, 'status=', res.status);
        return null;
      }

      const ct = (res.headers.get('content-type') || '').toLowerCase();
      if (!ct.includes('html')) {
        log('[SKIP NON-HTML]', url, 'content-type=', ct);
        return null;
      }

      let html = await res.text();

      // remove stylesheets and inline styles to avoid CSS parser crashes in JSDOM
      html = html.replace(/<style[\s\S]*?>[\s\S]*?<\/style\s*>/gi, '');
      html = html.replace(/<link[^>]+rel=(["']?)stylesheet\1[^>]*>/gi, '');
      html = html.replace(/\sstyle=(["'])(.*?)\1/gi, '');

      let dom;
      try {
        dom = new JSDOM(html);
      } catch (jsErr) {
        log('[SKIP JSDOM-ERR]', url, jsErr && jsErr.message ? jsErr.message : jsErr);
        return null;
      }

      const doc = dom.window.document;
      const title = doc.querySelector('title')?.textContent?.trim() || '';
      const metaDesc = doc.querySelector('meta[name="description"]')?.getAttribute('content')?.trim() || '';
      const ps = Array.from(doc.querySelectorAll('p')).map(p => p.textContent.trim()).filter(Boolean);
      const text = ps.join('\n').slice(0, CONFIG.maxTextChars);

      if (!title)      { log('[SKIP MISSING TITLE]', url); return null; }
      if (!metaDesc)   { log('[SKIP MISSING META]', url); return null; }
      if (!text)       { log('[SKIP MISSING TEXT]', url); return null; }

      return { title, description: metaDesc.slice(0, 100), text };
    } catch (e) {
      attempt++;
      if (attempt > CONFIG.retries) {
        log('[ERR FETCH GIVING UP]', url, e && e.message ? e.message : e);
        return null;
      }
      await new Promise(r => setTimeout(r, CONFIG.retryBackoffMs * Math.pow(2, attempt - 1)));
    }
  }
}

//
// MAIN
//
(async function main() {
  console.log('Shard', SHARD_INDEX, 'start: domainsCsv=', DOMAINS_CSV, 'shardCount=', SHARD_COUNT, 'maxDomainsPerShard=', CONFIG.maxDomainsPerShard);

  if (!fs.existsSync(DOMAINS_CSV)) {
    console.error('domains CSV missing:', DOMAINS_CSV);
    process.exit(1);
  }

  const globalLast = loadJsonSafe(GLOBAL_LAST_FILE) || {};

  // safe CSV streaming & shard selection
  const selected = [];
  const rs = fs.createReadStream(DOMAINS_CSV, { encoding: 'utf8' });
  let buf = '';
  let totalSeen = 0;
  for await (const chunk of rs) {
    buf += chunk;
    let idx;
    while ((idx = buf.indexOf('\n')) >= 0) {
      const line = buf.slice(0, idx).trim();
      buf = buf.slice(idx + 1);
      if (!line) continue;
      totalSeen++;

      const cols = line.split(',');
      const domainCandidate = (cols[1] && cols[1].trim()) || (cols[0] && cols[0].trim());
      if (!domainCandidate) continue;

      const normalizedBase = normalizeDomainCandidate(domainCandidate);
      if (!normalizedBase) {
        log('[SKIP INVALID DOMAIN LINE]', domainCandidate);
        continue;
      }

      const host = safeNewUrl(normalizedBase)?.host;
      if (!host) {
        log('[SKIP BAD HOST]', normalizedBase);
        continue;
      }

      const h = hashHost(host);
      if ((h % SHARD_COUNT) === SHARD_INDEX) {
        selected.push({ raw: normalizedBase, host });
        if (CONFIG.maxDomainsPerShard > 0 && selected.length >= CONFIG.maxDomainsPerShard) break;
      }
    }
    if (CONFIG.maxDomainsPerShard > 0 && selected.length >= CONFIG.maxDomainsPerShard) break;
  }
  // leftover last line
  if ((CONFIG.maxDomainsPerShard === 0 || selected.length < CONFIG.maxDomainsPerShard) && buf.trim()) {
    const cols = buf.trim().split(',');
    const domainCandidate = (cols[1] && cols[1].trim()) || (cols[0] && cols[0].trim());
    if (domainCandidate) {
      const normalizedBase = normalizeDomainCandidate(domainCandidate);
      if (normalizedBase) {
        const host = safeNewUrl(normalizedBase)?.host;
        if (host) {
          const h = hashHost(host);
          if ((h % SHARD_COUNT) === SHARD_INDEX) selected.push({ raw: normalizedBase, host });
        }
      } else {
        log('[SKIP INVALID DOMAIN LINE]', domainCandidate);
      }
    }
  }

  console.log('Shard', SHARD_INDEX, 'collected', selected.length, 'domains (file seen=', totalSeen, ')');

  let idx = 0;
  const outPages = [];
  const shardLast = {};

  async function worker() {
    while (true) {
      const i = idx++;
      if (i >= selected.length) break;
      const item = selected[i];

      try {
        const host = item.host;
        const rec = globalLast[host] || { lastAt: 0, urls: [] };
        const recently = !FORCE && rec.lastAt && ((nowMs() - rec.lastAt) < daysToMs(CONFIG.daysNoCheck));
        if (recently) {
          log('[SKIP-RECENT]', host);
          shardLast[host] = { lastAt: rec.lastAt, urls: Array.from(new Set(rec.urls || [])) };
          continue;
        }

        // guarded robots + fallback sitemap collection
        const domainSitemaps = new Set();
        try {
          const baseOrigin = item.raw;
          const robotsUrlObj = safeNewUrl('/robots.txt', baseOrigin);
          if (robotsUrlObj) {
            const r = await fetchWithTimeout(robotsUrlObj.toString(), { headers: { 'User-Agent': USER_AGENT } }, 6000).catch(() => null);
            if (r && r.ok) {
              const txt = await r.text().catch(() => '');
              for (const line of txt.split(/\r?\n/)) {
                if (line.toLowerCase().startsWith('sitemap:')) {
                  const smRaw = line.split(':').slice(1).join(':').trim();
                  const smObj = safeNewUrl(smRaw, robotsUrlObj.toString());
                  if (smObj) domainSitemaps.add(smObj.toString());
                  else log('[WARN] ignored invalid sitemap URL in robots:', smRaw, 'for', host);
                }
              }
            }
          }
        } catch (e) {
          log('[WARN] robots handling failed', item.host, e && e.message ? e.message : e);
        }

        // always add fallback sitemap
        const fallbackSitemap = safeNewUrl('/sitemap.xml', item.raw);
        if (fallbackSitemap) domainSitemaps.add(fallbackSitemap.toString());

        // expand sitemaps
        let pageUrls = [];
        const seenSitemaps = new Set();
        for (const sm of domainSitemaps) {
          try {
            const pages = await expandSitemap(sm, seenSitemaps);
            for (const p of pages) if (!pageUrls.includes(p)) pageUrls.push(p);
          } catch (e) { log('[WARN] sitemap expand fail', item.host, e && e.message ? e.message : e); }
          if (pageUrls.length >= 1000) break;
        }

        // ensure homepage is first
        const homepage = safeNewUrl('/', item.raw)?.toString() || (item.raw.endsWith('/') ? item.raw : item.raw + '/');
        if (!pageUrls.includes(homepage)) pageUrls.unshift(homepage);

        const already = new Set(rec.urls || []);
        const candidates = pageUrls.filter(u => !already.has(u)).slice(0, CONFIG.maxPages);

        if (candidates.length === 0) {
          shardLast[item.host] = { lastAt: nowMs(), urls: Array.from(new Set(rec.urls || [])) };
          log('[NO-NEW]', item.host);
          continue;
        }

        const got = [];
        for (const url of candidates) {
          await new Promise(r => setTimeout(r, CONFIG.rateMs));
          const info = await fetchHtml(url).catch(e => { log('[ERR FETCH]', url, e && e.message ? e.message : e); return null; });
          if (!info) continue;
          outPages.push({ id: url, url, title: info.title, description: info.description, text: info.text });
          got.push(url);
          if (got.length >= CONFIG.maxPages) break;
        }

        const mergedUrls = Array.from(new Set([...(rec.urls || []), ...got]));
        if (mergedUrls.length > CONFIG.maxUrlsPerHost) mergedUrls.splice(CONFIG.maxUrlsPerHost);

        shardLast[item.host] = { lastAt: nowMs(), urls: mergedUrls };
        log('[DONE]', item.host, 'added', got.length);

      } catch (domainErr) {
        console.error('[ERROR] domain worker error for', item && item.host ? item.host : item, domainErr && domainErr.stack ? domainErr.stack : domainErr);
        try {
          shardLast[item.host || ('bad-host-' + i)] = { lastAt: nowMs(), urls: (shardLast[item.host] && shardLast[item.host].urls) || [] };
        } catch (e) {}
        continue;
      }
    }
  }

  const pool = [];
  for (let w = 0; w < CONFIG.concurrency; w++) pool.push(worker());
  await Promise.all(pool);

  atomicWrite(OUT_FILE, outPages);
  atomicWrite(LAST_OUT_FILE, shardLast);
  console.log('Shard', SHARD_INDEX, 'wrote', OUT_FILE, 'pages=', outPages.length, 'hosts=', Object.keys(shardLast).length);

  httpAgent.destroy();
  httpsAgent.destroy();
  process.exit(0);
})().catch(err => {
  console.error('FATAL', err && err.stack ? err.stack : err);
  process.exit(1);
});
