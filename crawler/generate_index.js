// // crawler/generate_index.js
// // CommonJS-only — no p-limit, no p-retry. Uses native fetch (Node 18+).
// // Enforces 100-character limit on title/description/text (CONFIG.maxFieldChars).

// const fs = require('fs');
// const path = require('path');
// const { parse } = require('csv-parse/sync');
// const { JSDOM } = require('jsdom');
// const zlib = require('zlib');
// const { XMLParser } = require('fast-xml-parser');
// const { URL } = require('url');

// // files
// const DOMAINS_CSV = path.join(__dirname, 'domains.csv');
// const OUT = path.join(__dirname, '..', 'site', 'index.json');
// const LAST_INDEX_FILE = path.join(__dirname, '..', 'site', 'last_indexed.json');
// const LOG_FILE = path.join(__dirname, '..', 'site', 'crawler.log');

// // CONFIG - environment overrides allowed
// const CONFIG = {
//   USER_AGENT: process.env.USER_AGENT || 'EarthiliansCrawler/1.0 (+mailto:you@example.com)',
//   concurrency: Number(process.env.CRAWL_CONCURRENCY || 5),
//   rateMs: Number(process.env.CRAWL_RATE_MS || 200),
//   fetchTimeoutMs: Number(process.env.CRAWL_FETCH_TIMEOUT_MS || 12000),
//   maxPagesPerDomain: Number(process.env.CRAWL_PAGES_PER_DOMAIN || 10),
//   maxDomainsPerRun: Number(process.env.CRAWL_DOMAINS_PER_RUN || 3000),
//   maxTotalPages: Number(process.env.CRAWL_MAX_TOTAL_PAGES || 3000 * 10),
//   maxTextChars: Number(process.env.CRAWL_MAX_TEXT_CHARS || 120000), // used for initial paragraph aggregation
//   maxFieldChars: Number(process.env.CRAWL_MAX_FIELD_CHARS || 100), // <=100 chars for title/description/text output
//   retries: Number(process.env.CRAWL_RETRIES || 2),
//   retryBackoffMs: Number(process.env.CRAWL_RETRY_BACKOFF_MS || 600),
//   daysNoCheck: Number(process.env.CRAWL_DAYS_NO_CHECK || 4),
//   batchSize: Number(process.env.CRAWL_BATCH_SIZE || 3000)
// };

// function truncate(s, n) {
//   if (!s) return '';
//   const str = String(s);
//   return str.length <= n ? str : str.slice(0, n);
// }

// function log(...args) {
//   const line = new Date().toISOString() + ' ' + args.map(a => (typeof a === 'string' ? a : JSON.stringify(a))).join(' ');
//   console.log(line);
//   try { fs.appendFileSync(LOG_FILE, line + '\n'); } catch (e) { /* ignore log errors */ }
// }

// function nowMs() { return Date.now(); }
// function daysToMs(d) { return d * 24 * 60 * 60 * 1000; }

// if (!fs.existsSync(DOMAINS_CSV)) {
//   log('ERROR: domains.csv missing; create one with CSV and put domain in second column');
//   process.exit(1);
// }

// function loadDomainsFromCsv(p) {
//   const raw = fs.readFileSync(p, 'utf8');
//   const records = parse(raw, { skip_empty_lines: true });
//   const out = [];
//   for (const r of records) {
//     const val = (r[1] && String(r[1]).trim()) || (r[0] && String(r[0]).trim());
//     if (!val) continue;
//     let candidate = val.trim();
//     if (!/^https?:\/\//i.test(candidate)) candidate = 'https://' + candidate.replace(/\/+$/, '');
//     try {
//       const u = new URL(candidate);
//       out.push({ base: u.origin, host: u.host });
//     } catch (e) {
//       // skip invalid
//     }
//   }
//   return out;
// }

// // small retry helper (replaces p-retry)
// async function retry(fn, opts = {}) {
//   const retries = typeof opts.retries === 'number' ? opts.retries : 2;
//   const factor = typeof opts.factor === 'number' ? opts.factor : 2;
//   const minTimeout = typeof opts.minTimeout === 'number' ? opts.minTimeout : 100;
//   const maxTimeout = typeof opts.maxTimeout === 'number' ? opts.maxTimeout : 20000;

//   let attempt = 0;
//   while (true) {
//     try {
//       return await fn();
//     } catch (err) {
//       attempt++;
//       if (attempt > retries) throw err;
//       const backoff = Math.min(maxTimeout, Math.round(minTimeout * Math.pow(factor, attempt - 1)));
//       await new Promise(r => setTimeout(r, backoff));
//     }
//   }
// }

// // minimal concurrency limiter (replaces p-limit)
// function createLimiter(concurrency) {
//   let active = 0;
//   const queue = [];
//   function next() {
//     if (active >= concurrency) return;
//     const job = queue.shift();
//     if (!job) return;
//     active++;
//     job().finally(() => {
//       active--;
//       next();
//     });
//   }
//   return (fn) => {
//     return new Promise((resolve, reject) => {
//       const job = async () => {
//         try {
//           const res = await fn();
//           resolve(res);
//         } catch (err) {
//           reject(err);
//         }
//       };
//       queue.push(job);
//       // trigger
//       next();
//     });
//   };
// }

// // fetch with timeout using global fetch (Node 18+). If Node <18, install node-fetch and replace.
// async function fetchWithTimeout(url, opts = {}, timeout = CONFIG.fetchTimeoutMs) {
//   const controller = new AbortController();
//   const id = setTimeout(() => controller.abort(), timeout);
//   try {
//     const res = await globalThis.fetch(url, { ...opts, signal: controller.signal });
//     clearTimeout(id);
//     return res;
//   } catch (e) {
//     clearTimeout(id);
//     throw e;
//   }
// }

// async function fetchSitemapText(url) {
//   return await retry(async () => {
//     const res = await fetchWithTimeout(url, { headers: { 'User-Agent': CONFIG.USER_AGENT } }, CONFIG.fetchTimeoutMs);
//     if (!res.ok) throw new Error('HTTP ' + res.status);
//     const buf = Buffer.from(await res.arrayBuffer());
//     const ct = (res.headers.get('content-type') || '').toLowerCase();
//     if (url.toLowerCase().endsWith('.gz') || ct.includes('gzip')) {
//       try { return zlib.gunzipSync(buf).toString('utf8'); } catch (e) { return buf.toString('utf8'); }
//     }
//     return buf.toString('utf8');
//   }, { retries: 1, minTimeout: CONFIG.retryBackoffMs });
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

// async function expandSitemap(sitemapUrl, seen, domainAllowed, maxPagesPerDomain) {
//   const q = [sitemapUrl];
//   const pages = [];
//   while (q.length) {
//     const s = q.shift();
//     if (!s || seen.has(s)) continue;
//     seen.add(s);
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
//       // ignore sitemap fetch errors per domain
//     }
//     await sleep(CONFIG.rateMs);
//     if (pages.length >= maxPagesPerDomain) break;
//   }
//   return pages;
// }

// async function fetchHtml(url) {
//   return await retry(async () => {
//     const res = await fetchWithTimeout(url, { headers: { 'User-Agent': CONFIG.USER_AGENT } }, CONFIG.fetchTimeoutMs);
//     if (!res.ok) throw new Error('HTTP ' + res.status);
//     const ct = (res.headers.get('content-type') || '').toLowerCase();
//     if (!ct.includes('html')) throw new Error('non-html ' + ct);
//     const html = await res.text();
//     const dom = new JSDOM(html);
//     const doc = dom.window.document;
//     const titleRaw = doc.querySelector('title')?.textContent?.trim() || '';
//     const rawMeta = doc.querySelector('meta[name="description"]')?.getAttribute('content')?.trim() || '';
//     const ps = Array.from(doc.querySelectorAll('p')).map(p => p.textContent.trim()).filter(Boolean);
//     const textRaw = ps.join('\n').slice(0, CONFIG.maxTextChars);

//     // enforce field length limits
//     const title = truncate(titleRaw, CONFIG.maxFieldChars);
//     const description = truncate(rawMeta, CONFIG.maxFieldChars);
//     const text = truncate(textRaw, CONFIG.maxFieldChars);

//     return { title, description, text };
//   }, { retries: CONFIG.retries, minTimeout: CONFIG.retryBackoffMs });
// }

// function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
// function loadJsonSafe(p) { try { return JSON.parse(fs.readFileSync(p, 'utf8')); } catch (e) { return null; } }

// (async function main() {
//   log('Starting crawler run (100-char field limits enforced)');

//   const domains = loadDomainsFromCsv(DOMAINS_CSV);
//   log('Loaded domains count=', domains.length);

//   const lastIndex = loadJsonSafe(LAST_INDEX_FILE) || {};
//   const existingOut = loadJsonSafe(OUT) || [];

//   const seenPages = new Set(existingOut.map(x => x.url));
//   const seenSitemaps = new Set();
//   const out = [];
//   let total = 0;

//   function recentlyIndexed(host) {
//     const rec = lastIndex[host];
//     if (!rec || !rec.lastAt) return false;
//     return (nowMs() - rec.lastAt) < daysToMs(CONFIG.daysNoCheck);
//   }

//   function domainAllowedFactory(domainsList) {
//     const hostList = domainsList.map(d => d.host);
//     return (url) => {
//       try {
//         const h = new URL(url).host;
//         return hostList.some(hd => h === hd || h.endsWith('.' + hd));
//       } catch (e) {
//         return false;
//       }
//     };
//   }

//   const domainAllowed = domainAllowedFactory(domains);

//   // decide which domains to process this run (batch)
//   const toProcess = [];
//   for (const d of domains) {
//     if (toProcess.length >= Math.min(CONFIG.batchSize, CONFIG.maxDomainsPerRun)) break;
//     if (recentlyIndexed(d.host)) continue;
//     toProcess.push(d);
//   }

//   log('Domains to process this run=', toProcess.length);

//   // create limiter instance
//   const limiter = createLimiter(CONFIG.concurrency);

//   for (const d of toProcess) {
//     if (total >= CONFIG.maxTotalPages) break;
//     log('Processing domain=', d.base);

//     const domainSitemaps = new Set();

//     try {
//       const robots = new URL('/robots.txt', d.base).toString();
//       const r = await fetchWithTimeout(robots, { headers: { 'User-Agent': CONFIG.USER_AGENT } }, 6000);
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
//       log('[WARN] robots parse error for', d.base, e && e.message ? e.message : e);
//     }

//     domainSitemaps.add(new URL('/sitemap.xml', d.base).toString());

//     // expand sitemaps
//     let pageUrls = [];
//     for (const sm of domainSitemaps) {
//       try {
//         const pages = await expandSitemap(sm, seenSitemaps, domainAllowed, CONFIG.maxPagesPerDomain);
//         for (const p of pages) {
//           if (!seenPages.has(p)) { pageUrls.push(p); seenPages.add(p); }
//           if (pageUrls.length >= 1000) break;
//         }
//       } catch (e) {
//         log('[WARN] sitemap fallback error for', d.base, e && e.message ? e.message : e);
//       }
//       if (pageUrls.length >= 1000) break;
//     }

//     const homepage = new URL('/', d.base).toString();
//     if (!pageUrls.includes(homepage) && !seenPages.has(homepage)) pageUrls.unshift(homepage);

//     log(' Domain', d.base, 'collected urls=', pageUrls.length);

//     const hostRecord = lastIndex[d.host] || { lastAt: 0, urls: [] };
//     const alreadyIndexedSet = new Set(hostRecord.urls || []);
//     const candidates = pageUrls.filter(u => !alreadyIndexedSet.has(u));

//     if (candidates.length === 0) {
//       log(' No new pages for', d.host);
//       lastIndex[d.host] = { lastAt: nowMs(), urls: hostRecord.urls || [] };
//       continue;
//     }

//     const candidatesLimited = candidates.slice(0, CONFIG.maxPagesPerDomain);
//     const newUrlsForHost = [];

//     // schedule fetch tasks via limiter
//     const tasks = candidatesLimited.map(url => limiter(async () => {
//       await sleep(CONFIG.rateMs);
//       try {
//         const info = await fetchHtml(url);
//         if (!info) return;
//         // ensure final truncation before push (belt-and-suspenders)
//         const title = truncate(info.title, CONFIG.maxFieldChars);
//         const description = truncate(info.description, CONFIG.maxFieldChars);
//         const text = truncate(info.text, CONFIG.maxFieldChars);

//         out.push({ id: url, url, title, description, text });
//         newUrlsForHost.push(url);
//         total++;
//         log('[KEEP]', url, 'title-len=', (title || '').length, 'meta-len=', (description || '').length);
//       } catch (e) {
//         log('[ERR FETCH]', url, e && e.message ? e.message : e);
//       }
//     }));

//     // wait for all scheduled tasks for this domain to complete
//     await Promise.all(tasks);

//     lastIndex[d.host] = { lastAt: nowMs(), urls: Array.from(new Set([...(hostRecord.urls || []), ...newUrlsForHost])) };
//     log(' Added', newUrlsForHost.length, 'new pages for', d.host);
//   }

//   // merge with existingOut, dedupe
//   const merged = [];
//   const seen = new Set();
//   for (const e of existingOut) {
//     if (!e || !e.url) continue;
//     if (seen.has(e.url)) continue;
//     // truncate existing entries too (in case)
//     const title = truncate(e.title || '', CONFIG.maxFieldChars);
//     const description = truncate(e.description || '', CONFIG.maxFieldChars);
//     const text = truncate(e.text || '', CONFIG.maxFieldChars);
//     seen.add(e.url);
//     merged.push({ ...e, title, description, text });
//   }
//   for (const r of out) {
//     if (!r || !r.url) continue;
//     if (seen.has(r.url)) continue;
//     seen.add(r.url);
//     merged.push(r);
//   }

//   if (!fs.existsSync(path.dirname(OUT))) fs.mkdirSync(path.dirname(OUT), { recursive: true });
//   fs.writeFileSync(OUT, JSON.stringify(merged, null, 2), 'utf8');
//   log('Wrote', OUT, '->', merged.length, 'pages total (merged).');

//   fs.writeFileSync(LAST_INDEX_FILE, JSON.stringify(lastIndex, null, 2), 'utf8');
//   log('Updated', LAST_INDEX_FILE);
//   log('Crawler run completed — total new pages this run=', out.length);
// })();


// crawler/generate_index.js
// CommonJS-only — no p-limit, no p-retry. Uses native fetch (Node 18+).
// Enforces 100-character limit on title/description/text (CONFIG.maxFieldChars).

const fs = require('fs');
const path = require('path');
const { parse } = require('csv-parse/sync');
const { JSDOM } = require('jsdom');
const zlib = require('zlib');
const { XMLParser } = require('fast-xml-parser');
const { URL } = require('url');
const crypto = require('crypto');

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
  maxTextChars: Number(process.env.CRAWL_MAX_TEXT_CHARS || 120000), // used for initial paragraph aggregation
  maxFieldChars: Number(process.env.CRAWL_MAX_FIELD_CHARS || 100), // <=100 chars for title/description/text output
  retries: Number(process.env.CRAWL_RETRIES || 2),
  retryBackoffMs: Number(process.env.CRAWL_RETRY_BACKOFF_MS || 600),
  daysNoCheck: Number(process.env.CRAWL_DAYS_NO_CHECK || 4),
  batchSize: Number(process.env.CRAWL_BATCH_SIZE || 3000)
};

function truncate(s, n) {
  if (!s) return '';
  const str = String(s);
  return str.length <= n ? str : str.slice(0, n);
}

function log(...args) {
  const line = new Date().toISOString() + ' ' + args.map(a => (typeof a === 'string' ? a : JSON.stringify(a))).join(' ');
  console.log(line);
  try { fs.appendFileSync(LOG_FILE, line + '\n'); } catch (e) { /* ignore log errors */ }
}

function nowMs() { return Date.now(); }
function daysToMs(d) { return d * 24 * 60 * 60 * 1000; }

if (!fs.existsSync(DOMAINS_CSV)) {
  log('ERROR: domains.csv missing; create one with CSV and put domain in second column');
  process.exit(1);
}

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

// Normalize incoming URL-like strings:
// - If input is Markdown link like: [https://x](https://x) extract the (target).
// - If missing scheme, try to add https://
// - Return canonical new URL(...).toString() when possible, else return trimmed raw input.
function normalizeUrl(raw) {
  if (!raw) return '';
  let s = String(raw).trim();
  // If it looks like markdown [text](url), grab the url inside parentheses
  const mdMatch = s.match(/^\[.*?\]\((https?:\/\/[^)]+)\)$/i);
  if (mdMatch) s = mdMatch[1];
  // If it was wrapped like "<https://...>" remove < >
  if (s.startsWith('<') && s.endsWith('>')) s = s.slice(1, -1);
  // If scheme missing, add https://
  if (!/^[a-zA-Z][a-zA-Z0-9+.-]*:\/\//.test(s)) {
    s = 'https://' + s.replace(/^\/+/, '');
  }
  try {
    return new URL(s).toString();
  } catch (e) {
    return s; // best effort
  }
}

// produce deterministic id from canonical URL (sha1 hex)
function makeId(url) {
  if (!url) return '';
  try {
    const canonical = normalizeUrl(url);
    return crypto.createHash('sha1').update(canonical).digest('hex');
  } catch (e) {
    return crypto.createHash('sha1').update(String(url)).digest('hex');
  }
}

// small retry helper (replaces p-retry)
async function retry(fn, opts = {}) {
  const retries = typeof opts.retries === 'number' ? opts.retries : 2;
  const factor = typeof opts.factor === 'number' ? opts.factor : 2;
  const minTimeout = typeof opts.minTimeout === 'number' ? opts.minTimeout : 100;
  const maxTimeout = typeof opts.maxTimeout === 'number' ? opts.maxTimeout : 20000;

  let attempt = 0;
  while (true) {
    try {
      return await fn();
    } catch (err) {
      attempt++;
      if (attempt > retries) throw err;
      const backoff = Math.min(maxTimeout, Math.round(minTimeout * Math.pow(factor, attempt - 1)));
      await new Promise(r => setTimeout(r, backoff));
    }
  }
}

// minimal concurrency limiter (replaces p-limit)
function createLimiter(concurrency) {
  let active = 0;
  const queue = [];
  function next() {
    if (active >= concurrency) return;
    const job = queue.shift();
    if (!job) return;
    active++;
    job().finally(() => {
      active--;
      next();
    });
  }
  return (fn) => {
    return new Promise((resolve, reject) => {
      const job = async () => {
        try {
          const res = await fn();
          resolve(res);
        } catch (err) {
          reject(err);
        }
      };
      queue.push(job);
      // trigger
      next();
    });
  };
}

// fetch with timeout using global fetch (Node 18+). If Node <18, install node-fetch and replace.
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

async function fetchSitemapText(url) {
  return await retry(async () => {
    const res = await fetchWithTimeout(url, { headers: { 'User-Agent': CONFIG.USER_AGENT } }, CONFIG.fetchTimeoutMs);
    if (!res.ok) throw new Error('HTTP ' + res.status);
    const buf = Buffer.from(await res.arrayBuffer());
    const ct = (res.headers.get('content-type') || '').toLowerCase();
    if (url.toLowerCase().endsWith('.gz') || ct.includes('gzip')) {
      try { return zlib.gunzipSync(buf).toString('utf8'); } catch (e) { return buf.toString('utf8'); }
    }
    return buf.toString('utf8');
  }, { retries: 1, minTimeout: CONFIG.retryBackoffMs });
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
    if (out.length === 0) {
      for (const m of xml.matchAll(/<loc>([^<]+)<\/loc>/gi)) out.push(m[1].trim());
    }
    return out;
  } catch (e) {
    const out = []; for (const m of xml.matchAll(/<loc>([^<]+)<\/loc>/gi)) out.push(m[1].trim()); return out;
  }
}

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

async function fetchHtml(url) {
  return await retry(async () => {
    const res = await fetchWithTimeout(url, { headers: { 'User-Agent': CONFIG.USER_AGENT } }, CONFIG.fetchTimeoutMs);
    if (!res.ok) throw new Error('HTTP ' + res.status);
    const ct = (res.headers.get('content-type') || '').toLowerCase();
    if (!ct.includes('html')) throw new Error('non-html ' + ct);
    const html = await res.text();
    const dom = new JSDOM(html);
    const doc = dom.window.document;
    const titleRaw = doc.querySelector('title')?.textContent?.trim() || '';
    const rawMeta = doc.querySelector('meta[name="description"]')?.getAttribute('content')?.trim() || '';
    const ps = Array.from(doc.querySelectorAll('p')).map(p => p.textContent.trim()).filter(Boolean);
    const textRaw = ps.join('\n').slice(0, CONFIG.maxTextChars);

    // enforce field length limits
    const title = truncate(titleRaw, CONFIG.maxFieldChars);
    const description = truncate(rawMeta, CONFIG.maxFieldChars);
    const text = truncate(textRaw, CONFIG.maxFieldChars);

    return { title, description, text };
  }, { retries: CONFIG.retries, minTimeout: CONFIG.retryBackoffMs });
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function loadJsonSafe(p) { try { return JSON.parse(fs.readFileSync(p, 'utf8')); } catch (e) { return null; } }

(async function main() {
  log('Starting crawler run (100-char field limits enforced)');

  const domains = loadDomainsFromCsv(DOMAINS_CSV);
  log('Loaded domains count=', domains.length);

  const lastIndex = loadJsonSafe(LAST_INDEX_FILE) || {};
  const existingOut = loadJsonSafe(OUT) || [];

  // seenPages keyed by canonical url (normalize existing entries)
  const seenPages = new Set(existingOut.map(x => normalizeUrl(x && (x.url || x.id))).filter(Boolean));
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

  // create limiter instance
  const limiter = createLimiter(CONFIG.concurrency);

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

    // schedule fetch tasks via limiter
    const tasks = candidatesLimited.map(url => limiter(async () => {
      await sleep(CONFIG.rateMs);
      try {
        const info = await fetchHtml(url);
        if (!info) return;
        // normalize url and compute id
        const canonicalUrl = normalizeUrl(url);
        const recordId = makeId(canonicalUrl);

        // ensure final truncation before push (belt-and-suspenders)
        const title = truncate(info.title, CONFIG.maxFieldChars);
        const description = truncate(info.description, CONFIG.maxFieldChars);
        const text = truncate(info.text, CONFIG.maxFieldChars);

        out.push({ id: recordId, url: canonicalUrl, title, description, text });
        newUrlsForHost.push(canonicalUrl);
        total++;
        log('[KEEP]', canonicalUrl, 'title-len=', (title || '').length, 'meta-len=', (description || '').length);
      } catch (e) {
        log('[ERR FETCH]', url, e && e.message ? e.message : e);
      }
    }));

    // wait for all scheduled tasks for this domain to complete
    await Promise.all(tasks);

    lastIndex[d.host] = { lastAt: nowMs(), urls: Array.from(new Set([...(hostRecord.urls || []), ...newUrlsForHost])) };
    log(' Added', newUrlsForHost.length, 'new pages for', d.host);
  }

  // merge with existingOut, dedupe by canonical url
  const merged = [];
  const seen = new Set();
  for (const e of existingOut) {
    if (!e || !(e.url || e.id)) continue;
    const urlVal = normalizeUrl(e.url || e.id);
    if (!urlVal) continue;
    if (seen.has(urlVal)) continue;
    // re-truncate existing entries too (in case)
    const title = truncate(e.title || '', CONFIG.maxFieldChars);
    const description = truncate(e.description || '', CONFIG.maxFieldChars);
    const text = truncate(e.text || '', CONFIG.maxFieldChars);
    const id = makeId(urlVal);
    seen.add(urlVal);
    merged.push({ id, url: urlVal, title, description, text });
  }
  for (const r of out) {
    if (!r || !r.url) continue;
    const urlVal = normalizeUrl(r.url);
    if (!urlVal) continue;
    if (seen.has(urlVal)) continue;
    seen.add(urlVal);
    const id = r.id || makeId(urlVal);
    merged.push({
      id,
      url: urlVal,
      title: truncate(r.title || '', CONFIG.maxFieldChars),
      description: truncate(r.description || '', CONFIG.maxFieldChars),
      text: truncate(r.text || '', CONFIG.maxFieldChars)
    });
  }

  if (!fs.existsSync(path.dirname(OUT))) fs.mkdirSync(path.dirname(OUT), { recursive: true });
  fs.writeFileSync(OUT, JSON.stringify(merged, null, 2), 'utf8');
  log('Wrote', OUT, '->', merged.length, 'pages total (merged).');

  fs.writeFileSync(LAST_INDEX_FILE, JSON.stringify(lastIndex, null, 2), 'utf8');
  log('Updated', LAST_INDEX_FILE);
  log('Crawler run completed — total new pages this run=', out.length);
})();
