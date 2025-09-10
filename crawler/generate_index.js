// crawler/generate_index.js
// CommonJS version that avoids ESM-only deps (no p-limit require).
// Uses native fetch (Node 18+). Uses csv-parse, jsdom, fast-xml-parser.
// Minimal concurrency limiter implemented inline.

const fs = require('fs');
const path = require('path');
const { parse } = require('csv-parse/sync');
const { JSDOM } = require('jsdom');
const zlib = require('zlib');
const { XMLParser } = require('fast-xml-parser');
const pRetry = require('p-retry'); // p-retry is CommonJS-compatible in most versions
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

// Minimal concurrency limiter factory (CommonJS-friendly).
// Usage: const limiter = createLimiter(concurrency);
// await limiter(() => asyncWork());
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
      // try to start jobs immediately
      next();
    });
  };
}

// fetch with timeout using global fetch (Node 18+)
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

(async function main() {
  log('Starting crawler run (no p-limit)');

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
        out.push({ id: url, url, title: info.title, description: info.description, text: info.text });
        newUrlsForHost.push(url);
        total++;
        log('[KEEP]', url, 'title-len=', (info.title || '').length, 'meta-len=', (info.description || '').length);
      } catch (e) {
        log('[ERR FETCH]', url, e && e.message ? e.message : e);
      }
    }));

    // wait for all scheduled tasks for this domain to complete
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
