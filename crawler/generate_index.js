// const fs = require('fs');
// const path = require('path');
// const fetch = require('node-fetch');
// const { JSDOM } = require('jsdom');
// const zlib = require('zlib');
// const { XMLParser } = require('fast-xml-parser');
// const { URL } = require('url');

// const DOMAINS_FILE = './domains.txt';
// const OUT = '../site/index.json';
// const USER_AGENT = 'EarthiliansCrawler/1.0 (+mailto:you@example.com)';

// const CONFIG = {
//   concurrency: 5,
//   rateMs: 200,
//   fetchTimeoutMs: 12000,
//   maxPagesPerDomain: 500,
//   maxTotalPages: 1000,
//   maxTextChars: 120000,
//   retries: 2,
//   retryBackoffMs: 600,
//   requireMetaDescription: true,
//   keepIfPriorityPath: true,
//   // <-- CORRECT: use a regex literal (no double-escaped backslashes)
//   priorityPathRegex: /\/(blog|post|article|news|product|item|guide|docs)\b/i
// };

// if (!fs.existsSync(DOMAINS_FILE)) {
//   console.error('domains.txt missing; add one domain per line (e.g. https://example.com)');
//   process.exit(1);
// }

// const raw = fs.readFileSync(DOMAINS_FILE, 'utf8')
//   .split(/\r?\n/)               // correct: single-escaped regex literal
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

// // sitemap helpers (handles .gz)
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

// // fetch HTML page and extract title + meta description + body snippet
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
//       const metaDesc = doc.querySelector('meta[name="description"]')?.getAttribute('content')?.trim() || '';
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

// // MAIN
// (async function main() {
//   const seenPages = new Set();
//   const seenSitemaps = new Set();
//   const out = [];
//   let total = 0;

//   for (const d of domains) {
//     if (total >= CONFIG.maxTotalPages) break;
//     console.log('Processing', d.base);
//     const domainSitemaps = new Set();
//     // robots.txt
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
//     } catch (e) { /* ignore */ }
//     domainSitemaps.add(new URL('/sitemap.xml', d.base).toString());

//     // expand sitemaps
//     let pageUrls = [];
//     for (const sm of domainSitemaps) {
//       try {
//         const pages = await expandSitemap(sm, seenSitemaps);
//         for (const p of pages) { if (!seenPages.has(p)) { pageUrls.push(p); seenPages.add(p); } if (pageUrls.length >= CONFIG.maxPagesPerDomain) break; }
//       } catch (e) { /* ignore */ }
//       if (pageUrls.length >= CONFIG.maxPagesPerDomain) break;
//     }

//     // ensure homepage first
//     const homepage = new URL('/', d.base).toString();
//     if (!seenPages.has(homepage)) { pageUrls.unshift(homepage); seenPages.add(homepage); }

//     let idx = 0;
//     async function worker() {
//       while (true) {
//         const i = idx++;
//         if (i >= pageUrls.length || total >= CONFIG.maxTotalPages) break;
//         const url = pageUrls[i];
//         await sleep(CONFIG.rateMs);
//         const info = await fetchHtml(url).catch(() => null);
//         if (!info) continue;
//         const hasTitle = !!(info.title && info.title.trim());
//         const hasMeta = !!(info.description && info.description.trim());
//         const isPriority = CONFIG.keepIfPriorityPath && CONFIG.priorityPathRegex.test(new URL(url).pathname);
//         const keep = (hasTitle && hasMeta) || isPriority;
//         if (keep) {
//           out.push({ id: url, url, title: info.title, description: info.description, text: info.text });
//           total++;
//           console.log('[KEEP]', url, 'title-len=', (info.title || '').length, 'meta-len=', (info.description || '').length);
//         } else {
//           console.log('[SKIP]', url, `title=${hasTitle} meta=${hasMeta} priority=${isPriority}`);
//         }
//       }
//     }

//     const tasks = [];
//     for (let w = 0; w < CONFIG.concurrency; w++) tasks.push(worker());
//     await Promise.all(tasks);
//   }

//   // final dedupe & write
//   const final = [];
//   const seen = new Set();
//   for (const r of out) {
//     if (!r || !r.url) continue;
//     if (seen.has(r.url)) continue;
//     seen.add(r.url);
//     final.push(r);
//   }

//   if (!fs.existsSync(path.dirname(OUT))) fs.mkdirSync(path.dirname(OUT), { recursive: true });
//   fs.writeFileSync(OUT, JSON.stringify(final, null, 2), 'utf8');
//   console.log('Wrote', OUT, '->', final.length, 'important pages indexed');
// })();


const fs = require('fs');
const path = require('path');
const fetch = require('node-fetch');
const { JSDOM } = require('jsdom');
const zlib = require('zlib');
const { XMLParser } = require('fast-xml-parser');
const { URL } = require('url');

const DOMAINS_FILE = './domains.txt';
const OUT = '../site/index.json';
const LAST_INDEX_FILE = './.last_indexed.json';
const USER_AGENT = 'EarthiliansCrawler/1.0 (+mailto:you@example.com)';

const CONFIG = {
  concurrency: 5,
  rateMs: 200,
  fetchTimeoutMs: 12000,
  // per-site cap reduced to 10 as requested
  maxPagesPerDomain: 10,
  maxTotalPages: 1000,
  // only keep up to 100k text chars for body snippet, but meta desc will be trimmed to 100 chars
  maxTextChars: 120000,
  retries: 2,
  retryBackoffMs: 600,
  requireMetaDescription: true,
  keepIfPriorityPath: true,
  // priority paths regex (unchanged but kept as a RegExp literal)
  priorityPathRegex: /\/(blog|post|article|news|product|item|guide|docs)\b/i,
  // Don't touch the site if indexed within this many days
  daysNoCheck: 15
};

function nowMs() { return Date.now(); }
function daysToMs(d) { return d * 24 * 60 * 60 * 1000; }

if (!fs.existsSync(DOMAINS_FILE)) {
  console.error('domains.txt missing; add one domain per line (e.g. https://example.com)');
  process.exit(1);
}

const raw = fs.readFileSync(DOMAINS_FILE, 'utf8')
  .split(/\r?\n/)
  .map(s => s.trim()).filter(Boolean);

if (raw.length === 0) { console.error('domains.txt empty'); process.exit(1); }

const domains = raw.map(d => {
  try { const u = new URL(d); return { base: u.origin, host: u.host }; }
  catch (e) { const host = d.replace(/^https?:\/\//, '').replace(/\/+$/, ''); return { base: 'https://' + host, host }; }
});

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function domainAllowed(url) { try { const h = new URL(url).host; return domains.some(d => h === d.host || h.endsWith('.' + d.host)); } catch (e) { return false; } }

async function fetchWithTimeout(url, opts = {}, timeout = CONFIG.fetchTimeoutMs) {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), timeout);
  try { const res = await fetch(url, { ...opts, signal: controller.signal }); clearTimeout(id); return res; }
  catch (e) { clearTimeout(id); throw e; }
}

// sitemap helpers (handles .gz)
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

async function expandSitemap(sitemapUrl, seen) {
  const q = [sitemapUrl]; const pages = [];
  while (q.length) {
    const s = q.shift(); if (!s || seen.has(s)) continue; seen.add(s);
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
      // ignore sitemap fetch errors
    }
    await sleep(CONFIG.rateMs);
    if (pages.length >= CONFIG.maxPagesPerDomain) break;
  }
  return pages;
}

// fetch HTML page and extract title + meta description + body snippet
async function fetchHtml(url) {
  let attempt = 0;
  while (true) {
    try {
      const res = await fetchWithTimeout(url, { headers: { 'User-Agent': USER_AGENT } }, CONFIG.fetchTimeoutMs);
      if (!res.ok) throw new Error('HTTP ' + res.status);
      const ct = (res.headers.get('content-type') || '').toLowerCase();
      if (!ct.includes('html')) throw new Error('non-html ' + ct);
      const html = await res.text();
      const dom = new JSDOM(html);
      const doc = dom.window.document;
      const title = doc.querySelector('title')?.textContent?.trim() || '';
      // meta description trimmed to first 100 characters as requested
      const rawMeta = doc.querySelector('meta[name="description"]')?.getAttribute('content')?.trim() || '';
      const metaDesc = rawMeta.slice(0, 100);
      const ps = Array.from(doc.querySelectorAll('p')).map(p => p.textContent.trim()).filter(Boolean);
      const text = ps.join('\n').slice(0, CONFIG.maxTextChars);
      return { title, description: metaDesc, text };
    } catch (e) {
      attempt++;
      if (attempt > CONFIG.retries) return null;
      await sleep(CONFIG.retryBackoffMs * Math.pow(2, attempt - 1));
    }
  }
}

// load last-index timestamps and existing output (merge-friendly)
function loadJsonSafe(p) { try { return JSON.parse(fs.readFileSync(p, 'utf8')); } catch (e) { return null; } }
const lastIndex = loadJsonSafe(LAST_INDEX_FILE) || {};
const existingOut = loadJsonSafe(OUT) || [];

// helper to determine if we should skip re-checking a domain
function recentlyIndexed(host) {
  const t = lastIndex[host]; if (!t) return false; return (nowMs() - t) < daysToMs(CONFIG.daysNoCheck);
}

// MAIN
(async function main() {
  const seenPages = new Set(existingOut.map(x => x.url));
  const seenSitemaps = new Set();
  const out = []; // new results only for domains we process
  let total = 0;

  for (const d of domains) {
    // if domain was indexed recently, skip crawling it to honor the 15-day no-check rule
    if (recentlyIndexed(d.host)) {
      console.log('Skipping', d.base, '— indexed recently (within', CONFIG.daysNoCheck, 'days)');
      continue;
    }

    if (total >= CONFIG.maxTotalPages) break;
    console.log('Processing', d.base);
    const domainSitemaps = new Set();
    // robots.txt
    try {
      const robots = new URL('/robots.txt', d.base).toString();
      const r = await fetchWithTimeout(robots, { headers: { 'User-Agent': USER_AGENT } }, 6000);
      if (r && r.ok) {
        const txt = await r.text();
        for (const line of txt.split(/\r?\n/)) {
          if (line.toLowerCase().startsWith('sitemap:')) {
            const sm = line.split(':').slice(1).join(':').trim();
            try { domainSitemaps.add(new URL(sm, d.base).toString()); } catch (_) { domainSitemaps.add(sm); }
          }
        }
      }
    } catch (e) { /* ignore */ }
    domainSitemaps.add(new URL('/sitemap.xml', d.base).toString());

    // expand sitemaps
    let pageUrls = [];
    for (const sm of domainSitemaps) {
      try {
        const pages = await expandSitemap(sm, seenSitemaps);
        for (const p of pages) { if (!seenPages.has(p)) { pageUrls.push(p); seenPages.add(p); } if (pageUrls.length >= CONFIG.maxPagesPerDomain) break; }
      } catch (e) { /* ignore */ }
      if (pageUrls.length >= CONFIG.maxPagesPerDomain) break;
    }

    // ensure homepage first (still added to list but will only be kept if it matches priority regex)
    const homepage = new URL('/', d.base).toString();
    if (!pageUrls.includes(homepage)) pageUrls.unshift(homepage);

    let idx = 0;
    async function worker() {
      while (true) {
        const i = idx++;
        if (i >= pageUrls.length || total >= CONFIG.maxTotalPages) break;
        const url = pageUrls[i];
        await sleep(CONFIG.rateMs);
        const info = await fetchHtml(url).catch(() => null);
        if (!info) continue;
        const isPriority = CONFIG.keepIfPriorityPath && CONFIG.priorityPathRegex.test(new URL(url).pathname);
        // ONLY keep priority pages (per your request). This ignores title/meta checks so long as path matches priority.
        const keep = !!isPriority;
        if (keep) {
          out.push({ id: url, url, title: info.title, description: info.description, text: info.text });
          total++;
          console.log('[KEEP]', url, 'title-len=', (info.title || '').length, 'meta-len=', (info.description || '').length);
          if (out.filter(x => new URL(x.url).host === d.host).length >= CONFIG.maxPagesPerDomain) {
            console.log('Reached per-domain cap for', d.host);
            break;
          }
        } else {
          console.log('[SKIP]', url, `priority=${isPriority}`);
        }
      }
    }

    const tasks = [];
    for (let w = 0; w < CONFIG.concurrency; w++) tasks.push(worker());
    await Promise.all(tasks);

    // if we added pages for this domain, update lastIndex timestamp
    if (out.some(x => new URL(x.url).host === d.host)) {
      lastIndex[d.host] = nowMs();
    }
  }

  // merge: keep existing entries that weren't from domains we reindexed (or that we intentionally skipped)
  const reindexedHosts = new Set(out.map(x => new URL(x.url).host));
  const merged = [];
  const seen = new Set();

  // keep existing entries that are from hosts we did not reindex (or hosts that were recently skipped)
  for (const e of existingOut) {
    if (!e || !e.url) continue;
    const h = new URL(e.url).host;
    if (reindexedHosts.has(h)) continue; // we'll replace
    if (seen.has(e.url)) continue;
    seen.add(e.url);
    merged.push(e);
  }

  // add new results (dedup again)
  for (const r of out) {
    if (!r || !r.url) continue;
    if (seen.has(r.url)) continue;
    seen.add(r.url);
    merged.push(r);
  }

  // ensure output directory exists and write merged JSON
  if (!fs.existsSync(path.dirname(OUT))) fs.mkdirSync(path.dirname(OUT), { recursive: true });
  fs.writeFileSync(OUT, JSON.stringify(merged, null, 2), 'utf8');
  console.log('Wrote', OUT, '->', merged.length, 'pages total (merged).');

  // persist last-index file
  fs.writeFileSync(LAST_INDEX_FILE, JSON.stringify(lastIndex, null, 2), 'utf8');
  console.log('Updated', LAST_INDEX_FILE);
})();
