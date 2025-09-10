// const fs = require('fs');
// const fetch = require('node-fetch');
// const { JSDOM } = require('jsdom');

// const DOMAINS_FILE = './domains.txt';
// const OUT = '../site/index.json';
// const USER_AGENT = 'EarthiliansCrawler/1.0 (+mailto:you@example.com)';

// async function fetchText(url) {
//   try {
//     const res = await fetch(url, { headers: { 'User-Agent': USER_AGENT }, timeout: 15000 });
//     if (!res.ok) return null;
//     const html = await res.text();
//     const dom = new JSDOM(html);
//     const title = dom.window.document.querySelector('title')?.textContent || '';
//     const ps = Array.from(dom.window.document.querySelectorAll('p')).map(p => p.textContent.trim());
//     const text = ps.join('\n').slice(0, 200000);
//     return { title, text };
//   } catch (e) {
//     console.error('fetch error', url, e.message);
//     return null;
//   }
// }

// async function parseSitemap(surl) {
//   try {
//     const res = await fetch(surl, { headers: { 'User-Agent': USER_AGENT }, timeout: 15000 });
//     if (!res.ok) return [];
//     const xml = await res.text();
//     return [...xml.matchAll(/<loc>([^<]+)<\/loc>/g)].map(m => m[1]);
//   } catch (e) {
//     return [];
//   }
// }

// (async function main() {
//   if (!fs.existsSync(DOMAINS_FILE)) {
//     console.error('domains.txt missing');
//     process.exit(1);
//   }
//   const domains = fs.readFileSync(DOMAINS_FILE, 'utf8').split(/\r?\n/).map(s=>s.trim()).filter(Boolean);
//   const docs = [];
//   for (const d of domains) {
//     try {
//       // robots.txt sitemap hints
//       const robotsUrl = d.replace(/\/+$/, '') + '/robots.txt';
//       try {
//         const r = await fetch(robotsUrl, { headers: { 'User-Agent': USER_AGENT } });
//         if (r.ok) {
//           const txt = await r.text();
//           const sLines = txt.split(/\r?\n/).filter(l => l.toLowerCase().startsWith('sitemap:'));
//           for (const l of sLines) {
//             const sm = l.split(':').slice(1).join(':').trim();
//             const urls = await parseSitemap(sm);
//             for (const u of urls) {
//               const info = await fetchText(u);
//               if (info) docs.push({ id: u, url: u, title: info.title, text: info.text });
//               await new Promise(r=>setTimeout(r, 800));
//             }
//           }
//         }
//       } catch {}
//       // fallback to /sitemap.xml
//       const fallback = d.replace(/\/+$/, '') + '/sitemap.xml';
//       const urls = await parseSitemap(fallback);
//       for (const u of urls) {
//         const info = await fetchText(u);
//         if (info) docs.push({ id: u, url: u, title: info.title, text: info.text });
//         await new Promise(r=>setTimeout(r, 800));
//       }
//     } catch (e) {
//       console.error('domain failed', d, e && e.message);
//     }
//   }
//   if (!fs.existsSync('../site')) fs.mkdirSync('../site');
//   fs.writeFileSync(OUT, JSON.stringify(docs, null, 2));
//   console.log('Wrote', OUT, 'docs:', docs.length);
// })();


/**
 * crawl.js
 *
 * Usage:
 *   node crawl.js
 *
 * Requirements:
 *   npm i node-fetch@3 jsdom
 *
 * Notes:
 * - Uses AbortController for fetch timeouts (node >=16).
 * - Adjust CONFIG at top for concurrency, rate limit, max pages, etc.
 */

/*
 Improved crawler: index home pages and HTML pages — skip sitemap XML files.
 Save as crawl_index.js and run with: node crawl_index.js
 Requires: node >= 16, npm i node-fetch@3 jsdom fast-xml-parser

 Features:
 - Reads domains from domains.txt (one per line, can be https://example.com or example.com)
 - Finds sitemap URLs from robots.txt and /sitemap.xml, expands sitemapindex recursively
 - Ignores sitemap XML files as pages (they're expanded, not indexed)
 - Ensures homepage (/) is indexed for each domain
 - Filters page URLs to only allowed domains (including subdomains)
 - Handles .xml.gz sitemaps (decompresses)
 - Respects Content-Type, only indexes HTML pages
 - Extracts title and meta description + first N chars of body paragraphs
 - Concurrency, retries, rate-limiting, caps
 - Writes ./site/index.json
*/

const fs = require('fs');
const path = require('path');
const fetch = require('node-fetch');
const { JSDOM } = require('jsdom');
const zlib = require('zlib');
const { XMLParser } = require('fast-xml-parser');
const { URL } = require('url');

const DOMAINS_FILE = './domains.txt';
const OUT = './site/index.json';
const USER_AGENT = 'EarthiliansCrawler/1.0 (+mailto:you@example.com)';

const CONFIG = {
  concurrency: 5,
  rateMs: 250,
  fetchTimeoutMs: 15000,
  maxPagesPerDomain: 500,
  maxTotalPages: 5000,
  maxTextChars: 200000,
  retries: 2,
  retryBackoffMs: 800,
};

if (!fs.existsSync(DOMAINS_FILE)) {
  console.error('domains.txt missing');
  process.exit(1);
}

const rawDomains = fs.readFileSync(DOMAINS_FILE, 'utf8')
  .split(/\r?\n/).map(s => s.trim()).filter(Boolean);

// Normalize domains to base URL and host
const domains = rawDomains.map(d => {
  try { const u = new URL(d); return { base: u.origin, host: u.host }; }
  catch (e) { // treat as host
    const host = d.replace(/^https?:\/\//, '').replace(/\/+$/, '');
    return { base: 'https://' + host, host };
  }
});

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function domainAllowed(url) {
  try {
    const host = new URL(url).host;
    return domains.some(d => host === d.host || host.endsWith('.' + d.host));
  } catch (e) { return false; }
}

async function fetchWithTimeout(url, opts = {}, timeout = CONFIG.fetchTimeoutMs) {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), timeout);
  try {
    const res = await fetch(url, { ...opts, signal: controller.signal });
    clearTimeout(id);
    return res;
  } catch (err) {
    clearTimeout(id);
    throw err;
  }
}

// Fetch sitemap text, handle .gz
async function fetchSitemapText(url) {
  const res = await fetchWithTimeout(url, { headers: { 'User-Agent': USER_AGENT } }, CONFIG.fetchTimeoutMs);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const buf = Buffer.from(await res.arrayBuffer());
  const contentType = (res.headers.get('content-type') || '').toLowerCase();
  if (url.toLowerCase().endsWith('.gz') || contentType.includes('gzip')) {
    try { return zlib.gunzipSync(buf).toString('utf8'); } catch (e) { return buf.toString('utf8'); }
  }
  return buf.toString('utf8');
}

// Parse sitemap XML (both sitemapindex and urlset) and return list of locs
function parseSitemapXmlText(xmlText) {
  try {
    const parser = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: '@_' });
    const obj = parser.parse(xmlText);
    const locs = [];
    // sitemapindex.sitemap[].loc or urlset.url[].loc
    if (obj.sitemapindex && obj.sitemapindex.sitemap) {
      const s = obj.sitemapindex.sitemap;
      const items = Array.isArray(s) ? s : [s];
      for (const it of items) if (it.loc) locs.push(String(it.loc));
    }
    if (obj.urlset && obj.urlset.url) {
      const u = obj.urlset.url;
      const items = Array.isArray(u) ? u : [u];
      for (const it of items) if (it.loc) locs.push(String(it.loc));
    }
    // fallback regex extraction for robustness
    if (locs.length === 0) {
      for (const m of xmlText.matchAll(/<loc>([^<]+)<\/loc>/gi)) locs.push(m[1].trim());
    }
    return locs;
  } catch (e) {
    // fallback regex
    const locs = [];
    for (const m of xmlText.matchAll(/<loc>([^<]+)<\/loc>/gi)) locs.push(m[1].trim());
    return locs;
  }
}

async function expandSitemapRoot(sitemapUrl, seenSitemaps) {
  const queue = [sitemapUrl];
  const pages = [];
  while (queue.length) {
    const s = queue.shift();
    if (!s || seenSitemaps.has(s)) continue;
    seenSitemaps.add(s);
    try {
      const xml = await fetchSitemapText(s);
      const locs = parseSitemapXmlText(xml);
      for (const loc of locs) {
        // normalize
        let norm;
        try { norm = new URL(loc).toString(); } catch (e) { try { norm = new URL(loc, s).toString(); } catch (e2) { continue; } }
        if (norm.toLowerCase().endsWith('.xml') || norm.toLowerCase().endsWith('.xml.gz')) {
          queue.push(norm);
        } else {
          // only keep page URLs that belong to allowed domains
          if (domainAllowed(norm)) pages.push(norm);
        }
      }
    } catch (e) {
      // skip sitemap if unreachable
      // console.warn('sitemap fetch failed', s, e.message || e);
    }
    await sleep(CONFIG.rateMs);
    if (pages.length >= CONFIG.maxPagesPerDomain) break;
  }
  return pages;
}

async function fetchHtmlPage(url) {
  let attempt = 0;
  while (true) {
    try {
      const res = await fetchWithTimeout(url, { headers: { 'User-Agent': USER_AGENT } }, CONFIG.fetchTimeoutMs);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const ct = (res.headers.get('content-type') || '').toLowerCase();
      if (!ct.includes('html')) throw new Error(`non-html content-type=${ct}`);
      const html = await res.text();
      const dom = new JSDOM(html);
      const doc = dom.window.document;
      const title = doc.querySelector('title')?.textContent?.trim() || '';
      const metaDesc = doc.querySelector('meta[name="description"]')?.getAttribute('content') || '';
      const ps = Array.from(doc.querySelectorAll('p')).map(p=>p.textContent.trim()).filter(Boolean);
      const bodySnippet = ps.join('\n').slice(0, CONFIG.maxTextChars);
      return { title, description: metaDesc, text: bodySnippet };
    } catch (err) {
      attempt++;
      if (attempt > CONFIG.retries) throw err;
      const backoff = CONFIG.retryBackoffMs * Math.pow(2, attempt-1);
      await sleep(backoff);
    }
  }
}

// Main
(async function main() {
  const seenPages = new Set();
  const seenSitemaps = new Set();
  const out = [];
  let total = 0;

  console.log('Crawl start. Domains:', domains.map(d=>d.base));

  for (const d of domains) {
    if (total >= CONFIG.maxTotalPages) break;
    console.log('\nProcessing domain:', d.base);
    const domainSitemaps = new Set();

    // robots.txt
    try {
      const robotsUrl = new URL('/robots.txt', d.base).toString();
      const r = await fetchWithTimeout(robotsUrl, { headers: { 'User-Agent': USER_AGENT } }, 5000);
      if (r && r.ok) {
        const txt = await r.text();
        for (const line of txt.split(/\r?\n/)) {
          if (line.toLowerCase().startsWith('sitemap:')) {
            const sm = line.split(':').slice(1).join(':').trim();
            try { domainSitemaps.add(new URL(sm, d.base).toString()); } catch (_) { domainSitemaps.add(sm); }
          }
        }
      }
    } catch (e) { /* ignore robots failures */ }

    // fallback
    domainSitemaps.add(new URL('/sitemap.xml', d.base).toString());

    // Expand sitemaps
    const pageUrls = [];
    for (const sm of domainSitemaps) {
      const pages = await expandSitemapRoot(sm, seenSitemaps);
      for (const p of pages) {
        if (!seenPages.has(p)) { pageUrls.push(p); seenPages.add(p); }
        if (pageUrls.length >= CONFIG.maxPagesPerDomain) break;
      }
      if (pageUrls.length >= CONFIG.maxPagesPerDomain) break;
    }

    // Ensure homepage is included
    const homepage = new URL('/', d.base).toString();
    if (!seenPages.has(homepage)) {
      pageUrls.unshift(homepage);
      seenPages.add(homepage);
    }

    console.log(`Domain ${d.base}: discovered ${pageUrls.length} candidate page urls`);

    // Crawl pages with concurrency
    const tasks = [];
    let idx = 0;
    async function worker() {
      while (true) {
        const i = idx++;
        if (i >= pageUrls.length) break;
        const url = pageUrls[i];
        try {
          await sleep(CONFIG.rateMs);
          const info = await fetchHtmlPage(url);
          out.push({ id: url, url, title: info.title, description: info.description, text: info.text });
          total++;
          if (total % 10 === 0) console.log('Indexed', total, 'pages so far');
          if (total >= CONFIG.maxTotalPages) break;
        } catch (e) {
          // skip failures
          // console.warn('page fetch failed', url, e.message || e);
        }
      }
    }

    // start workers
    for (let w=0; w<CONFIG.concurrency; w++) tasks.push(worker());
    await Promise.all(tasks);

    if (total >= CONFIG.maxTotalPages) break;
  }

  // write output
  const dir = path.dirname(OUT);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(OUT, JSON.stringify(out, null, 2), 'utf8');
  console.log('\nWrote', OUT, '->', out.length, 'records');
})();
