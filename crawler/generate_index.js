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

const fs = require('fs');
const fetch = require('node-fetch');
const { JSDOM } = require('jsdom');
const { URL } = require('url');

const DOMAINS_FILE = './domains.txt';
const OUT = './site/index.json';
const USER_AGENT = 'EarthiliansCrawler/1.0 (+mailto:you@example.com)';

// ============ Config ============
const CONFIG = {
  concurrency: 5,        // number of simultaneous fetchText workers
  rateMs: 250,           // delay between requests per worker (politeness)
  fetchTimeoutMs: 15000, // per-request timeout
  maxPagesPerDomain: 500,// safety cap per domain (set null for unlimited)
  maxTotalPages: 5000,   // safety cap overall
  maxTextChars: 200000,  // max chars to store in "text"
  retries: 2,            // retry fetch on transient errors
  retryBackoffMs: 800,   // initial backoff
};
// ==============================

if (!fs.existsSync(DOMAINS_FILE)) {
  console.error('domains.txt missing');
  process.exit(1);
}

const domains = fs.readFileSync(DOMAINS_FILE, 'utf8')
  .split(/\r?\n/).map(s => s.trim()).filter(Boolean);

// normalize domains to host-only (no protocol, no trailing slash)
const domainHosts = domains.map(d => {
  try { return (new URL(d)).host; } catch (e) { return d.replace(/^https?:\/\//, '').replace(/\/+$/, ''); }
});

function normalizeUrl(u, base) {
  try {
    return new URL(u, base).toString();
  } catch (e) {
    return null;
  }
}

function domainOf(url) {
  try { return new URL(url).host; } catch (e) { return null; }
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

async function safeFetchText(url) {
  // retries + backoff
  let attempt = 0;
  while (true) {
    try {
      const res = await fetchWithTimeout(url, { headers: { 'User-Agent': USER_AGENT } }, CONFIG.fetchTimeoutMs);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const html = await res.text();
      const dom = new JSDOM(html);
      const doc = dom.window.document;
      const title = doc.querySelector('title')?.textContent?.trim() || '';
      const ps = Array.from(doc.querySelectorAll('p')).map(p => p.textContent.trim()).filter(Boolean);
      const bodyText = ps.join('\n');
      const text = bodyText.slice(0, CONFIG.maxTextChars);
      return { title, text };
    } catch (err) {
      attempt++;
      if (attempt > CONFIG.retries) {
        // final fail
        throw err;
      }
      const backoff = CONFIG.retryBackoffMs * Math.pow(2, attempt - 1);
      await new Promise(r => setTimeout(r, backoff));
    }
  }
}

async function parseSitemapXml(sitemapUrl, parentUrl = null) {
  try {
    const res = await fetchWithTimeout(sitemapUrl, { headers: { 'User-Agent': USER_AGENT } }, CONFIG.fetchTimeoutMs);
    if (!res.ok) return [];
    const xml = await res.text();

    // Find all <loc> values (works for sitemap and sitemapindex)
    const locs = [...xml.matchAll(/<loc>([^<]+)<\/loc>/gi)].map(m => m[1].trim());

    // If sitemap contains other sitemaps (sitemapindex), detect and return them to be expanded by caller.
    // We'll return list of locs and let caller decide whether they're page URLs or sitemap URLs.
    return locs;
  } catch (e) {
    // network or parse error
    return [];
  }
}

function isAllowedDomain(url) {
  const host = domainOf(url);
  if (!host) return false;
  return domainHosts.some(d => host === d || host.endsWith('.' + d));
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// Simple async queue to limit concurrency
function createQueue(worker, concurrency = 4) {
  const queue = [];
  let active = 0;
  let closed = false;

  async function runOne(task) {
    active++;
    try {
      await worker(task);
    } catch (e) {
      // worker handles its own errors typically; swallow to keep queue running
      console.error('worker error', e && e.message || e);
    } finally {
      active--;
      processNext();
    }
  }

  function processNext() {
    if (closed) return;
    while (active < concurrency && queue.length) {
      const t = queue.shift();
      runOne(t);
    }
  }

  return {
    push(task) {
      queue.push(task);
      processNext();
    },
    close() {
      closed = true;
    },
    idlePromise() {
      return new Promise(res => {
        const check = () => {
          if (queue.length === 0 && active === 0) return res();
          setTimeout(check, 200);
        };
        check();
      });
    }
  };
}

(async function main() {
  const seen = new Set();
  const docs = [];
  let totalCount = 0;

  console.log('Starting crawl for domains:', domainHosts);

  // For each domain, gather sitemaps (from robots.txt sitemap: entries and fallback /sitemap.xml)
  for (const domain of domains) {
    const base = domain.replace(/\/+$/, '');
    const robotsUrl = normalizeUrl('/robots.txt', base) || (base + '/robots.txt');

    try {
      const res = await fetchWithTimeout(robotsUrl, { headers: { 'User-Agent': USER_AGENT } }, 5000);
      if (res && res.ok) {
        const txt = await res.text();
        const sitemapLines = txt.split(/\r?\n/).filter(l => l.toLowerCase().startsWith('sitemap:'));
        for (const l of sitemapLines) {
          const sm = l.split(':').slice(1).join(':').trim();
          const smUrl = normalizeUrl(sm, base);
          if (smUrl) {
            console.log('Found sitemap in robots.txt ->', smUrl);
            // expand sitemap(s) later
            // push to a list below by adding to an array
            // we'll handle expansion per-domain below
            // For simplicity, we collect them now into domainSitemaps
          }
        }
      }
    } catch (e) {
      // ignore robots failures; fallback to /sitemap.xml below
    }
  }

  // We'll process each domain separately: fetch robots sitemaps and fallback sitemap.xml
  for (const d of domains) {
    const base = d.replace(/\/+$/, '');
    const domainSitemaps = new Set();

    // robots.txt sitemap entries
    const robotsUrl = normalizeUrl('/robots.txt', base) || (base + '/robots.txt');
    try {
      const r = await fetchWithTimeout(robotsUrl, { headers: { 'User-Agent': USER_AGENT } }, 5000);
      if (r && r.ok) {
        const txt = await r.text();
        const sitemapLines = txt.split(/\r?\n/).filter(l => l.toLowerCase().startsWith('sitemap:'));
        for (const l of sitemapLines) {
          const sm = l.split(':').slice(1).join(':').trim();
          const smUrl = normalizeUrl(sm, base);
          if (smUrl) domainSitemaps.add(smUrl);
        }
      }
    } catch (e) { /* ignore */ }

    // fallback to /sitemap.xml
    const fallback = normalizeUrl('/sitemap.xml', base) || (base + '/sitemap.xml');
    domainSitemaps.add(fallback);

    // Expand sitemap(s) recursively, but only keep page URLs that belong to allowed domains
    const pageUrls = [];
    const toExpand = [...domainSitemaps];

    while (toExpand.length) {
      const sm = toExpand.shift();
      if (!sm) continue;
      // avoid expanding same sitemap twice
      if (seen.has(`__sitemap:${sm}`)) continue;
      seen.add(`__sitemap:${sm}`);

      const locs = await parseSitemapXml(sm);
      for (const loc of locs) {
        const norm = normalizeUrl(loc, sm);
        if (!norm) continue;
        // If the loc is another xml (sitemap), expand it
        if (norm.endsWith('.xml')) {
          toExpand.push(norm);
          continue;
        }
        // Otherwise treat as page URL
        if (isAllowedDomain(norm)) {
          pageUrls.push(norm);
        } else {
          // skip external domains
        }
      }
      // small delay to avoid hammering
      await sleep(CONFIG.rateMs);
    }

    console.log(`Domain ${d}: discovered ${pageUrls.length} page urls (before dedupe/cap).`);

    // Deduplicate pageUrls per domain
    const domainSeen = new Set();
    const domainFiltered = [];
    for (const u of pageUrls) {
      if (domainSeen.has(u)) continue;
      domainSeen.add(u);
      domainFiltered.push(u);
      if (CONFIG.maxPagesPerDomain && domainFiltered.length >= CONFIG.maxPagesPerDomain) break;
    }

    // Push domainFiltered into main crawl queue below
    for (const u of domainFiltered) {
      // reuse seen set for final URL dedupe as well
      if (seen.has(u)) continue;
      seen.add(u);
      // push into a list for fetching
      // We'll accumulate tasks in an array for the queue worker below
      docs.push({ __url: u }); // placeholder; we'll convert to full doc after fetching
      totalCount++;
      if (CONFIG.maxTotalPages && totalCount >= CONFIG.maxTotalPages) break;
    }
    if (CONFIG.maxTotalPages && totalCount >= CONFIG.maxTotalPages) break;
  }

  console.log('Total tasks queued for fetching:', docs.length);

  // Worker to fetch page content, update docs array in-place
  let completed = 0;
  const worker = async (task) => {
    const url = task.__url;
    try {
      // polite per-worker rate limit
      await sleep(CONFIG.rateMs);
      const info = await safeFetchText(url);
      task.id = url;
      task.url = url;
      task.title = info.title;
      task.text = info.text;
    } catch (e) {
      // mark failed tasks with null title/text (they'll be filtered out before writing)
      console.error('fetch failed', url, e && e.message || e);
      task.id = null;
    } finally {
      completed++;
      if (completed % 20 === 0) {
        console.log(`Progress: ${completed}/${docs.length}`);
      }
    }
  };

  // Create queue and push tasks
  const q = createQueue(worker, CONFIG.concurrency);
  for (const t of docs) q.push(t);

  // Wait until queue is drained
  await q.idlePromise();

  // Filter out failed placeholders and convert to final array
  const final = docs.filter(d => d && d.id).map(d => ({ id: d.id, url: d.url, title: d.title, text: d.text }));

  // Ensure output dir exists
  const outdir = require('path').dirname(OUT);
  if (!fs.existsSync(outdir)) fs.mkdirSync(outdir, { recursive: true });

  fs.writeFileSync(OUT, JSON.stringify(final, null, 2), 'utf8');
  console.log('Wrote', OUT, '->', final.length, 'records');
})();
