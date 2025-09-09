const fs = require('fs');
const fetch = require('node-fetch');
const { JSDOM } = require('jsdom');

const DOMAINS_FILE = './domains.txt';
const OUT = '../site/index.json';
const USER_AGENT = 'EarthiliansCrawler/1.0 (+mailto:you@example.com)';

async function fetchText(url) {
  try {
    const res = await fetch(url, { headers: { 'User-Agent': USER_AGENT }, timeout: 15000 });
    if (!res.ok) return null;
    const html = await res.text();
    const dom = new JSDOM(html);
    const title = dom.window.document.querySelector('title')?.textContent || '';
    const ps = Array.from(dom.window.document.querySelectorAll('p')).map(p => p.textContent.trim());
    const text = ps.join('\n').slice(0, 200000);
    return { title, text };
  } catch (e) {
    console.error('fetch error', url, e.message);
    return null;
  }
}

async function parseSitemap(surl) {
  try {
    const res = await fetch(surl, { headers: { 'User-Agent': USER_AGENT }, timeout: 15000 });
    if (!res.ok) return [];
    const xml = await res.text();
    return [...xml.matchAll(/<loc>([^<]+)<\/loc>/g)].map(m => m[1]);
  } catch (e) {
    return [];
  }
}

(async function main() {
  if (!fs.existsSync(DOMAINS_FILE)) {
    console.error('domains.txt missing');
    process.exit(1);
  }
  const domains = fs.readFileSync(DOMAINS_FILE, 'utf8').split(/\r?\n/).map(s=>s.trim()).filter(Boolean);
  const docs = [];
  for (const d of domains) {
    try {
      // robots.txt sitemap hints
      const robotsUrl = d.replace(/\/+$/, '') + '/robots.txt';
      try {
        const r = await fetch(robotsUrl, { headers: { 'User-Agent': USER_AGENT } });
        if (r.ok) {
          const txt = await r.text();
          const sLines = txt.split(/\r?\n/).filter(l => l.toLowerCase().startsWith('sitemap:'));
          for (const l of sLines) {
            const sm = l.split(':').slice(1).join(':').trim();
            const urls = await parseSitemap(sm);
            for (const u of urls) {
              const info = await fetchText(u);
              if (info) docs.push({ id: u, url: u, title: info.title, text: info.text });
              await new Promise(r=>setTimeout(r, 800));
            }
          }
        }
      } catch {}
      // fallback to /sitemap.xml
      const fallback = d.replace(/\/+$/, '') + '/sitemap.xml';
      const urls = await parseSitemap(fallback);
      for (const u of urls) {
        const info = await fetchText(u);
        if (info) docs.push({ id: u, url: u, title: info.title, text: info.text });
        await new Promise(r=>setTimeout(r, 800));
      }
    } catch (e) {
      console.error('domain failed', d, e && e.message);
    }
  }
  if (!fs.existsSync('../site')) fs.mkdirSync('../site');
  fs.writeFileSync(OUT, JSON.stringify(docs, null, 2));
  console.log('Wrote', OUT, 'docs:', docs.length);
})();
