// crawler/merge_shards.js
// Usage:
// node crawler/merge_shards.js ./shard_artifacts site/index.json site/last_indexed.json
//
// This processes shard-output-*.json and shard-last-*.json one-by-one,
// writes site/index.json incrementally (so we don't keep the entire merged array in memory),
// and merges last-index records. It still keeps a `seen` Set of URLs for dedupe.

const fs = require('fs');
const path = require('path');

function loadJSON(p){ try { return JSON.parse(fs.readFileSync(p,'utf8')); } catch(e) { return null } }
function saveJSONAtomic(p, obj){
  const tmp = p + '.tmp.' + process.pid;
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(tmp, JSON.stringify(obj, null, 2), 'utf8');
  fs.renameSync(tmp, p);
}

const artifactsDir = process.argv[2] || './shard_artifacts';
const outIndex = path.join(__dirname, '..', 'site', 'index.json');
const outLast = path.join(__dirname, '..', 'site', 'last_indexed.json');
// const OUT = path.join(__dirname, '..', 'site', 'index.json');
// const LAST_INDEX_FILE =path.join(__dirname, '..', 'site', 'last_indexed.json')

const MAX_URLS_PER_HOST = 10000;

if (!fs.existsSync(artifactsDir)) {
  console.error('Artifacts dir not found:', artifactsDir);
  process.exit(1);
}

// Load existing last-index and seed
const existingLast = loadJSON(outLast) || {};
const lastByHost = { ...existingLast };

// Seed seen Set from existing index.json if present (optional but avoids duplicates)
const seen = new Set();
if (fs.existsSync(outIndex)) {
  try {
    const stat = fs.statSync(outIndex);
    if (stat.size > 200 * 1024 * 1024) {
      console.warn('[WARN] existing index.json >200MB. Seeding seen may use significant memory.');
    }
    const arr = loadJSON(outIndex) || [];
    if (Array.isArray(arr)) {
      for (const it of arr) if (it && it.url) seen.add(it.url);
      console.log('[INFO] seeded seen set from existing index.json ->', seen.size, 'urls');
    } else {
      console.warn('[WARN] existing index.json not an array; skipping seed.');
    }
  } catch (e) {
    console.warn('[WARN] failed to seed seen from existing index.json:', e && e.message ? e.message : e);
  }
}

// Prepare output stream for incremental JSON array
fs.mkdirSync(path.dirname(outIndex), { recursive: true });
const tmpOut = outIndex + '.tmp.' + process.pid;
const outStream = fs.createWriteStream(tmpOut, { encoding: 'utf8' });
outStream.write('[\n');

let firstWritten = false;
function writeJsonObject(obj) {
  if (firstWritten) outStream.write(',\n');
  outStream.write(JSON.stringify(obj));
  firstWritten = true;
}

// Process files
const files = fs.readdirSync(artifactsDir).sort();
for (const f of files) {
  const full = path.join(artifactsDir, f);
  try {
    if (f.startsWith('shard-output-') && f.endsWith('.json')) {
      const raw = fs.readFileSync(full, 'utf8').trim();
      if (!raw) continue;
      if (raw[0] === '[') {
        let arr;
        try {
          arr = JSON.parse(raw);
        } catch (e) {
          console.warn('[WARN] failed to parse', f, 'as JSON array - skipping:', e && e.message ? e.message : e);
          continue;
        }
        for (const it of arr) {
          if (!it || !it.url) continue;
          if (seen.has(it.url)) continue;
          seen.add(it.url);
          writeJsonObject(it);
        }
      } else {
        // Fallback: file might be NDJSON (line JSONs)
        const lines = raw.split(/\r?\n/);
        for (const line of lines) {
          if (!line) continue;
          try {
            const it = JSON.parse(line);
            if (!it || !it.url) continue;
            if (seen.has(it.url)) continue;
            seen.add(it.url);
            writeJsonObject(it);
          } catch (e) {
            continue;
          }
        }
      }
      console.log('[MERGED] shard-output', f);
    }

    if (f.startsWith('shard-last-') && f.endsWith('.json')) {
      const recs = loadJSON(full) || {};
      for (const host of Object.keys(recs)) {
        const r = recs[host] || { lastAt: 0, urls: [] };
        const existing = lastByHost[host] || { lastAt: 0, urls: [] };
        const mergedAt = Math.max(existing.lastAt || 0, r.lastAt || 0);
        const urls = Array.from(new Set([...(existing.urls||[]), ...(r.urls||[])]));
        if (urls.length > MAX_URLS_PER_HOST) urls.splice(MAX_URLS_PER_HOST);
        lastByHost[host] = { lastAt: mergedAt, urls };
      }
      console.log('[MERGED] shard-last', f);
    }
  } catch (e) {
    console.error('[ERR] processing artifact', f, e && e.stack ? e.stack : e);
    // continue processing remaining files
  }
}

// finish JSON array
outStream.write('\n]\n');
outStream.end();

try {
  fs.renameSync(tmpOut, outIndex);
  console.log('[OK] wrote', outIndex);
} catch (e) {
  console.error('[ERR] failed to rename tmp index file', e && e.stack ? e.stack : e);
  try { fs.unlinkSync(tmpOut); } catch (e2) {}
  process.exit(1);
}

// write last_indexed
saveJSONAtomic(outLast, lastByHost);

console.log('Merged complete. total pages in index (approx):', seen.size);
console.log('Hosts tracked:', Object.keys(lastByHost).length);
