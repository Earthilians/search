// crawler/merge_shards.js
// Usage: node crawler/merge_shards.js <artifactsDir> <outIndex> <outLast>
// Example: node crawler/merge_shards.js shard_artifacts site/index.json site/last_indexed.json

const fs = require('fs');
const path = require('path');

function loadJSON(p){ try { return JSON.parse(fs.readFileSync(p,'utf8')); } catch(e) { return null } }
function writeAtomicFile(p, dataStr) {
  const dir = path.dirname(p);
  fs.mkdirSync(dir, { recursive: true });
  const tmp = path.join(dir, path.basename(p) + '.tmp.' + process.pid + '.' + Date.now());
  fs.writeFileSync(tmp, dataStr, 'utf8');
  try { fs.renameSync(tmp, p); }
  catch(e) {
    // fallback copy then remove tmp
    try { fs.copyFileSync(tmp, p); fs.unlinkSync(tmp); } catch(err){ throw err; }
  }
}

const artifactsDir = process.argv[2] || './shard_artifacts';
const outIndex = process.argv[3] || './site/index.json';
const outLast = process.argv[4] || './site/last_indexed.json';
const MAX_URLS_PER_HOST = 10000;

if (!fs.existsSync(artifactsDir)) {
  console.error('[FATAL] artifacts dir not found:', artifactsDir);
  process.exit(1);
}

console.log('[INFO] artifactsDir=', artifactsDir, 'outIndex=', outIndex, 'outLast=', outLast);

const existingIndex = loadJSON(outIndex) || [];
const existingLast = loadJSON(outLast) || {};

const seen = new Set();
if (Array.isArray(existingIndex)) {
  for (const it of existingIndex) if (it && it.url) seen.add(it.url);
  console.log('[INFO] seeded seen from existing index ->', seen.size);
} else {
  console.log('[WARN] existing index.json is not an array or missing');
}

const files = fs.readdirSync(artifactsDir).sort();
let mergedCount = 0;
const lastByHost = { ...existingLast };

// prepare output stream (write array incrementally)
const outDir = path.dirname(outIndex);
fs.mkdirSync(outDir, { recursive: true });
const tmpOut = path.join(outDir, path.basename(outIndex) + '.tmp.' + process.pid + '.' + Date.now());
const outStream = fs.createWriteStream(tmpOut, { encoding: 'utf8' });
outStream.write('[\n');
let first = false;

function pushObj(obj){
  if (!obj || !obj.url) return;
  if (seen.has(obj.url)) return;
  if (first) outStream.write(',\n');
  outStream.write(JSON.stringify(obj));
  first = true;
  seen.add(obj.url);
  mergedCount++;
}

for (const f of files) {
  const full = path.join(artifactsDir, f);
  try {
    if ((f.startsWith('shard-output-') && (f.endsWith('.json') || f.endsWith('.ndjson')))) {
      const raw = fs.readFileSync(full, 'utf8').trim();
      if (!raw) { console.log('[SKIP EMPTY]', f); continue; }

      if (f.endsWith('.json')) {
        // could be an array or NDJSON with .json extension
        if (raw[0] === '[') {
          let arr;
          try { arr = JSON.parse(raw); } catch(e) { console.warn('[WARN] failed to parse array', f, e && e.message); continue; }
          if (!Array.isArray(arr)) { console.warn('[WARN] not an array', f); continue; }
          for (const it of arr) pushObj(it);
        } else {
          // treat as NDJSON lines
          const lines = raw.split(/\r?\n/);
          for (const line of lines) {
            if (!line) continue;
            try { const it = JSON.parse(line); pushObj(it); } catch(e) { /* skip bad line */ }
          }
        }
      } else if (f.endsWith('.ndjson')) {
        const lines = raw.split(/\r?\n/);
        for (const line of lines) {
          if (!line) continue;
          try { const it = JSON.parse(line); pushObj(it); } catch(e) { /* skip bad line */ }
        }
      }
      console.log('[MERGED] output artifact', f);
      continue;
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
      console.log('[MERGED] last artifact', f);
      continue;
    }
  } catch (e) {
    console.error('[ERR] processing', f, e && e.stack ? e.stack : e);
    continue; // keep going with other artifacts
  }
}

// finish JSON array
outStream.write('\n]\n');
outStream.end();

// wait for stream finish
outStream.on('finish', () => {
  try {
    // move tmpOut -> outIndex
    writeAtomicFile(outIndex, fs.readFileSync(tmpOut, 'utf8'));
    try { fs.unlinkSync(tmpOut); } catch(_) {}
    console.log('[OK] wrote', outIndex, 'mergedCount=', mergedCount);
  } catch (e) {
    console.error('[FATAL] could not finalize index.json', e && e.stack ? e.stack : e);
    process.exit(1);
  }

  // write last_indexed
  try {
    writeAtomicFile(outLast, JSON.stringify(lastByHost, null, 2));
    console.log('[OK] wrote', outLast, 'hosts=', Object.keys(lastByHost).length);
  } catch (e) {
    console.error('[ERR] could not write last_indexed.json', e && e.stack ? e.stack : e);
    process.exit(1);
  }

  console.log('Merge complete. total pages in index (approx):', seen.size);
});

outStream.on('error', (err) => {
  console.error('[FATAL] outStream error', err && err.stack ? err.stack : err);
  process.exit(1);
});
