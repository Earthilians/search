// crawler/merge_shards.js
// Usage:
//   node crawler/merge_shards.js ./shard_artifacts site/index.json site/last_indexed.json
// This merges shard-output-*.json and shard-last-*.json into site/index.json and site/last_indexed.json

const fs = require('fs');
const path = require('path');

function loadJSON(p){
  try { return JSON.parse(fs.readFileSync(p,'utf8')); }
  catch(e){ return null; }
}

function saveJSONAtomic(p, obj){
  const dir = path.dirname(p);
  fs.mkdirSync(dir, { recursive: true });

  // tmp file in same dir to ensure rename is atomic
  const base = path.basename(p);
  const tmp = path.join(dir, base + '.tmp.' + process.pid + '.' + Date.now());
  try {
    fs.writeFileSync(tmp, JSON.stringify(obj, null, 2), 'utf8');
  } catch (wErr) {
    console.error('[ERR] failed to write tmp file', tmp, wErr && (wErr.stack || wErr.message) ? (wErr.stack || wErr.message) : wErr);
    // fallback: try direct write
    try {
      fs.writeFileSync(p, JSON.stringify(obj, null, 2), 'utf8');
      console.warn('[WARN] wrote final file directly after tmp write error:', p);
      try { if (fs.existsSync(tmp)) fs.unlinkSync(tmp); } catch(_) {}
      return;
    } catch (finalErr) {
      console.error('[FATAL] could not write final file either', p, finalErr && (finalErr.stack || finalErr.message) ? (finalErr.stack || finalErr.message) : finalErr);
      throw finalErr;
    }
  }

  if (!fs.existsSync(tmp)) {
    console.error('[ERR] tmp file missing before rename:', tmp);
    try {
      fs.writeFileSync(p, JSON.stringify(obj, null, 2), 'utf8');
      console.warn('[WARN] tmp missing -> wrote final file directly:', p);
      return;
    } catch (finalErr) {
      console.error('[FATAL] could not write final file when tmp missing', finalErr && (finalErr.stack || finalErr.message) ? (finalErr.stack || finalErr.message) : finalErr);
      throw finalErr;
    }
  }

  try {
    fs.renameSync(tmp, p);
    return;
  } catch (renameErr) {
    console.error('[ERR] failed to rename tmp file', tmp, '->', p, renameErr && (renameErr.stack || renameErr.message) ? (renameErr.stack || renameErr.message) : renameErr);
    // try copy fallback
    try {
      fs.copyFileSync(tmp, p);
      try { fs.unlinkSync(tmp); } catch(_) {}
      console.warn('[WARN] fallback: copied tmp -> final and removed tmp');
      return;
    } catch (copyErr) {
      console.error('[ERR] fallback copy failed', copyErr && (copyErr.stack || copyErr.message) ? (copyErr.stack || copyErr.message) : copyErr);
      try {
        fs.writeFileSync(p, JSON.stringify(obj, null, 2), 'utf8');
        console.warn('[WARN] final fallback: wrote final file directly after rename+copy failed');
        try { if (fs.existsSync(tmp)) fs.unlinkSync(tmp); } catch(_) {}
        return;
      } catch (finalErr) {
        console.error('[FATAL] last-resort write failed', finalErr && (finalErr.stack || finalErr.message) ? (finalErr.stack || finalErr.message) : finalErr);
        try { if (fs.existsSync(tmp)) fs.unlinkSync(tmp); } catch(_) {}
        throw finalErr;
      }
    }
  }
}

const artifactsDir = process.argv[2] || './shard_artifacts';
const outIndex = process.argv[3] || './site/index.json';
const outLast = process.argv[4] || './site/last_indexed.json';
const MAX_URLS_PER_HOST = 10000;

if (!fs.existsSync(artifactsDir)) {
  console.error('Artifacts dir not found:', artifactsDir);
  process.exit(1);
}

console.log('[INFO] artifactsDir=', artifactsDir, 'outIndex=', outIndex, 'outLast=', outLast);

const existingIndex = loadJSON(outIndex) || [];
const existingLast = loadJSON(outLast) || {};

console.log('[INFO] seeded seen set from existing index.json ->', (existingIndex && existingIndex.length) ? existingIndex.length : 0, 'urls');

const files = fs.readdirSync(artifactsDir).sort();
const indexItems = [];
const lastByHost = { ...existingLast };

// Build a seen set seeded from existing index to avoid duplicates
const seen = new Set();
if (Array.isArray(existingIndex)) {
  for (const it of existingIndex) if (it && it.url) seen.add(it.url);
}

function pushToStream(outStream, obj, state) {
  if (state.firstWritten) outStream.write(',\n');
  outStream.write(JSON.stringify(obj));
  state.firstWritten = true;
}

(async function run() {
  // prepare output temp file in same dir as outIndex
  const outDir = path.dirname(outIndex);
  fs.mkdirSync(outDir, { recursive: true });
  const tmpOut = path.join(outDir, path.basename(outIndex) + '.tmp.' + process.pid + '.' + Date.now());
  const outStream = fs.createWriteStream(tmpOut, { encoding: 'utf8' });
  outStream.write('[\n');
  const state = { firstWritten: false };

  for (const f of files) {
    const full = path.join(artifactsDir, f);
    try {
      if (f.startsWith('shard-output-') && f.endsWith('.json')) {
        const raw = fs.readFileSync(full, 'utf8').trim();
        if (!raw) { console.log('[SKIP] empty', f); continue; }

        if (raw[0] === '[') {
          let arr;
          try {
            arr = JSON.parse(raw);
          } catch (e) {
            console.warn('[WARN] failed to parse', f, 'as JSON array - skipping:', e && e.message ? e.message : e);
            continue;
          }
          if (!Array.isArray(arr)) { console.warn('[WARN] shard-output not array, skipping:', f); continue; }
          for (const it of arr) {
            if (!it || !it.url) continue;
            if (seen.has(it.url)) continue;
            seen.add(it.url);
            pushToStream(outStream, it, state);
          }
        } else {
          // Fallback: NDJSON (one JSON per line)
          const lines = raw.split(/\r?\n/);
          for (const line of lines) {
            if (!line) continue;
            try {
              const it = JSON.parse(line);
              if (!it || !it.url) continue;
              if (seen.has(it.url)) continue;
              seen.add(it.url);
              pushToStream(outStream, it, state);
            } catch (e) {
              // skip bad line
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

  // wait for stream finish
  await new Promise((resolve, reject) => {
    outStream.on('finish', resolve);
    outStream.on('error', reject);
  });

  // rename tmp to final with robust fallback
  try {
    saveJSONAtomic(outIndex, { _placeholder: true }); // ensure dir & rename logic used below in saveJSONAtomic, but we need to place real content
    // hack: saveJSONAtomic will write something; instead we must move the tmpOut written above into outIndex.
    // do the robust rename/copy here directly (tmpOut -> outIndex)
    if (!fs.existsSync(tmpOut)) {
      throw new Error('tmpOut missing: ' + tmpOut);
    }
    try {
      fs.renameSync(tmpOut, outIndex);
      console.log('[OK] renamed', tmpOut, '->', outIndex);
    } catch (renameErr) {
      console.error('[WARN] rename failed', renameErr && (renameErr.stack || renameErr.message) ? (renameErr.stack || renameErr.message) : renameErr);
      try {
        fs.copyFileSync(tmpOut, outIndex);
        try { fs.unlinkSync(tmpOut); } catch(_) {}
        console.log('[OK] copied tmp -> final and removed tmp');
      } catch (copyErr) {
        console.error('[ERR] copy fallback failed', copyErr && (copyErr.stack || copyErr.message) ? (copyErr.stack || copyErr.message) : copyErr);
        // final fallback: read tmp and write final directly
        try {
          const data = fs.readFileSync(tmpOut, 'utf8');
          fs.writeFileSync(outIndex, data, 'utf8');
          try { fs.unlinkSync(tmpOut); } catch(_) {}
          console.log('[OK] direct-write final from tmp fallback');
        } catch (finalErr) {
          console.error('[FATAL] could not write final index.json', finalErr && (finalErr.stack || finalErr.message) ? (finalErr.stack || finalErr.message) : finalErr);
          // cleanup tmp if exists
          try { if (fs.existsSync(tmpOut)) fs.unlinkSync(tmpOut); } catch(_) {}
          process.exit(1);
        }
      }
    }
  } catch (e) {
    console.error('[FATAL] failed to move tmpOut to outIndex', e && e.stack ? e.stack : e);
    try { if (fs.existsSync(tmpOut)) fs.unlinkSync(tmpOut); } catch(_) {}
    process.exit(1);
  }

  // write last_indexed
  try {
    saveJSONAtomic(outLast, lastByHost);
    console.log('[OK] wrote', outLast);
  } catch (e) {
    console.error('[ERR] failed to write last_indexed.json', e && (e.stack || e.message) ? (e.stack || e.message) : e);
    process.exit(1);
  }

  console.log('Merged complete. total pages in index (approx):', seen.size);
  console.log('Hosts tracked:', Object.keys(lastByHost).length);
})();
