/**
 * engine.js  —  DataClean Pro core
 * State management, ML worker bridge, persistence, CSV parser.
 */
'use strict';

// ══════════════════════════════════════════════════════════════
//  GLOBAL STATE
// ══════════════════════════════════════════════════════════════
window.S = {
  datasets: {},       // { name: {headers, rows, fileName, loaded} }
  active: null,
  mlReady: false,
  sqlReady: false,
  mlWorker: null,
  sqlConn: null,
  sqlDB: null,
  useFallbackSQL: false,
  lastSQL: null,        // {headers, rows}
  chartType: 'bar',
  _mlCalls: new Map(),
  _mlId: 0,
};

// ══════════════════════════════════════════════════════════════
//  ML WORKER BRIDGE
// ══════════════════════════════════════════════════════════════
function mlStart() {
  S.mlWorker = new Worker('js/ml-worker.js');
  S.mlWorker.onmessage = ({ data }) => {
    if (data.type === 'status') { mlStatus(data.msg); return; }
    if (data.type === 'ready') {
      S.mlReady = true;
      mlStatus('ML engine ready ✓', 'ok');
      setBadge('badge-ml', 'ML Ready', 'ok');
      return;
    }
    const cb = S._mlCalls.get(data.id);
    if (!cb) return;
    S._mlCalls.delete(data.id);
    if (data.type === 'error') cb.reject(new Error(data.msg));
    else cb.resolve(data.data);
  };
  S.mlWorker.onerror = e => mlStatus('ML error: ' + e.message, 'err');
}

function ml(fn, args) {
  return new Promise((resolve, reject) => {
    if (!S.mlWorker) return reject(new Error('Worker not init'));
    const id = ++S._mlId;
    S._mlCalls.set(id, { resolve, reject });
    S.mlWorker.postMessage({ id, fn, args });
  });
}

function mlStatus(msg, type = 'info') {
  const el = document.getElementById('ml-status-txt');
  if (el) { el.textContent = msg; el.className = 'ml-msg ' + type; }
}

// ══════════════════════════════════════════════════════════════
//  DUCKDB SQL ENGINE
// ══════════════════════════════════════════════════════════════
// ══════════════════════════════════════════════════════════════
//  DUCKDB SQL ENGINE  (DuckDB only, no fallback)
// ══════════════════════════════════════════════════════════════
// ══════════════════════════════════════════════════════════════
//  DUCKDB SQL ENGINE  (DuckDB only)
// ══════════════════════════════════════════════════════════════
async function sqlInit() {
  try {
    // Use official helper to get the correct jsDelivr bundles
    const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();
    const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);

    // Start DuckDB worker for the selected bundle
    const worker = new Worker(bundle.mainWorker);
    const logger = new duckdb.ConsoleLogger();

    S.sqlDB = new duckdb.AsyncDuckDB(logger, worker);

    // Instantiate the database
    await S.sqlDB.instantiate(bundle.mainModule, bundle.pthreadWorker);
    S.sqlConn = await S.sqlDB.connect();

    S.sqlReady = true;
    setBadge('badge-sql', 'DuckDB Ready', 'ok');

    // Register any datasets already loaded into S.datasets
    for (const [n, ds] of Object.entries(S.datasets)) {
      await _sqlRegister(n, ds);
    }
  } catch (e) {
    console.error('DuckDB init failed:', e);
    S.sqlReady = false;
    setBadge('badge-sql', 'DuckDB Error', 'err');
    notify('DuckDB SQL engine failed to initialize. Check console.', 'err');
  }
}
async function _sqlRegister(name, ds) {
  if (S.useFallbackSQL || !S.sqlConn) return;
  try {
    const csv = [ds.headers.join(','), ...ds.rows.map(r =>
      r.map(v => `"${String(v ?? '').replace(/"/g, '""')}"`).join(',')
    )].join('\n');
    await S.sqlDB.registerFileText(name + '.csv', csv);
    await S.sqlConn.query(
      `CREATE OR REPLACE TABLE "${name}" AS SELECT * FROM read_csv_auto('${name}.csv', header=true)`
    );
  } catch (e) { console.warn('sqlRegister:', e.message); }
}

async function sqlRun(sql) {
  if (!S.sqlReady || !S.sqlConn) {
    throw new Error('DuckDB engine not ready yet');
  }
  try {
    const res = await S.sqlConn.query(sql);

    // Column names from Arrow schema
    const headers = res.schema.fields.map(f => f.name);

    // Rows as array of objects
    const objs = res.toArray();  // DuckDB-Wasm pattern [web:45][web:61]

    // Convert to array-of-arrays of strings/nulls for the grid
    const rows = objs.map(o =>
      headers.map(h => {
        const v = o[h];
        return v === null || v === undefined ? null : String(v);
      })
    );

    return { headers, rows };
  } catch (e) {
    throw new Error(e.message);
  }
}

// ── Fallback SQL parser (SELECT with JOIN/GROUP BY/ORDER BY/LIMIT/CTE) ──
function _sqlFallback(sql) {
  const up = sql.toUpperCase();
  if (!up.startsWith('SELECT') && !up.startsWith('WITH'))
    throw new Error('Only SELECT statements are supported');

  // Handle CTE
  const ctes = {};
  let mainSQL = sql;
  const cteMatch = sql.match(/^WITH\s+([\s\S]+?)\s+(?=SELECT\s)/i);
  if (cteMatch) {
    // parse CTE definitions
    const ctePart = cteMatch[1];
    const cteRe = /(\w+)\s+AS\s*\(([\s\S]+?)\)(?=\s*,\s*\w+\s+AS\s*\(|\s*SELECT)/gi;
    let m;
    while ((m = cteRe.exec(ctePart + ' SELECT')) !== null) {
      const res = _sqlFallback(m[2].trim());
      const tmp = '__cte_' + m[1];
      S.datasets[tmp] = { headers: res.headers, rows: res.rows };
      ctes[m[1]] = tmp;
    }
    mainSQL = sql.slice(sql.toUpperCase().lastIndexOf('\nSELECT') + 1) ||
              sql.slice(sql.toUpperCase().lastIndexOf('SELECT'));
  }

  const g = (kw, nexts) => {
    const p = new RegExp(`\\b${kw}\\b([\\s\\S]+?)(?=\\b(?:${nexts.join('|')})\\b|$)`, 'i');
    return (mainSQL.match(p) || [])[1]?.trim() || '';
  };
  const selRaw = g('SELECT', ['FROM','WHERE','GROUP\\s+BY','ORDER\\s+BY','LIMIT','HAVING']);
  const fromRaw = g('FROM', ['WHERE','(?:INNER|LEFT|RIGHT)\\s+JOIN','JOIN','GROUP\\s+BY','ORDER\\s+BY','LIMIT','HAVING']);
  const whereRaw = g('WHERE', ['GROUP\\s+BY','HAVING','ORDER\\s+BY','LIMIT']);
  const groupRaw = g('GROUP\\s+BY', ['HAVING','ORDER\\s+BY','LIMIT']);
  const havingRaw = g('HAVING', ['ORDER\\s+BY','LIMIT']);
  const orderRaw = g('ORDER\\s+BY', ['LIMIT']);
  const limitM = mainSQL.match(/LIMIT\s+(\d+)(?:\s+OFFSET\s+(\d+))?/i);
  const limitN = limitM ? +limitM[1] : null;
  const offsetN = limitM ? +(limitM[2] || 0) : 0;
  const distinct = /^\s*DISTINCT\b/i.test(selRaw);
  const selClean = distinct ? selRaw.replace(/^\s*DISTINCT\s+/i, '') : selRaw;

  const fromTable = (fromRaw.match(/^\s*(\w+)/) || [])[1];
  if (!fromTable) throw new Error('Cannot parse FROM clause');
  const actualFrom = ctes[fromTable] || fromTable;
  if (!S.datasets[actualFrom])
    throw new Error(`Table "${fromTable}" not found. Available: ${Object.keys(S.datasets).filter(k=>!k.startsWith('__cte_')).join(', ') || '(none — load a dataset first)'}`);

  let { headers: H, rows: R } = S.datasets[actualFrom];
  R = R.map(r => [...r]);

  // JOINs
  for (const jm of (mainSQL.match(/\b(?:INNER\s+|LEFT\s+)?JOIN\s+\w+(?:\s+(?:AS\s+)?\w+)?\s+ON\s+[\w.]+\s*=\s*[\w.]+/gi) || [])) {
    const jp = jm.match(/JOIN\s+(\w+)(?:\s+(?:AS\s+)?(\w+))?\s+ON\s+([\w.]+)\s*=\s*([\w.]+)/i);
    if (!jp) continue;
    const jTbl = ctes[jp[1]] || jp[1];
    if (!S.datasets[jTbl]) throw new Error(`JOIN table "${jp[1]}" not found`);
    const jDs = S.datasets[jTbl];
    const lc = jp[3].split('.').pop(), rc = jp[4].split('.').pop();
    const li = H.findIndex(h => h.toLowerCase() === lc.toLowerCase());
    const ri = jDs.headers.findIndex(h => h.toLowerCase() === rc.toLowerCase());
    if (li < 0) throw new Error(`Column "${lc}" not found in ${fromTable}`);
    if (ri < 0) throw new Error(`Column "${rc}" not found in ${jp[1]}`);
    const newCols = jDs.headers.filter(h => !H.includes(h));
    const joined = [];
    R.forEach(lr => {
      const matches = jDs.rows.filter(jr => String(jr[ri]) === String(lr[li]));
      if (matches.length) matches.forEach(jr => joined.push([...lr, ...newCols.map(h => jr[jDs.headers.indexOf(h)])]));
      else joined.push([...lr, ...newCols.map(() => null)]);
    });
    H = [...H, ...newCols]; R = joined;
  }

  // WHERE
  if (whereRaw) R = R.filter(r => _evalWhere(whereRaw, r, H));

  // SELECT / GROUP BY
  const cols = _parseSEL(selClean, H);
  const hasAgg = cols.some(c => c.agg);
  let outH, outR;

  if (hasAgg || groupRaw) {
    const gcols = groupRaw ? groupRaw.split(',').map(s => s.trim()) : [];
    const gidx  = gcols.map(gc => {
      const i = H.findIndex(h => h.toLowerCase() === gc.toLowerCase());
      if (i < 0) throw new Error(`GROUP BY column "${gc}" not found`);
      return i;
    });
    const groups = new Map();
    R.forEach(r => {
      const k = gidx.map(i => String(r[i] ?? '')).join('\x01');
      if (!groups.has(k)) groups.set(k, []);
      groups.get(k).push(r);
    });
    outH = cols.map(c => c.alias || c.name);
    outR = [...groups.values()].map(grp =>
      cols.map(c => c.agg ? _agg(c.agg, c.aggCol, grp, H) : grp[0][H.findIndex(h => h.toLowerCase() === (c.col||'').toLowerCase())])
    );
    if (havingRaw) outR = outR.filter(r => _evalWhere(havingRaw, r, outH));
  } else {
    outH = selClean.trim() === '*' ? H : cols.map(c => c.alias || c.name);
    outR = selClean.trim() === '*' ? R : R.map(r => cols.map(c => {
      if (c.agg) return _agg(c.agg, c.aggCol, [r], H);
      if (c.literal !== undefined) return c.literal;
      const ci = H.findIndex(h => h.toLowerCase() === (c.col || '').toLowerCase());
      return ci >= 0 ? r[ci] : null;
    }));
  }

  // ORDER BY
  if (orderRaw) {
    outR.sort((a, b) => {
      for (const part of orderRaw.split(',')) {
        const m = part.trim().match(/^(.+?)\s*(ASC|DESC)?$/i);
        if (!m) continue;
        let ci = outH.findIndex(h => h.toLowerCase() === m[1].trim().toLowerCase());
        if (ci < 0) { const n = +m[1]; if (!isNaN(n)) ci = n - 1; }
        if (ci < 0) continue;
        const [av, bv] = [a[ci], b[ci]];
        const [an, bn] = [parseFloat(av), parseFloat(bv)];
        let cmp = (!isNaN(an) && !isNaN(bn)) ? an - bn : String(av ?? '').localeCompare(String(bv ?? ''));
        if ((m[2] || 'ASC').toUpperCase() === 'DESC') cmp = -cmp;
        if (cmp !== 0) return cmp;
      }
      return 0;
    });
  }

  if (distinct) {
    const seen = new Set();
    outR = outR.filter(r => { const k = r.join('\x01'); return !seen.has(k) && seen.add(k); });
  }

  if (limitN !== null) outR = outR.slice(offsetN, offsetN + limitN);

  // cleanup temp CTE tables
  Object.values(ctes).forEach(k => delete S.datasets[k]);

  return { headers: outH, rows: outR };
}

function _parseSEL(sel, headers) {
  if (sel.trim() === '*') return headers.map(h => ({ name: h, col: h }));
  const parts = []; let depth = 0, cur = '';
  for (const ch of sel + ',') {
    if (ch === '(') { depth++; cur += ch; }
    else if (ch === ')') { depth--; cur += ch; }
    else if (ch === ',' && depth === 0) { parts.push(cur.trim()); cur = ''; }
    else cur += ch;
  }
  return parts.filter(Boolean).map(raw => {
    const am = raw.match(/\s+AS\s+(\w+)$/i);
    const alias = am ? am[1] : null;
    const expr = am ? raw.slice(0, raw.lastIndexOf(am[0])).trim() : raw.trim();
    const aggM = expr.match(/^(COUNT|SUM|AVG|MIN|MAX|ROUND|ABS|UPPER|LOWER|LENGTH|COALESCE)\s*\((.+)\)$/i);
    if (aggM) {
      const fn = aggM[1].toUpperCase(), arg = aggM[2].trim();
      return { name: expr, col: arg, aggCol: arg, agg: fn, alias: alias || fn.toLowerCase() + '_' + arg.replace(/[^a-z0-9]/gi, '_') };
    }
    const numLit = parseFloat(expr);
    if (!isNaN(numLit) && String(numLit) === expr) return { name: expr, col: null, alias, literal: numLit };
    if (/^['"]/.test(expr)) return { name: expr, col: null, alias, literal: expr.slice(1, -1) };
    const cn = expr.replace(/[`"[\]]/g, '');
    return { name: cn, col: cn, alias };
  });
}

function _agg(fn, col, rows, headers) {
  const ci = col === '*' ? 0 : headers.findIndex(h => h.toLowerCase() === (col || '').toLowerCase());
  const vals = rows.map(r => ci >= 0 ? r[ci] : r[0]).filter(v => v !== null && v !== '' && !_N(v));
  const nums = vals.map(v => parseFloat(v)).filter(v => !isNaN(v));
  switch (fn) {
    case 'COUNT': return col === '*' ? rows.length : vals.length;
    case 'SUM': return nums.length ? +nums.reduce((a, b) => a + b, 0).toFixed(6) : 0;
    case 'AVG': return nums.length ? +(nums.reduce((a, b) => a + b) / nums.length).toFixed(6) : null;
    case 'MIN': return nums.length ? Math.min(...nums) : (vals.length ? vals.sort()[0] : null);
    case 'MAX': return nums.length ? Math.max(...nums) : (vals.length ? vals.sort().pop() : null);
    case 'ABS': return nums.length ? Math.abs(nums[0]) : null;
    case 'ROUND': return nums.length ? Math.round(nums[0]) : null;
    case 'UPPER': return vals.length ? String(vals[0]).toUpperCase() : null;
    case 'LOWER': return vals.length ? String(vals[0]).toLowerCase() : null;
    case 'LENGTH': return vals.length ? String(vals[0]).length : 0;
    default: return null;
  }
}

function _evalWhere(clause, row, headers) {
  try {
    let e = clause
      .replace(/\bAND\b/gi, '&&').replace(/\bOR\b/gi, '||').replace(/\bNOT\s+/gi, '!')
      .replace(/\bIS\s+NOT\s+NULL\b/gi, '__ISNOTNULL__')
      .replace(/\bIS\s+NULL\b/gi, '__ISNULL__');
    headers.forEach((h, ci) => {
      const v = row[ci] ?? '';
      const n = parseFloat(v);
      const val = (!isNaN(n) && v !== '' && v !== null) ? n : `"${String(v).replace(/"/g, '\\"')}"`;
      e = e.replace(new RegExp('\\b' + h.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + '\\b', 'gi'), String(val));
    });
    e = e.replace(/__ISNULL__/g, '=== null').replace(/__ISNOTNULL__/g, '!== null');
    return Boolean(Function('"use strict";return(' + e + ')')());
  } catch { return true; }
}

const _N = v => ['', 'null', 'none', 'nan', 'na', 'n/a', 'nil', '#n/a', 'undefined'].includes(String(v ?? '').toLowerCase().trim());

// ══════════════════════════════════════════════════════════════
//  DATASET MANAGEMENT
// ══════════════════════════════════════════════════════════════
function dsAdd(name, headers, rows, fileName) {
  const n = name.replace(/[^a-zA-Z0-9_]/g, '_').replace(/^(\d)/, 't$1');
  S.datasets[n] = { headers, rows, fileName, loaded: new Date() };
  if (!S.active) S.active = n;
  _idbSave(n);
  if (S.sqlReady && !S.useFallbackSQL) _sqlRegister(n, S.datasets[n]);
  refreshUI();
  notify(`Loaded "${fileName}" → ${rows.length.toLocaleString()} rows × ${headers.length} cols`, 'ok');
}

function dsDel(name) {
  delete S.datasets[name];
  if (S.active === name) S.active = Object.keys(S.datasets)[0] || null;
  _idbDel(name);
  refreshUI();
}

// ══════════════════════════════════════════════════════════════
//  CSV PARSER  (handles quoted fields, any separator)
// ══════════════════════════════════════════════════════════════
function csvParse(text, sep = ',') {
  const lines = text.split(/\r?\n/);
  const parse = line => {
    const cols = []; let cur = '', inQ = false;
    for (let i = 0; i < line.length; i++) {
      const c = line[i];
      if (c === '"') { if (inQ && line[i+1] === '"') { cur += '"'; i++; } else inQ = !inQ; }
      else if (c === sep && !inQ) { cols.push(cur); cur = ''; }
      else cur += c;
    }
    cols.push(cur);
    return cols.map(v => v.trim());
  };
  const nonEmpty = lines.filter(l => l.trim());
  if (!nonEmpty.length) return { headers: [], rows: [] };
  const headers = parse(nonEmpty[0]);
  const len = headers.length;
  const rows = nonEmpty.slice(1)
    .map(l => { const r = parse(l); while (r.length < len) r.push(''); return r.slice(0, len); })
    .filter(r => r.some(c => c));
  return { headers, rows };
}

// ══════════════════════════════════════════════════════════════
//  INDEXEDDB  (session persistence)
// ══════════════════════════════════════════════════════════════
const IDB = { db: null, name: 'dcp_v2', store: 'ds' };

async function _idbOpen() {
  if (IDB.db) return IDB.db;
  return new Promise((res, rej) => {
    const req = indexedDB.open(IDB.name, 1);
    req.onupgradeneeded = e => e.target.result.createObjectStore(IDB.store);
    req.onsuccess = e => { IDB.db = e.target.result; res(IDB.db); };
    req.onerror = rej;
  });
}
async function _idbSave(name) {
  try { const db = await _idbOpen(); const tx = db.transaction(IDB.store,'readwrite'); tx.objectStore(IDB.store).put(S.datasets[name], name); } catch {}
}
async function _idbDel(name) {
  try { const db = await _idbOpen(); const tx = db.transaction(IDB.store,'readwrite'); tx.objectStore(IDB.store).delete(name); } catch {}
}
async function _idbLoad() {
  try {
    const db = await _idbOpen();
    const tx = db.transaction(IDB.store,'readonly');
    const ks = await new Promise(r => { const q = tx.objectStore(IDB.store).getAllKeys(); q.onsuccess = () => r(q.result); });
    const vs = await new Promise(r => { const q = tx.objectStore(IDB.store).getAll(); q.onsuccess = () => r(q.result); });
    ks.forEach((k, i) => { if (vs[i]) { S.datasets[k] = vs[i]; if (!S.active) S.active = k; } });
    if (Object.keys(S.datasets).length) refreshUI();
  } catch {}
}

// ══════════════════════════════════════════════════════════════
//  UI HELPERS
// ══════════════════════════════════════════════════════════════
function refreshUI() {
  _sidebarRender();
  _selectsUpdate();
  _fileListRender();
  _tableChipsUpdate();
  qs('#nav-count').textContent = `${dsCount()} dataset${dsCount() !== 1 ? 's' : ''}`;
}

function dsCount() { return Object.keys(S.datasets).filter(k => !k.startsWith('__cte_')).length; }
function dsNames() { return Object.keys(S.datasets).filter(k => !k.startsWith('__cte_')); }

function _sidebarRender() {
  const el = qs('#sb-ds');
  const names = dsNames();
  const COLORS = ['#22c55e','#60a5fa','#f59e0b','#a78bfa','#f87171','#34d399','#fb923c','#38bdf8'];
  el.innerHTML = names.length ? names.map((n, i) => {
    const d = S.datasets[n];
    const nulls = d.rows.reduce((s, r) => s + r.filter(v => _N(v)).length, 0);
    const pct = Math.round((1 - nulls / (d.rows.length * d.headers.length || 1)) * 100);
    return `<div class="sb-item ${S.active === n ? 'active' : ''}" onclick="setActive('${n}')">
      <div class="sb-dot" style="background:${COLORS[i % COLORS.length]}"></div>
      <div class="sb-info"><div class="sb-name">${n}</div><div class="sb-meta">${d.rows.length.toLocaleString()}r · ${d.headers.length}c · ${pct}%</div></div>
      <button class="sb-del" onclick="event.stopPropagation();dsDel('${n}')" title="Remove">×</button>
    </div>`;
  }).join('') : '<div class="sb-empty">No datasets.<br>Drop CSV files anywhere.</div>';
}

function _selectsUpdate() {
  const names = dsNames();
  const opts = '<option value="">Select dataset…</option>' + names.map(n => `<option value="${n}">${n} (${S.datasets[n].rows.length.toLocaleString()}r)</option>`).join('');
  ['#sel-grid','#sel-chart','#sel-cmp-a','#sel-cmp-b','#sel-profile'].forEach(id => {
    const el = qs(id); if (!el) return;
    const prev = el.value; el.innerHTML = opts;
    if (names.includes(prev)) el.value = prev;
  });
}

function _fileListRender() {
  const el = qs('#files-list'); if (!el) return;
  const names = dsNames();
  el.innerHTML = names.length ? `<div class="sec-lbl">Loaded Datasets (${names.length})</div>` + names.map(n => {
    const d = S.datasets[n];
    const nulls = d.rows.reduce((s, r) => s + r.filter(v => _N(v)).length, 0);
    const pct = Math.round((1 - nulls / (d.rows.length * d.headers.length || 1)) * 100);
    const dupes = d.rows.length - new Set(d.rows.map(r => r.join('|'))).size;
    return `<div class="ds-card">
      <div class="ds-card-head">
        <span class="ds-card-name">${n}</span>
        <span class="badge bg-n">${d.fileName}</span>
        <span class="badge bg-g">${d.rows.length.toLocaleString()} rows</span>
        <span class="badge bg-i">${d.headers.length} cols</span>
        <span class="badge ${pct >= 90 ? 'bg-g' : pct >= 70 ? 'bg-w' : 'bg-e'}">${pct}% complete</span>
        ${dupes ? `<span class="badge bg-w">${dupes} dupes</span>` : ''}
        <div class="ml-auto flex gap2">
          <button class="btn btn-sm" onclick="setActive('${n}');showTab('grid')">Grid</button>
          <button class="btn btn-sm" onclick="SQL.quickQuery('${n}');showTab('sql')">SQL</button>
          <button class="btn btn-sm" onclick="Cleaner.openFor('${n}')">Clean</button>
          <button class="btn btn-sm" onclick="Chart.quick('${n}');showTab('chart')">Chart</button>
          <button class="btn btn-sm btn-danger" onclick="dsDel('${n}')">×</button>
        </div>
      </div>
      <div class="ds-card-body"><div class="col-pills">${d.headers.map(h => `<span class="col-pill">${escH(h)}</span>`).join('')}</div></div>
    </div>`;
  }).join('') : '';
}

function _tableChipsUpdate() {
  const el = qs('#table-chips'); if (!el) return;
  el.innerHTML = dsNames().map(n =>
    `<span class="tbl-chip" onclick="SQL.insertTable('${n}')" title="${S.datasets[n].headers.join(', ')}">${n}</span>`
  ).join('');
}

function setActive(name) {
  S.active = name;
  _sidebarRender();
  const sg = qs('#sel-grid'); if (sg) { sg.value = name; Grid.setDs(name); }
}

function setBadge(id, text, type) {
  const el = qs('#' + id); if (!el) return;
  el.textContent = text;
  el.className = 'badge ' + (type === 'ok' ? 'bg-g' : type === 'err' ? 'bg-e' : 'bg-n');
}

// ══════════════════════════════════════════════════════════════
//  NOTIFICATIONS
// ══════════════════════════════════════════════════════════════
function notify(msg, type = 'info', dur = 4000) {
  const area = qs('#notif-area');
  const el = document.createElement('div');
  el.className = 'notif ' + type;
  el.innerHTML = `<span>${type === 'ok' ? '✓' : type === 'err' ? '✕' : 'ℹ'}</span>${escH(msg)}`;
  area.appendChild(el);
  setTimeout(() => { el.style.opacity = '0'; el.style.transform = 'translateX(110%)'; setTimeout(() => el.remove(), 350); }, dur);
}

// ══════════════════════════════════════════════════════════════
//  FILE LOADING
// ══════════════════════════════════════════════════════════════
function loadFiles(files) {
  [...files].forEach(f => {
    const r = new FileReader();
    r.onload = e => {
      const sep = f.name.endsWith('.tsv') ? '\t' : ',';
      const { headers, rows } = csvParse(e.target.result, sep);
      if (!headers.length) { notify(`"${f.name}" appears empty`, 'err'); return; }
      dsAdd(f.name.replace(/\.[^.]+$/, '').replace(/[^a-zA-Z0-9_]/g, '_').replace(/^(\d)/, 't$1'), headers, rows, f.name);
    };
    r.readAsText(f);
  });
}

// ══════════════════════════════════════════════════════════════
//  SAMPLE DATASETS
// ══════════════════════════════════════════════════════════════
const SAMPLES = {
  sales: {
    fileName: 'sales.csv',
    headers: ['order_id','date','region','salesperson','category','product','qty','unit_price','total','discount','profit'],
    rows: [
      ['S001','2024-01-05','North','Alice','Electronics','Laptop','2','899.99','1799.98','0.05','360'],
      ['S002','2024-01-08','South','Bob','Furniture','Desk Chair','5','299.00','1495.00','0','448.50'],
      ['S003','2024-01-10','East','Carol','Electronics','Monitor','3','349.00','1047.00','0.10','209.40'],
      ['S004','2024-01-12','West','Dave','Clothing','Jacket','10','89.99','899.90','0.15','224.98'],
      ['S005','2024-01-15','North','Alice','Electronics','Headphones','8','79.99','639.92','0','192.00'],
      ['S006','2024-01-18','South','Bob','Furniture','Standing Desk','2','549.00','1098.00','0.05','219.60'],
      ['S007','2024-01-20','East','Carol','Clothing','Sneakers','15','129.00','1935.00','0.20','580.50'],
      ['S008','2024-01-22','West','Dave','Electronics','Tablet','4','499.00','1996.00','0','598.80'],
      ['S009','2024-01-25','North','Eve','Furniture','Bookshelf','3','199.00','597.00','0','179.10'],
      ['S010','2024-01-28','South','Bob','Electronics','Laptop','1','899.99','899.99','0.10','179.99'],
      ['S011','2024-02-02','East','Carol','Clothing','T-Shirt','50','24.99','1249.50','0','374.85'],
      ['S012','2024-02-05','West','Dave','Furniture','Monitor Stand','6','89.00','534.00','0','160.20'],
    ]
  },
  employees: {
    fileName: 'employees.csv',
    headers: ['emp_id','name','dept','role','salary','hire_date','city','manager','perf','remote'],
    rows: [
      ['E001','Alice Chen','Engineering','Senior Engineer','125000','2020-03-15','New York','E010','Excellent','Yes'],
      ['E002','Bob Martinez','Sales','Account Executive','75000','2021-06-01','Chicago','E011','Good','No'],
      ['E003','Carol White','Engineering','Staff Engineer','155000','2018-01-10','San Francisco','E010','Excellent','Yes'],
      ['E004','David Kim','Marketing','Marketing Manager','95000','2019-09-22','New York','E012','Good','No'],
      ['E005','Eve Johnson','Engineering','Junior Engineer','85000','2023-02-14','Remote','E010','Good','Yes'],
      ['E006','Frank Lee','Sales','Sales Director','130000','2017-05-30','Chicago','E011','Excellent','No'],
      ['E007','Grace Park','HR','HR Specialist','72000','2022-08-01','New York','E013','Satisfactory','Yes'],
      ['E008','Henry Adams','Engineering','Principal Engineer','175000','2016-03-01','San Francisco','E010','Outstanding','Yes'],
      ['E009','Iris Wong','Marketing','Growth Analyst','88000','2021-11-15','Remote','E012','Good','Yes'],
      ['E010','Jack Miller','Engineering','VP Engineering','220000','2015-01-01','San Francisco','','Outstanding','No'],
    ]
  },
  messy: {
    fileName: 'messy_data.csv',
    headers: ['id','name','age','salary','dept','score','date','phone','country','notes'],
    rows: [
      ['1','Alice Johnson','28','75000','Engineering','8.5','2024-01-15','+1-555-0100','USA','Top performer'],
      ['2','bob smith','','62000','Sales','7.2','2024-01-16','','Canada',''],
      ['3','Carol  White','34','','Engineering','','2024-01-17','+1-555-0102','USA','Needs review'],
      ['4','DAVID LEE','29','58000','Marketing','9.1','','555.0103','UK',''],
      ['5','eve martinez','45','90000','Engineering','6.8','2024-01-19','+1-555-0104','USA',''],
      ['6','Frank N/A','null','72000','Sales','N/A','2024-01-20','','Australia','Invalid age'],
      ['2','bob smith','','62000','Sales','7.2','2024-01-16','','Canada',''],
      ['7','Grace Kim','31','81000','HR','8.0','2024-01-22','+1-555-0106','USA',''],
      ['8','henry park','999','65000','Engineering','7.5','2024-01-23','','Canada','Age outlier'],
      ['9','Iris Chen','36','','Marketing','','2024-01-24','+1-555-0108','USA',''],
      ['10','jack williams','-5','78000','Sales','8.3','2024-01-25','','UK','Neg age'],
      ['11','Karen Davis','41','95000','Engineering','9.0','2024-01-26','+1-555-0110','USA',''],
    ]
  },
  transactions: {
    fileName: 'transactions.csv',
    headers: ['txn_id','date','customer','channel','amount','currency','method','status','category','country'],
    rows: [
      ['T001','2024-01-01','C-100','Web','142.50','USD','Credit Card','Completed','Electronics','USA'],
      ['T002','2024-01-02','C-201','Mobile','29.99','USD','PayPal','Completed','Clothing','USA'],
      ['T003','2024-01-02','C-102','Web','899.00','USD','Credit Card','Completed','Electronics','Canada'],
      ['T004','2024-01-03','C-305','In-Store','54.30','USD','Cash','Completed','Food','USA'],
      ['T005','2024-01-04','C-100','Web','199.99','USD','Credit Card','Refunded','Furniture','USA'],
      ['T006','2024-01-05','C-408','Mobile','14.99','GBP','Credit Card','Completed','Clothing','UK'],
      ['T007','2024-01-06','C-102','Web','349.00','USD','Wire','Pending','Electronics','Canada'],
      ['T008','2024-01-07','C-509','In-Store','82.40','EUR','Credit Card','Completed','Food','Germany'],
      ['T009','2024-01-08','C-201','Web','459.99','USD','PayPal','Completed','Electronics','USA'],
      ['T010','2024-01-09','C-610','Mobile','34.99','USD','Credit Card','Completed','Clothing','USA'],
      ['T011','2024-01-10','C-100','Web','1299.00','USD','Credit Card','Completed','Electronics','USA'],
      ['T012','2024-01-11','C-711','In-Store','18.75','GBP','Cash','Completed','Food','UK'],
    ]
  }
};

function loadSample(name) {
  const s = SAMPLES[name];
  if (!s) return;
  const n = name;
  dsAdd(n, s.headers, s.rows.map(r => [...r]), s.fileName);
}

// ══════════════════════════════════════════════════════════════
//  UTILS
// ══════════════════════════════════════════════════════════════
const qs = sel => document.querySelector(sel);
const escH = s => String(s ?? '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
const nFmt = (n, d = 4) => typeof n === 'number' ? +n.toFixed(d) : n;
function dl(content, name, mime = 'text/plain') {
  const a = Object.assign(document.createElement('a'), { href: URL.createObjectURL(new Blob([content], { type: mime })), download: name });
  a.click(); URL.revokeObjectURL(a.href);
}
function dlJSON(obj, name) { dl(JSON.stringify(obj, null, 2), name, 'application/json'); }
function showTab(t) {
  document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  const p = qs('#p-' + t), b = qs('#t-' + t);
  if (p) p.classList.add('active');
  if (b) b.classList.add('active');
  if (t === 'grid') Grid.render();
  if (t === 'profile') Profiler.render();
  if (t === 'sql') _tableChipsUpdate();
  if (t === 'compare') Compare.init();
  if (t === 'chart') Chart.init();
}

// expose
Object.assign(window, {
  S, ml, sqlRun, dsAdd, dsDel, loadFiles, loadSample, setActive,
  refreshUI, notify, dl, dlJSON, escH, nFmt, _N, qs, showTab, dsNames,
});

// ── BOOT ─────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', async () => {
  window.addEventListener('dragover', e => e.preventDefault());
  window.addEventListener('drop', e => {
    e.preventDefault();
    const files = [...e.dataTransfer.files].filter(f => /\.(csv|tsv)$/i.test(f.name));
    if (files.length) loadFiles(files);
  });
  mlStart();
  await _idbLoad();
  if (typeof duckdb !== 'undefined') {
  sqlInit().catch(e => {
    console.error('DuckDB init failed:', e);
    S.sqlReady = false;
    setBadge('badge-sql', 'DuckDB Error', 'err');
    notify('DuckDB SQL engine failed to initialize. Check console for details.', 'err');
  });
} else {
  S.sqlReady = false;
  setBadge('badge-sql', 'DuckDB not loaded', 'err');
  notify('DuckDB not loaded. SQL will not work without DuckDB.', 'err');
}
});
