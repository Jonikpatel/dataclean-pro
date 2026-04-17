/**
 * ml-worker.js
 * Runs inside a Web Worker.
 * Loads Pyodide + numpy + pandas + scipy + scikit-learn.
 * ALL ML operations are real Python / sklearn — zero simulation.
 */

importScripts('https://cdn.jsdelivr.net/pyodide/v0.25.1/full/pyodide.js');

let py = null;

async function boot() {
  post('status', { msg: 'Downloading Python runtime (~15 MB)…' });
  py = await loadPyodide({ indexURL: 'https://cdn.jsdelivr.net/pyodide/v0.25.1/full/' });

  post('status', { msg: 'Installing numpy + pandas + scipy…' });
  await py.loadPackage(['numpy', 'pandas', 'scipy']);

  post('status', { msg: 'Installing scikit-learn…' });
  await py.loadPackage(['scikit-learn']);

  post('status', { msg: 'Initialising ML engine…' });
  await py.runPythonAsync(`
import json, warnings, re
import numpy as np
import pandas as pd
import scipy.stats as sp_stats
from sklearn.impute import KNNImputer, SimpleImputer
from sklearn.experimental import enable_iterative_imputer  # noqa
from sklearn.impute import IterativeImputer
from sklearn.ensemble import (IsolationForest,
                               RandomForestRegressor,
                               RandomForestClassifier)
from sklearn.linear_model import BayesianRidge
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.decomposition import PCA
warnings.filterwarnings('ignore')

# ── helpers ────────────────────────────────────────────────
_NULLS = {'', 'null', 'none', 'nan', 'na', 'n/a', 'nil', '#n/a',
           '#null', 'undefined', 'missing', 'unknown', '-', '--'}

def _is_null(v):
    return str(v).strip().lower() in _NULLS if v is not None else True

def _to_df(records, headers):
    df = pd.DataFrame(records, columns=headers)
    for col in df.columns:
        df[col] = df[col].apply(lambda x: np.nan if _is_null(x) else x)
        # try numeric coercion
        converted = pd.to_numeric(df[col], errors='coerce')
        if converted.notna().sum() >= df[col].notna().sum() * 0.8:
            df[col] = converted
    return df

def _df_to_records(df):
    df = df.copy()
    for col in df.select_dtypes(include=[np.floating]).columns:
        df[col] = df[col].round(6)
    df = df.where(pd.notna(df), None)
    return df.columns.tolist(), df.values.tolist()

# ── PROFILE ────────────────────────────────────────────────
def profile(records_json, headers_json):
    records = json.loads(records_json)
    headers = json.loads(headers_json)
    df = _to_df(records, headers)
    cols = {}
    for col in df.columns:
        s = df[col]
        null_mask = s.isna()
        n_null = int(null_mask.sum())
        n_total = len(s)
        non_null = s.dropna()
        n_unique = int(non_null.nunique())
        nums = pd.to_numeric(non_null, errors='coerce').dropna()
        is_num = len(nums) >= max(1, len(non_null) * 0.7)
        p = dict(name=col, total=n_total, null_count=n_null,
                 null_pct=round(n_null/n_total*100, 2) if n_total else 0,
                 unique=n_unique,
                 type='numeric' if is_num else 'text',
                 completeness=round((n_total-n_null)/n_total*100,2) if n_total else 100)
        if is_num and len(nums):
            q1,q3 = float(nums.quantile(.25)), float(nums.quantile(.75))
            iqr = q3-q1
            lo, hi = q1-1.5*iqr, q3+1.5*iqr
            z = float('nan')
            try: z = float(sp_stats.skewtest(nums).statistic) if len(nums)>=8 else float('nan')
            except: pass
            hist_c, hist_e = np.histogram(nums, bins=min(20, len(nums)))
            p.update(
                min=float(nums.min()), max=float(nums.max()),
                mean=round(float(nums.mean()),6),
                median=float(nums.median()),
                std=round(float(nums.std()),6),
                variance=round(float(nums.var()),6),
                q1=q1, q3=q3, iqr=round(iqr,6),
                skewness=round(float(nums.skew()),4),
                kurtosis=round(float(nums.kurt()),4),
                outliers_iqr=int(((nums<lo)|(nums>hi)).sum()),
                outliers_z=int((np.abs(sp_stats.zscore(nums,nan_policy='omit'))>3).sum()) if len(nums)>=4 else 0,
                sum=round(float(nums.sum()),4),
                zeros=int((nums==0).sum()),
                negatives=int((nums<0).sum()),
                p5=float(nums.quantile(.05)),
                p95=float(nums.quantile(.95)),
                histogram=dict(counts=hist_c.tolist(),
                               edges=[round(float(e),4) for e in hist_e])
            )
        else:
            vc = non_null.astype(str).value_counts()
            p['top_values'] = {str(k):int(v) for k,v in vc.head(10).items()}
            p['mode'] = str(vc.index[0]) if len(vc) else ''
            p['avg_length'] = round(float(non_null.astype(str).str.len().mean()),1) if len(non_null) else 0
        cols[col] = p
    dups = int(df.duplicated().sum())
    return json.dumps(dict(columns=cols, duplicates=dups,
                           rows=len(df), cols=len(headers)))

# ── CORRELATIONS ───────────────────────────────────────────
def correlations(records_json, headers_json):
    records = json.loads(records_json)
    headers = json.loads(headers_json)
    df = _to_df(records, headers)
    num = df.select_dtypes(include=[np.number]).columns.tolist()
    if len(num) < 2:
        return json.dumps(dict(error='Need ≥2 numeric columns', matrix=[], cols=[]))
    corr = df[num].corr().round(4)
    return json.dumps(dict(matrix=corr.values.tolist(), cols=num))

# ── CLEAN ──────────────────────────────────────────────────
def clean(records_json, headers_json, config_json):
    records = json.loads(records_json)
    headers = json.loads(headers_json)
    cfg     = json.loads(config_json)
    df = _to_df(records, headers)
    log  = []
    stat = dict(rows_in=len(df), nulls_filled=0, outliers=0,
                dupes=0, str_ops=0, coerced=0, rows_removed=0)

    # ① String / encoding cleanup
    str_cols = df.select_dtypes(include='object').columns.tolist()
    for col in str_cols:
        c = df[col].copy()
        if cfg.get('trim', True):
            df[col] = df[col].str.strip()
        if cfg.get('collapse_spaces', True):
            df[col] = df[col].str.replace(r'\\s+', ' ', regex=True)
        if cfg.get('fix_encoding', True):
            df[col] = (df[col]
                .str.replace('â€"','—').str.replace('â€™',"'")
                .str.replace('â€˜',"'").str.replace('â€œ','"')
                .str.replace('Ã©','é').str.replace('Ã¨','è'))
        gt = cfg.get('str_transform','none')
        col_gt = cfg.get('columns',{}).get(col,{}).get('str_transform','none')
        t = col_gt if col_gt != 'none' else gt
        if   t == 'lower': df[col] = df[col].str.lower()
        elif t == 'upper': df[col] = df[col].str.upper()
        elif t == 'title': df[col] = df[col].str.title()
        elif t == 'strip_special':
            df[col] = df[col].str.replace(r'[^\\w\\s.,!?@#\\-]','',regex=True)
        changed = (df[col].fillna('') != c.fillna('')).sum()
        stat['str_ops'] += int(changed)
    log.append(f'String normalisation: {stat["str_ops"]} cells modified')

    # ② Numeric coercion
    if cfg.get('coerce_numeric', True):
        for col in df.select_dtypes(include='object').columns:
            cleaned = df[col].str.replace(r'[$,€£¥%\\s]','',regex=True)
            attempt = pd.to_numeric(cleaned, errors='coerce')
            hits = int((attempt.notna() & df[col].notna()).sum())
            if hits > int(df[col].notna().sum() * 0.5):
                df[col] = attempt; stat['coerced'] += hits
    if cfg.get('coerce_bool', True):
        bmap = {'yes':1,'no':0,'true':1,'false':0,'y':1,'n':0,'on':1,'off':0}
        for col in df.select_dtypes(include='object').columns:
            mapped = df[col].str.lower().str.strip().map(bmap)
            if mapped.notna().sum() == df[col].notna().sum() > 0:
                df[col] = mapped; stat['coerced'] += int(mapped.notna().sum())
    log.append(f'Type coercion: {stat["coerced"]} cells converted')

    # ③ Deduplicate
    dup_strat = cfg.get('duplicates','keep_first')
    if dup_strat != 'none':
        before = len(df)
        subset = cfg.get('dupe_subset') or None
        keep = {'keep_first':'first','keep_last':'last','drop_all':False}.get(dup_strat,'first')
        df = df.drop_duplicates(subset=subset, keep=keep)
        stat['dupes'] = before - len(df)
        log.append(f'Duplicates: removed {stat["dupes"]}')

    # ④ Outlier handling
    method = cfg.get('outlier_method','iqr')
    action = cfg.get('outlier_action','cap')
    num_cols = df.select_dtypes(include=[np.number]).columns.tolist()

    if method == 'isolation_forest' and len(num_cols) >= 2 and len(df) >= 10:
        X = df[num_cols].copy()
        imp = SimpleImputer(strategy='median')
        X_imp = imp.fit_transform(X)
        cont = float(cfg.get('iso_contamination', 0.05))
        cont = max(0.01, min(0.5, cont))
        iso = IsolationForest(contamination=cont, random_state=42, n_jobs=-1)
        preds = iso.fit_predict(X_imp)
        mask = pd.Series(preds == -1, index=df.index)
        n = int(mask.sum())
        if action == 'remove':
            df = df[~mask]
        elif action == 'flag':
            df['_outlier_flag'] = mask.astype(int)
            df['_anomaly_score'] = iso.score_samples(X_imp).round(4)
        elif action in ('cap','replace_mean','replace_median'):
            for col in num_cols:
                q1,q3 = df[col].quantile(.25), df[col].quantile(.75)
                iqr = q3-q1
                lo,hi = q1-1.5*iqr, q3+1.5*iqr
                if action=='cap':
                    df.loc[mask, col] = df[col].clip(lo, hi)
                elif action=='replace_mean':
                    df.loc[mask, col] = df[col].mean()
                else:
                    df.loc[mask, col] = df[col].median()
        stat['outliers'] = n
        log.append(f'Outliers (IsolationForest, cont={cont}): {n} found, action={action}')

    elif method in ('iqr','zscore','modified_z') and action != 'keep':
        for col in num_cols:
            col_action = cfg.get('columns',{}).get(col,{}).get('outlier_action') or action
            if col_action == 'keep': continue
            s = df[col].dropna()
            if len(s) < 4: continue
            if method == 'iqr':
                q1,q3 = s.quantile(.25), s.quantile(.75)
                lo,hi = q1-1.5*(q3-q1), q3+1.5*(q3-q1)
            elif method == 'zscore':
                mu,sigma = s.mean(), s.std()
                lo,hi = mu-3*sigma, mu+3*sigma
            else:  # modified z
                med = s.median()
                mad = np.abs(s-med).median()
                lo,hi = med-3.5*mad, med+3.5*mad
            mask_col = (df[col]<lo)|(df[col]>hi)
            n = int(mask_col.sum())
            if n == 0: continue
            if col_action == 'remove':
                df = df[~mask_col]
            elif col_action == 'cap':
                df[col] = df[col].clip(lo, hi)
            elif col_action == 'replace_mean':
                df.loc[mask_col, col] = s.mean()
            elif col_action == 'replace_median':
                df.loc[mask_col, col] = s.median()
            elif col_action == 'nan':
                df.loc[mask_col, col] = np.nan
            stat['outliers'] += n
        log.append(f'Outliers ({method}): {stat["outliers"]} handled')

    # ⑤ Missing value imputation — REAL sklearn
    model = cfg.get('ml_model','knn')
    g_strat = cfg.get('missing_strategy','model')
    num_now = df.select_dtypes(include=[np.number]).columns.tolist()
    obj_now = df.select_dtypes(include='object').columns.tolist()
    null_before = int(df[num_now].isna().sum().sum()) if num_now else 0

    if null_before > 0 and num_now:
        strat = g_strat if g_strat != 'model' else model
        try:
            if strat == 'knn':
                k = min(5, max(1, len(df)//20 or 1))
                imp = KNNImputer(n_neighbors=k, weights='distance')
                df[num_now] = imp.fit_transform(df[num_now])
                log.append(f'KNN imputation (k={k}) on {len(num_now)} numeric cols')
            elif strat in ('mice','iterative'):
                imp = IterativeImputer(estimator=BayesianRidge(),
                                       max_iter=10, random_state=42, tol=1e-3)
                df[num_now] = imp.fit_transform(df[num_now])
                log.append('MICE/Iterative imputation (BayesianRidge)')
            elif strat == 'random_forest':
                imp = IterativeImputer(
                    estimator=RandomForestRegressor(n_estimators=50, random_state=42, n_jobs=-1),
                    max_iter=5, random_state=42)
                df[num_now] = imp.fit_transform(df[num_now])
                log.append('Random Forest iterative imputation')
            elif strat == 'mean':
                df[num_now] = SimpleImputer(strategy='mean').fit_transform(df[num_now])
            elif strat == 'median':
                df[num_now] = SimpleImputer(strategy='median').fit_transform(df[num_now])
            elif strat == 'most_frequent':
                df[num_now] = SimpleImputer(strategy='most_frequent').fit_transform(df[num_now])
            elif strat == 'zero':
                df[num_now] = df[num_now].fillna(0)
            elif strat == 'drop':
                df = df.dropna(subset=num_now)
        except Exception as e:
            df[num_now] = SimpleImputer(strategy='median').fit_transform(df[num_now])
            log.append(f'Fallback to median imputation: {str(e)[:60]}')

    # text columns
    for col in obj_now:
        col_cfg = cfg.get('columns',{}).get(col,{})
        s = col_cfg.get('missing') or g_strat
        custom = col_cfg.get('custom_fill','')
        if s == 'drop':
            df = df.dropna(subset=[col])
        elif s in ('model','knn','mice','random_forest','most_frequent','mode'):
            mode_val = df[col].mode().iloc[0] if len(df[col].dropna()) else ''
            df[col] = df[col].fillna(mode_val)
        elif s == 'ffill': df[col] = df[col].ffill()
        elif s == 'bfill': df[col] = df[col].bfill()
        elif s == 'zero':  df[col] = df[col].fillna('')
        if custom: df[col] = df[col].fillna(custom)

    null_after = int(df.isnull().sum().sum())
    stat['nulls_filled'] = null_before - null_after + int(df[obj_now].isnull().sum().sum() == 0 and True)
    # recount accurately
    orig_null_total = int(pd.DataFrame(records, columns=headers).apply(lambda c: c.apply(_is_null)).sum().sum())
    clean_null_total = int(df.isnull().sum().sum())
    stat['nulls_filled'] = max(0, orig_null_total - clean_null_total)
    stat['rows_removed'] = stat['rows_in'] - len(df)
    stat['rows_out'] = len(df)
    log.append(f'Nulls filled: {stat["nulls_filled"]}')
    log.append(f'✓ Done — {stat["rows_out"]} rows output ({stat["rows_removed"]} removed)')

    out_headers, out_rows = _df_to_records(df)
    return json.dumps(dict(headers=out_headers, rows=out_rows, stats=stat, log=log))

# ── ANOMALY DETECTION ──────────────────────────────────────
def anomalies(records_json, headers_json, contamination_f):
    records = json.loads(records_json)
    headers = json.loads(headers_json)
    df = _to_df(records, headers)
    num = df.select_dtypes(include=[np.number]).columns.tolist()
    if not num:
        return json.dumps(dict(error='No numeric columns', flags=[]))
    X = SimpleImputer(strategy='median').fit_transform(df[num])
    cont = max(0.01, min(0.5, float(contamination_f)))
    iso = IsolationForest(contamination=cont, random_state=42, n_jobs=-1)
    preds = iso.fit_predict(X)
    scores = iso.score_samples(X)
    flags = [dict(row=int(i), is_anomaly=bool(preds[i]==-1),
                  score=round(float(scores[i]),4)) for i in range(len(preds))]
    return json.dumps(dict(flags=flags, n_anomalies=int((preds==-1).sum())))

# ── PCA ────────────────────────────────────────────────────
def pca_analysis(records_json, headers_json, n_components_i):
    records = json.loads(records_json)
    headers = json.loads(headers_json)
    df = _to_df(records, headers)
    num = df.select_dtypes(include=[np.number]).columns.tolist()
    if len(num) < 2:
        return json.dumps(dict(error='Need ≥2 numeric columns'))
    X = SimpleImputer(strategy='median').fit_transform(df[num])
    X = StandardScaler().fit_transform(X)
    n = min(int(n_components_i), len(num), len(df))
    pca = PCA(n_components=n, random_state=42)
    comps = pca.fit_transform(X)
    return json.dumps(dict(
        components=comps.tolist(),
        explained_variance=pca.explained_variance_ratio_.tolist(),
        loadings=pca.components_.tolist(),
        feature_names=num,
        n_components=n
    ))

print('ML engine ready ✓')
`);

  post('ready', {});
}

// ── message dispatch ────────────────────────────────────────
const PY_FN = {
  profile:   (a) => py.runPythonAsync(`profile(${q(a.rows)}, ${q(a.headers)})`),
  clean:     (a) => py.runPythonAsync(`clean(${q(a.rows)}, ${q(a.headers)}, ${q(a.config)})`),
  correlations: (a) => py.runPythonAsync(`correlations(${q(a.rows)}, ${q(a.headers)})`),
  anomalies: (a) => py.runPythonAsync(`anomalies(${q(a.rows)}, ${q(a.headers)}, ${a.contamination || 0.05})`),
  pca:       (a) => py.runPythonAsync(`pca_analysis(${q(a.rows)}, ${q(a.headers)}, ${a.n_components || 2})`),
};

const q = (v) => JSON.stringify(JSON.stringify(v));
const post = (type, data) => self.postMessage({ type, ...data });

self.onmessage = async ({ data }) => {
  const { id, fn, args } = data;
  if (!py) { post('error', { id, msg: 'ML engine not ready' }); return; }
  const handler = PY_FN[fn];
  if (!handler) { post('error', { id, msg: `Unknown function: ${fn}` }); return; }
  try {
    const raw = await handler(args);
    post('result', { id, data: JSON.parse(raw) });
  } catch (e) {
    post('error', { id, msg: e.message });
  }
};

boot().catch(e => post('error', { id: null, msg: 'Boot failed: ' + e.message }));
