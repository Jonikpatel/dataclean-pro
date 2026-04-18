# DataClean Pro — Analyst Studio

> A real ML-powered data cleaning and analysis studio that runs 100% in your browser.

**Live demo:** `https://jonikpatel.github.io/dataclean-pro`

---

## What makes this different from other "data tools"

Every ML operation is **real Python running in your browser** via Pyodide:

| Feature | Technology |
|---|---|
| KNN Imputation | `sklearn.impute.KNNImputer` |
| MICE Imputation | `sklearn.impute.IterativeImputer` + `BayesianRidge` |
| Random Forest Imputation | `sklearn.ensemble.RandomForestRegressor` |
| Outlier Detection | `sklearn.ensemble.IsolationForest` + IQR + Z-score |
| Correlation Matrix | `pandas.DataFrame.corr()` |
| PCA Analysis | `sklearn.decomposition.PCA` |
| Anomaly Detection | `sklearn.ensemble.IsolationForest` |
| Statistical Profile | `pandas` + `scipy.stats` |
| SQL Engine | DuckDB-WASM (real SQL, not a fake parser) |
| Session Persistence | IndexedDB |

---

## Tabs

| Tab | What it does |
|---|---|
| **Files** | Load multiple CSV/TSV files. Each becomes a SQL table. Persisted in IndexedDB. |
| **Grid** | Excel-like explorer: sort, per-column filters (`>=`, `<`, `null`, `!=`), freeze pane, hide columns, column stats panel with sparkline |
| **Auto Clean** | Real sklearn cleaning pipeline: KNN/MICE/RF imputation, IsolationForest outlier detection, string normalization, type coercion, deduplication. Per-column overrides. |
| **SQL** | DuckDB-WASM SQL engine. Full SQL: JOINs, CTEs, window functions, aggregates. Ctrl+Enter to run. Query history. |
| **Charts** | 8 chart types (bar, line, pie, donut, scatter, radar, polar, bubble). Aggregation by group. Export PNG. |
| **Compare** | Side-by-side stats diff of two datasets: null%, means, column differences, row counts |
| **Profile** | Full statistical profile using pandas/scipy: min/max/mean/median/std/skewness/kurtosis/outliers/histogram per column. Includes correlation matrix, PCA, and anomaly detection. |

---

## Project structure

```
dataclean-pro/
├── index.html                  # App shell — all tabs wired up
├── css/
│   └── app.css                 # Dark analyst theme
├── js/
│   ├── engine.js               # Core: state, ML bridge, SQL engine, IndexedDB, CSV parser
│   ├── ml-worker.js            # Web Worker: Pyodide + sklearn (real Python)
│   ├── grid.js                 # Grid Explorer module
│   └── modules.js              # Cleaner, SQL, Chart, Profiler, Compare
├── .github/
│   └── workflows/
│       └── pages.yml           # Auto-deploy to GitHub Pages on push
├── .gitignore
└── README.md
```

---

## Deploy in 3 steps

### 1. Create a public GitHub repo

Go to [github.com/new](https://github.com/new), name it `dataclean-pro`, set it to **Public**, do NOT add a README.


## How the ML engine works

When the page loads, a Web Worker downloads Pyodide (~15 MB, cached after first load) and installs `numpy`, `pandas`, `scipy`, and `scikit-learn`. Once ready, the badge in the top bar turns green.

All ML calls are async — the UI never blocks. Large datasets (100k+ rows) work fine because everything runs off the main thread.

**First load:** ~20–30 seconds (downloads Python runtime + packages)  
**Subsequent loads:** ~3–5 seconds (cached by browser)

---

## SQL engine

Uses **DuckDB-WASM** for real SQL with a built-in fallback parser for environments where WASM isn't available.

Supported: `SELECT`, `FROM`, `WHERE`, `JOIN`, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`, `OFFSET`, `WITH` (CTEs), `DISTINCT`, `IS NULL`, `IS NOT NULL`, aggregate functions, `AND`/`OR`/`NOT`.

---

## Privacy

- Zero data sent to any server
- All processing happens in your browser tab
- Datasets persist in IndexedDB (your device only)
- No analytics, no tracking, no cookies

---

## License

MIT
