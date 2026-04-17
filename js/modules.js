'use strict';
// ══════════════════════════════════════════════════════════════
//  CLEANER  — real sklearn via Pyodide ML worker
// ══════════════════════════════════════════════════════════════
const Cleaner = (() => {
  let target=null,colCfg={};
  function openFor(name){
    target=name||S.active;
    if(!target||!S.datasets[target]){notify('No dataset selected','err');return;}
    showTab('clean');buildUI();
  }
  function buildUI(){
    if(!target||!S.datasets[target])return;
    const d=S.datasets[target];
    qs('#cln-ds').textContent=target;
    const nulls=d.rows.reduce((s,r)=>s+r.filter(v=>_N(v)).length,0);
    const dupes=d.rows.length-new Set(d.rows.map(r=>r.join('|'))).size;
    const pct=Math.round((1-nulls/(d.rows.length*d.headers.length||1))*100);
    qs('#cln-rows').textContent=d.rows.length.toLocaleString();
    qs('#cln-cols').textContent=d.headers.length;
    qs('#cln-nulls').textContent=`${nulls.toLocaleString()} (${100-pct}%)`;
    qs('#cln-dupes').textContent=dupes;
    qs('#cln-pct').textContent=pct+'%';
    qs('#cln-pct').className='mv '+(pct>=90?'ok':pct>=70?'warn':'bad');
    colCfg={};
    const cc=qs('#col-cards');
    cc.innerHTML=d.headers.map((h,ci)=>{
      const vals=d.rows.map(r=>r[ci]??'');
      const nullC=vals.filter(v=>_N(v)).length;
      const nullP=Math.round(nullC/vals.length*100);
      const nonNull=vals.filter(v=>!_N(v));
      const numV=nonNull.map(v=>parseFloat(v)).filter(v=>!isNaN(v)).sort((a,b)=>a-b);
      const isN=numV.length>=nonNull.length*.7&&nonNull.length>0;
      const mean=isN&&numV.length?+(numV.reduce((a,b)=>a+b)/numV.length).toFixed(3):null;
      const mode=(()=>{const f={};nonNull.forEach(v=>{f[String(v)]=(f[String(v)]||0)+1;});return Object.entries(f).sort((a,b)=>b[1]-a[1])[0]?.[0]||'';})();
      const q1=isN?numV[Math.floor(numV.length*.25)]:null,q3=isN?numV[Math.floor(numV.length*.75)]:null;
      const outliers=(q1&&q3)?numV.filter(v=>v<q1-1.5*(q3-q1)||v>q3+1.5*(q3-q1)).length:0;
      colCfg[h]={missing:'global',outlier:'global',str_transform:'none',custom_fill:''};
      const bc=nullP>30?'#f87171':nullP>10?'#fbbf24':'#22c55e';
      return`<div class="col-card" id="cc${ci}">
        <div class="col-card-hd" onclick="Cleaner.toggle(${ci})">
          <span class="cc-nm">${escH(h)}</span>
          <span class="badge ${isN?'bg-g':'bg-n'}">${isN?'numeric':'text'}</span>
          ${nullC>0?`<span class="badge bg-w">${nullP}% null</span>`:'<span class="badge bg-g">complete</span>'}
          ${outliers>0?`<span class="badge bg-w">${outliers} outliers</span>`:''}
          <span class="ml-auto text-xs" style="color:var(--t3)">${new Set(nonNull.map(v=>String(v).toLowerCase())).size} uniq</span>
        </div>
        <div class="col-card-bd hidden" id="cbd${ci}">
          <div class="cc-stats">${[`Total <b>${vals.length}</b>`,`Null <b>${nullC}</b>`,`Uniq <b>${new Set(nonNull.map(v=>String(v).toLowerCase())).size}</b>`,isN?`Mean <b>${mean??'—'}</b>`:null,`Mode <b>${escH(String(mode).slice(0,12))}</b>`].filter(Boolean).map(s=>`<span>${s}</span>`).join('')}</div>
          <div class="null-trk"><div class="null-fill" style="width:${nullP}%;background:${bc}"></div></div>
          <div class="form-row-2 mt8">
            <div class="fg">
              <label>Missing strategy</label>
              <select onchange="Cleaner.set('${h}','missing',this.value)" id="ms${ci}">
                <option value="global">Global setting</option>
                <option value="knn">KNN (sklearn.impute.KNNImputer)</option>
                <option value="mice">MICE/Iterative (BayesianRidge)</option>
                <option value="random_forest">Random Forest iterative</option>
                <option value="mean"${!isN?' disabled':''}>Mean (${mean??'—'})</option>
                <option value="median"${!isN?' disabled':''}>Median</option>
                <option value="most_frequent">Mode ("${escH(String(mode).slice(0,12))}")</option>
                <option value="ffill">Forward fill</option>
                <option value="bfill">Backward fill</option>
                <option value="zero">Fill 0 / ""</option>
                <option value="custom">Custom value…</option>
                <option value="drop">Drop rows</option>
                <option value="none">Leave as-is</option>
              </select>
            </div>
            <div class="fg">
              <label>Outlier action ${!isN?'(N/A)':''}</label>
              <select onchange="Cleaner.set('${h}','outlier',this.value)"${!isN?' disabled':''}>
                <option value="global">Global setting</option>
                <option value="cap">Winsorize/Cap</option>
                <option value="remove">Remove rows</option>
                <option value="replace_mean">Replace with mean</option>
                <option value="replace_median">Replace with median</option>
                <option value="nan">Set to NaN</option>
                <option value="keep">Keep (flag only)</option>
              </select>
            </div>
          </div>
          <div class="hidden mt4" id="cv${ci}"><div class="fg"><label>Custom fill value</label><input type="text" oninput="Cleaner.set('${h}','custom_fill',this.value)" placeholder="e.g. Unknown"></div></div>
          <div class="fg mt8">
            <label>String transform ${isN?'(N/A)':''}</label>
            <select onchange="Cleaner.set('${h}','str_transform',this.value)"${isN?' disabled':''}>
              <option value="none">None / use global</option>
              <option value="lower">Lowercase</option>
              <option value="upper">Uppercase</option>
              <option value="title">Title Case</option>
              <option value="trim">Trim only</option>
              <option value="strip_special">Strip special chars</option>
            </select>
          </div>
        </div>
      </div>`;
    }).join('');
    // wire custom value toggles
    d.headers.forEach((_,ci)=>{
      const sel=qs(`#ms${ci}`);
      if(sel) sel.addEventListener('change',function(){qs(`#cv${ci}`)?.classList.toggle('hidden',this.value!=='custom');});
    });
  }
  function toggle(ci){qs(`#cbd${ci}`)?.classList.toggle('hidden');}
  function set(col,key,val){if(!colCfg[col])colCfg[col]={};colCfg[col][key]=val;}
  function addLog(t,m){const el=qs('#cln-log');if(!el)return;const d=document.createElement('div');d.className='log-line '+t;d.textContent=m;el.appendChild(d);el.scrollTop=el.scrollHeight;}
  async function run(){
    if(!target||!S.datasets[target]){notify('No dataset selected','err');return;}
    if(!S.mlReady){notify('ML engine still loading — please wait…','err');return;}
    if(S._cleaning){notify('Already cleaning…','err');return;}
    S._cleaning=true;
    const btn=qs('#cln-run');btn.disabled=true;btn.innerHTML='<span class="spinner"></span> Running…';
    qs('#cln-log').innerHTML='';qs('#cln-sum').classList.add('hidden');
    const d=S.datasets[target];
    const cfg={
      ml_model:qs('#cln-model').value,
      missing_strategy:qs('#cln-missing').value,
      duplicates:qs('#cln-dupes').value,
      outlier_method:qs('#cln-omethod').value,
      outlier_action:qs('#cln-oaction').value,
      iso_contamination:parseFloat(qs('#cln-cont')?.value||0.05),
      trim:qs('#cln-trim').checked,
      collapse_spaces:qs('#cln-collapse').checked,
      fix_encoding:qs('#cln-enc').checked,
      coerce_numeric:qs('#cln-num').checked,
      coerce_bool:qs('#cln-bool').checked,
      str_transform:qs('#cln-strtransform').value,
      columns:colCfg,
    };
    try{
      addLog('info',`Dataset: ${target} (${d.rows.length.toLocaleString()} × ${d.headers.length})`);
      addLog('info',`Model: ${cfg.ml_model} | Missing: ${cfg.missing_strategy} | Outlier: ${cfg.outlier_method}`);
      const res=await ml('clean',{rows:d.rows,headers:d.headers,config:cfg});
      if(res.error) throw new Error(res.error);
      res.log.forEach(m=>addLog('ok',m));
      const cleanName=target+'_cleaned';
      dsAdd(cleanName,res.headers,res.rows.map(r=>r.map(v=>v===null?'':String(v))),d.fileName+' (cleaned)');
      const s=res.stats;
      qs('#cln-sum').classList.remove('hidden');
      qs('#cln-badges').innerHTML=[
        `<span class="dbadge">${s.rows_out.toLocaleString()} rows kept</span>`,
        s.rows_removed>0?`<span class="dbadge neg">${s.rows_removed} removed</span>`:'',
        s.nulls_filled>0?`<span class="dbadge">${s.nulls_filled.toLocaleString()} nulls filled</span>`:'',
        s.outliers>0?`<span class="dbadge warn">${s.outliers} outliers handled</span>`:'',
        s.dupes>0?`<span class="dbadge warn">${s.dupes} dupes removed</span>`:'',
        s.str_ops>0?`<span class="dbadge">${s.str_ops.toLocaleString()} strings cleaned</span>`:'',
        s.coerced>0?`<span class="dbadge">${s.coerced} types coerced</span>`:'',
      ].filter(Boolean).join('');
      notify(`Cleaned! Saved as "${cleanName}"`, 'ok', 6000);
    }catch(e){addLog('err','Error: '+e.message);notify('Cleaning failed: '+e.message,'err');}
    S._cleaning=false;btn.disabled=false;btn.innerHTML='<svg width="12" height="12" viewBox="0 0 24 24" fill="currentColor"><polygon points="5 3 19 12 5 21 5 3"/></svg> Run Cleaning';
  }
  function expandAll(){document.querySelectorAll('.col-card-bd').forEach(b=>b.classList.remove('hidden'));}
  function collapseAll(){document.querySelectorAll('.col-card-bd').forEach(b=>b.classList.add('hidden'));}
  return{openFor,buildUI,toggle,set,run,expandAll,collapseAll};
})();
window.Cleaner=Cleaner;

// ══════════════════════════════════════════════════════════════
//  SQL EDITOR
// ══════════════════════════════════════════════════════════════
const SQL = (() => {
  let hist=[];
  function quickQuery(name){qs('#sql-ed').value=`SELECT *\nFROM ${name}\nLIMIT 50`;}
  function insertTable(name){qs('#sql-ed').value=`SELECT *\nFROM ${name}\nLIMIT 50`;qs('#sql-ed').focus();}
  function snippet(s){const el=qs('#sql-ed');const p=el.selectionStart;el.value=el.value.slice(0,p)+s+el.value.slice(el.selectionEnd);el.focus();}
  async function run(){
    const sql=(qs('#sql-ed').value||'').trim();if(!sql)return;
    if(!S.sqlReady){notify('SQL engine loading…','err');return;}
    const btn=qs('#sql-run');btn.disabled=true;btn.innerHTML='<span class="spinner"></span>';
    const t0=performance.now();
    try{
      const res=await sqlRun(sql);
      S.lastSQL=res;
      hist.unshift({sql,rows:res.rows.length,ms:+(performance.now()-t0).toFixed(0),ts:new Date()});
      if(hist.length>30)hist.pop();
      renderResult(res,+(performance.now()-t0).toFixed(0));
      renderHist();
    }catch(e){renderError(e.message);notify('SQL: '+e.message.slice(0,80),'err');}
    btn.disabled=false;btn.innerHTML='<svg width="12" height="12" viewBox="0 0 24 24" fill="currentColor"><polygon points="5 3 19 12 5 21 5 3"/></svg> Run';
  }
  function renderResult({headers,rows},ms){
    const area=qs('#sql-res');
    const lim=Math.min(rows.length,1000);
    area.innerHTML=`<div class="sql-rh">
      <span class="badge bg-g">${rows.length.toLocaleString()} rows</span>
      <span class="badge bg-n">${headers.length} cols</span>
      <span style="font-size:10px;color:var(--t3);font-family:var(--mf)">${ms}ms</span>
      <div class="ml-auto flex gap2">
        <button class="btn btn-sm" onclick="SQL.export('csv')">CSV</button>
        <button class="btn btn-sm" onclick="SQL.export('json')">JSON</button>
        <button class="btn btn-sm" onclick="SQL.saveTable()">Save as Table</button>
        <button class="btn btn-sm" onclick="Chart.useSQL();showTab('chart')">Chart</button>
      </div>
    </div>
    <div class="sql-rb">
      <div class="tbl-wrap" style="border:none;border-radius:0;max-height:calc(100vh - 380px)">
        <table>
          <thead><tr><th class="rn-th">#</th>${headers.map(h=>`<th>${escH(h)}</th>`).join('')}</tr></thead>
          <tbody>${rows.slice(0,lim).map((r,ri)=>`<tr><td class="rn-td">${ri+1}</td>${headers.map((_,ci)=>{const v=r[ci]??'';if(_N(v))return`<td class="null-c">∅</td>`;const n=parseFloat(v);return(!isNaN(n)&&v!==null)?`<td class="num-c">${escH(String(v))}</td>`:`<td>${escH(String(v))}</td>`;}).join('')}</tr>`).join('')}
          ${rows.length>1000?`<tr><td colspan="${headers.length+1}" class="trunc">… ${rows.length-1000} more rows</td></tr>`:''}
          </tbody>
        </table>
      </div>
    </div>`;
  }
  function renderError(msg){qs('#sql-res').innerHTML=`<div class="sql-err"><strong>SQL Error</strong><pre>${escH(msg)}</pre></div>`;}
  function renderHist(){
    const el=qs('#sql-hist');if(!el)return;
    el.innerHTML=hist.slice(0,10).map((h,i)=>`<div class="qh" onclick="SQL.restoreHist(${i})">
      <div class="qh-sql">${escH(h.sql.slice(0,80))}</div>
      <div class="qh-meta">${h.rows} rows · ${h.ms}ms · ${h.ts.toLocaleTimeString()}</div>
    </div>`).join('');
  }
  function restoreHist(i){if(hist[i])qs('#sql-ed').value=hist[i].sql;}
  function exportSQL(fmt){
    if(!S.lastSQL){notify('Run a query first','err');return;}
    const{headers,rows}=S.lastSQL;
    const ts=new Date().toISOString().slice(0,10);
    if(fmt==='csv') dl([headers.join(','),...rows.map(r=>r.map(v=>`"${String(v??'').replace(/"/g,'""')}"`).join(','))].join('\n'),`query_${ts}.csv`,'text/csv');
    else dlJSON(rows.map(r=>Object.fromEntries(headers.map((h,i)=>[h,r[i]]))),`query_${ts}.json`);
    notify(`Exported ${rows.length} rows`,'ok');
  }
  function saveTable(){
    if(!S.lastSQL){notify('Run a query first','err');return;}
    const name=prompt('Save as table name:','query_result');if(!name)return;
    const clean=name.replace(/[^a-zA-Z0-9_]/g,'_');
    dsAdd(clean,S.lastSQL.headers,S.lastSQL.rows.map(r=>r.map(v=>String(v??''))),`query: ${clean}`);
    notify(`Saved as "${clean}"`,'ok');
  }
  function format(){
    let s=qs('#sql-ed').value;
    const kw=['SELECT','DISTINCT','FROM','WHERE','GROUP BY','HAVING','ORDER BY','LIMIT','OFFSET','JOIN','INNER JOIN','LEFT JOIN','RIGHT JOIN','ON','UNION ALL','UNION','WITH','AS','AND','OR','NOT IN','NOT','IN','BETWEEN','LIKE','IS NULL','IS NOT NULL','CASE','WHEN','THEN','ELSE','END','OVER','PARTITION BY'];
    kw.forEach(k=>{s=s.replace(new RegExp(`\\b${k}\\b`,'gi'),'\n'+k);});
    qs('#sql-ed').value=s.replace(/\n{3,}/g,'\n\n').trim();
  }
  function clear(){qs('#sql-ed').value='';qs('#sql-res').innerHTML='<div class="empty-st" style="height:100%"><p>Run a query to see results</p></div>';}
  const _export=exportSQL;
  return{quickQuery,insertTable,snippet,run,export:_export,saveTable,format,clear,restoreHist};
})();
window.SQL=SQL;

// ══════════════════════════════════════════════════════════════
//  CHART BUILDER
// ══════════════════════════════════════════════════════════════
const Chart = (() => {
  let inst=null;
  const PAL=['#22c55e','#60a5fa','#f59e0b','#a78bfa','#f87171','#34d399','#fb923c','#38bdf8','#e879f9','#4ade80'];
  function init(){const v=qs('#sel-chart').value;if(v)populateCols(v);}
  function populateCols(name,colHint){
    if(!name||!S.datasets[name])return;
    const h=S.datasets[name].headers;
    const o='<option value="">Select…</option>'+h.map(c=>`<option value="${c}">${c}</option>`).join('');
    qs('#ch-x').innerHTML=o;qs('#ch-y').innerHTML=o;
    // auto detect
    const numH=h.find(hh=>{const ci=S.datasets[name].headers.indexOf(hh);return S.datasets[name].rows.slice(0,20).filter(r=>!_N(r[ci])).every(r=>!isNaN(parseFloat(r[ci])));});
    const txtH=h.find(hh=>hh!==numH);
    if(txtH)qs('#ch-x').value=txtH;
    if(colHint!=null){const hname=S.datasets[name].headers[colHint];if(hname)qs('#ch-y').value=hname;}
    else if(numH)qs('#ch-y').value=numH;
    build();
  }
  function quick(name,colIdx){
    if(!name||!S.datasets[name])return;
    qs('#sel-chart').value=name;
    populateCols(name,colIdx);
  }
  function setType(t){
    S.chartType=t;
    document.querySelectorAll('.ct-btn').forEach(b=>b.classList.remove('active-btn'));
    qs(`#ct-${t}`)?.classList.add('active-btn');
    build();
  }
  function build(override){
    const name=qs('#sel-chart').value;
    let data=override||(name&&S.datasets[name]?{headers:S.datasets[name].headers,rows:S.datasets[name].rows}:null)||S.lastSQL;
    if(!data){toggleEmpty(true);return;}
    const xCol=qs('#ch-x').value,yCol=qs('#ch-y').value;
    if(!xCol||!yCol){toggleEmpty(true);return;}
    const xi=data.headers.indexOf(xCol),yi=data.headers.indexOf(yCol);
    if(xi<0||yi<0){toggleEmpty(true);return;}
    const maxR=parseInt(qs('#ch-max')?.value||50);
    const agg=qs('#ch-agg').value;
    const sortC=qs('#ch-sort').value;
    let labels=[],values=[],scData=[];
    if(agg!=='none'){
      const groups=new Map();
      data.rows.forEach(r=>{const k=String(r[xi]??'');if(!groups.has(k))groups.set(k,[]);const v=parseFloat(r[yi]??0);if(!isNaN(v))groups.get(k).push(v);});
      let entries=[...groups.entries()];
      if(sortC==='asc')entries.sort((a,b)=>{const f=(vs)=>vs.length?vs.reduce((s,v)=>s+v,0):0;return f(a[1])-f(b[1]);});
      if(sortC==='desc')entries.sort((a,b)=>{const f=(vs)=>vs.length?vs.reduce((s,v)=>s+v,0):0;return f(b[1])-f(a[1]);});
      entries.slice(0,maxR).forEach(([k,vs])=>{
        labels.push(k);
        const s=vs.reduce((a,b)=>a+b,0);
        switch(agg){
          case'sum':values.push(+s.toFixed(4));break;
          case'avg':values.push(vs.length?+(s/vs.length).toFixed(4):0);break;
          case'count':values.push(vs.length);break;
          case'median':{const sv=[...vs].sort((a,b)=>a-b);values.push(sv[Math.floor(sv.length/2)]||0);}break;
          case'min':values.push(vs.length?Math.min(...vs):0);break;
          case'max':values.push(vs.length?Math.max(...vs):0);break;
          default:values.push(vs[0]||0);
        }
      });
    }else if(S.chartType==='scatter'){
      scData=data.rows.slice(0,maxR).map(r=>({x:parseFloat(r[xi])||0,y:parseFloat(r[yi])||0}));
    }else{
      data.rows.slice(0,maxR).forEach(r=>{labels.push(String(r[xi]??''));values.push(parseFloat(r[yi]??0)||0);});
    }
    toggleEmpty(false);
    const canvas=qs('#main-chart');
    const color=qs('#ch-color').value;
    const title=qs('#ch-title').value||`${yCol} by ${xCol}`;
    const isPie=['pie','doughnut','polarArea'].includes(S.chartType);
    if(inst)inst.destroy();
    const bgC=isPie?labels.map((_,i)=>PAL[i%PAL.length]+'cc'):color+'cc';
    const brdC=isPie?labels.map((_,i)=>PAL[i%PAL.length]):color;
    inst=new globalThis.Chart(canvas,{
      type:S.chartType,
      data:{labels:S.chartType==='scatter'?undefined:labels,datasets:[{
        label:yCol,data:S.chartType==='scatter'?scData:values,
        backgroundColor:bgC,borderColor:brdC,
        borderWidth:S.chartType==='line'?2.5:1,
        fill:S.chartType==='line'&&qs('#ch-fill')?.checked,
        tension:S.chartType==='line'?.4:undefined,
        pointRadius:S.chartType==='scatter'?5:S.chartType==='line'?3:undefined,
      }]},
      options:{
        responsive:true,maintainAspectRatio:false,animation:{duration:250},
        plugins:{
          legend:{display:qs('#ch-legend').checked,labels:{color:'#a0a0a0',font:{size:11}}},
          title:{display:!!title,text:title,color:'#efefef',font:{size:13,weight:'600'}},
          tooltip:{backgroundColor:'#1c1c1c',borderColor:'#2e2e2e',borderWidth:1,titleColor:'#efefef',bodyColor:'#999'},
        },
        scales:isPie?{}:{
          x:{display:true,grid:{display:qs('#ch-grid').checked,color:'rgba(255,255,255,0.05)'},ticks:{color:'#555',font:{size:10},maxRotation:45,maxTicksLimit:20}},
          y:{display:true,grid:{display:qs('#ch-grid').checked,color:'rgba(255,255,255,0.05)'},ticks:{color:'#555',font:{size:10}}},
        },
      }
    });
    qs('#ch-info').textContent=`${S.chartType} · ${labels.length||scData.length} points`;
  }
  function toggleEmpty(s){qs('#ch-empty').style.display=s?'flex':'none';qs('#main-chart').style.display=s?'none':'block';}
  function useSQL(){if(!S.lastSQL){notify('Run a query first','err');return;}const d=S.lastSQL;const o='<option value="">Select…</option>'+d.headers.map(h=>`<option value="${h}">${h}</option>`).join('');qs('#ch-x').innerHTML=o;qs('#ch-y').innerHTML=o;if(d.headers[0])qs('#ch-x').value=d.headers[0];if(d.headers[1])qs('#ch-y').value=d.headers[1];build(d);}
  function exportPNG(){if(!inst){notify('Build a chart first','err');return;}const a=Object.assign(document.createElement('a'),{href:qs('#main-chart').toDataURL('image/png',1),download:'chart.png'});a.click();notify('Saved','ok');}
  return{init,populateCols,quick,setType,build,useSQL,exportPNG};
})();
window.Chart=Chart;

// ══════════════════════════════════════════════════════════════
//  PROFILER  — uses real Python/pandas when ML ready
// ══════════════════════════════════════════════════════════════
const Profiler = (() => {
  async function render(){
    const name=qs('#sel-profile').value||S.active;
    if(name)qs('#sel-profile').value=name;
    const el=qs('#profile-content');
    if(!name||!S.datasets[name]){el.innerHTML='<div class="empty-st" style="height:300px"><p>Select a dataset to profile</p></div>';return;}
    el.innerHTML='<div class="loading-st"><span class="spinner"></span> Profiling with Python/pandas…</div>';
    if(S.mlReady){
      try{
        const d=S.datasets[name];
        const res=await ml('profile',{rows:d.rows,headers:d.headers});
        renderML(name,res);return;
      }catch(e){console.warn('ML profile fallback:',e);}
    }
    renderJS(name);
  }
  function metricRow(label,val,cls=''){return`<div class="metric"><div class="ml">${label}</div><div class="mv ${cls}">${val}</div></div>`;}
  function renderML(name,r){
    const cols=r.columns;
    const avgComp=Object.values(cols).reduce((s,c)=>s+c.completeness,0)/Object.keys(cols).length;
    qs('#profile-content').innerHTML=`
      <div class="mrow">
        ${metricRow('Rows',r.rows.toLocaleString())}
        ${metricRow('Columns',r.cols,'info')}
        ${metricRow('Completeness',avgComp.toFixed(1)+'%',avgComp>=90?'ok':avgComp>=70?'warn':'bad')}
        ${metricRow('Duplicates',r.duplicates,r.duplicates>0?'warn':'ok')}
        ${metricRow('Numeric cols',Object.values(cols).filter(c=>c.type==='numeric').length,'info')}
        ${metricRow('Text cols',Object.values(cols).filter(c=>c.type==='text').length)}
      </div>
      <div style="display:flex;gap:8px;margin-bottom:12px;flex-wrap:wrap">
        <button class="btn btn-sm" onclick="Profiler.correlations('${name}')">Correlation Matrix</button>
        <button class="btn btn-sm" onclick="Profiler.pca('${name}')">PCA Analysis</button>
        <button class="btn btn-sm" onclick="Profiler.anomalies('${name}')">Anomaly Detection</button>
        <button class="btn btn-sm ml-auto" onclick="Profiler.exportReport('${name}')">Export CSV Report</button>
      </div>
      <div class="tbl-wrap">
        <table><thead><tr>
          <th>Column</th><th>Type</th><th>Non-null</th><th>Null%</th><th>Unique</th>
          <th>Min</th><th>Max</th><th>Mean</th><th>Median</th><th>Std</th>
          <th>Skew</th><th>Outliers</th><th>Distribution</th>
        </tr></thead><tbody>
        ${Object.entries(cols).map(([h,c])=>{
          const hist=c.histogram?`<svg width="80" height="18">${c.histogram.counts.map((cnt,i)=>{
            const mc=Math.max(...c.histogram.counts);const bh=mc?cnt/mc*16:0;const bw=80/c.histogram.counts.length-1;
            return`<rect x="${i*(bw+1)}" y="${18-bh}" width="${bw}" height="${bh}" fill="#22c55e" opacity=".8"/>`;
          }).join('')}</svg>`:'—';
          return`<tr>
            <td style="font-family:var(--mf);font-weight:500">${escH(h)}</td>
            <td><span class="badge ${c.type==='numeric'?'bg-g':'bg-n'}">${c.type}</span></td>
            <td class="nc">${(c.total-c.null_count).toLocaleString()}</td>
            <td><span class="badge ${c.null_pct>30?'bg-e':c.null_pct>10?'bg-w':'bg-g'}">${c.null_pct}%</span></td>
            <td class="nc">${c.unique.toLocaleString()}</td>
            <td class="nc">${c.min??'—'}</td><td class="nc">${c.max??'—'}</td>
            <td class="nc">${c.mean??'—'}</td><td class="nc">${c.median??'—'}</td><td class="nc">${c.std??'—'}</td>
            <td class="nc ${c.skewness!=null&&Math.abs(c.skewness)>1?'warnc':''}">${c.skewness??'—'}</td>
            <td><span class="badge ${(c.outliers_iqr||0)>0?'bg-w':'bg-n'}">${c.outliers_iqr??'—'}</span></td>
            <td>${hist}</td>
          </tr>`;
        }).join('')}
        </tbody></table>
      </div>
      <div id="corr-out"></div><div id="pca-out"></div><div id="anom-out"></div>`;
  }
  function renderJS(name){
    const d=S.datasets[name];
    const nulls=d.rows.reduce((s,r)=>s+r.filter(v=>_N(v)).length,0);
    const pct=Math.round((1-nulls/(d.rows.length*d.headers.length||1))*100);
    const dupes=d.rows.length-new Set(d.rows.map(r=>r.join('|'))).size;
    const cdata=d.headers.map((h,ci)=>{
      const vals=d.rows.map(r=>r[ci]??'');
      const nullC=vals.filter(v=>_N(v)).length;
      const nonNull=vals.filter(v=>!_N(v));
      const numV=nonNull.map(v=>parseFloat(v)).filter(v=>!isNaN(v)).sort((a,b)=>a-b);
      const isN=numV.length>=nonNull.length*.7&&nonNull.length>0;
      const mean=isN&&numV.length?+(numV.reduce((a,b)=>a+b)/numV.length).toFixed(4):null;
      const med=isN&&numV.length?numV[Math.floor(numV.length/2)]:null;
      const std=mean!=null?+(Math.sqrt(numV.reduce((s,v)=>s+(v-mean)**2,0)/numV.length)).toFixed(4):null;
      const q1=isN?numV[Math.floor(numV.length*.25)]:null,q3=isN?numV[Math.floor(numV.length*.75)]:null;
      const outliers=(q1&&q3)?numV.filter(v=>v<q1-1.5*(q3-q1)||v>q3+1.5*(q3-q1)).length:0;
      return{h,nullC,nullP:Math.round(nullC/vals.length*100),uniq:new Set(nonNull.map(v=>String(v).toLowerCase())).size,isN,min:isN?numV[0]:null,max:isN?numV[numV.length-1]:null,mean,med,std,outliers};
    });
    qs('#profile-content').innerHTML=`
      <div class="mrow">
        ${metricRow('Rows',d.rows.length.toLocaleString())}
        ${metricRow('Columns',d.headers.length,'info')}
        ${metricRow('Completeness',pct+'%',pct>=90?'ok':pct>=70?'warn':'bad')}
        ${metricRow('Duplicates',dupes,dupes>0?'warn':'ok')}
      </div>
      <div style="margin-bottom:10px;font-size:11px;color:var(--t2)">💡 ML engine loading — richer stats (skewness, kurtosis, histograms) available once ready.</div>
      <div style="margin-bottom:10px"><button class="btn btn-sm" onclick="Profiler.exportReport('${name}')">Export CSV Report</button></div>
      <div class="tbl-wrap"><table><thead><tr>
        <th>Column</th><th>Type</th><th>Null%</th><th>Unique</th><th>Min</th><th>Max</th><th>Mean</th><th>Median</th><th>Std</th><th>Outliers</th>
      </tr></thead><tbody>
        ${cdata.map(c=>`<tr>
          <td class="mc">${escH(c.h)}</td>
          <td><span class="badge ${c.isN?'bg-g':'bg-n'}">${c.isN?'numeric':'text'}</span></td>
          <td><span class="badge ${c.nullP>30?'bg-e':c.nullP>10?'bg-w':'bg-g'}">${c.nullP}%</span></td>
          <td class="nc">${c.uniq}</td>
          <td class="nc">${c.min??'—'}</td><td class="nc">${c.max??'—'}</td>
          <td class="nc">${c.mean??'—'}</td><td class="nc">${c.med??'—'}</td>
          <td class="nc">${c.std??'—'}</td>
          <td><span class="badge ${c.outliers>0?'bg-w':'bg-n'}">${c.outliers}</span></td>
        </tr>`).join('')}
      </tbody></table></div>`;
  }
  async function correlations(name){
    if(!S.mlReady){notify('ML engine loading…','err');return;}
    notify('Computing Pearson correlations…','info');
    const d=S.datasets[name];
    const r=await ml('correlations',{rows:d.rows,headers:d.headers});
    if(r.error){notify(r.error,'err');return;}
    const el=qs('#corr-out');
    const maxAbs=Math.max(...r.matrix.flat().map(v=>Math.abs(v)).filter(v=>v<1));
    el.innerHTML=`<div style="margin-top:1.5rem"><div class="sec-lbl">Pearson Correlation Matrix</div>
      <div style="overflow-x:auto"><table style="border-collapse:collapse;font-size:11px">
        <thead><tr><th style="padding:6px 10px;background:var(--s2)"></th>${r.cols.map(c=>`<th style="padding:6px 8px;background:var(--s2);color:var(--t2);max-width:80px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${escH(c)}">${escH(c.slice(0,8))}</th>`).join('')}</tr></thead>
        <tbody>${r.matrix.map((row,ri)=>`<tr><td style="padding:6px 10px;background:var(--s2);font-weight:500">${escH(r.cols[ri])}</td>${row.map((v,ci)=>{
          const ia=maxAbs>0?Math.abs(v)/maxAbs:0;
          const R=ri===ci?200:v>0?Math.round(ia*80):0;
          const G=ri===ci?220:Math.round(ia*(v>0?180:80));
          const B=ri===ci?50:v<0?Math.round(ia*220):0;
          return`<td style="padding:6px 8px;text-align:center;background:rgba(${R},${G},${B},0.35);font-family:var(--mf)">${v.toFixed(3)}</td>`;
        }).join('')}</tr>`).join('')}
        </tbody></table>
      </div></div>`;
  }
  async function pca(name){
    if(!S.mlReady){notify('ML engine loading…','err');return;}
    notify('Running PCA…','info');
    const d=S.datasets[name];
    const r=await ml('pca',{rows:d.rows,headers:d.headers,n_components:3});
    if(r.error){notify(r.error,'err');return;}
    const cumVar=r.explained_variance.reduce((acc,v,i)=>[...acc,(acc[i-1]||0)+v*100],[]);
    qs('#pca-out').innerHTML=`<div style="margin-top:1.5rem"><div class="sec-lbl">PCA — Explained Variance</div>
      <div class="tbl-wrap" style="margin-top:8px"><table><thead><tr><th>Component</th><th>Explained Var %</th><th>Cumulative %</th></tr></thead><tbody>
        ${r.explained_variance.map((v,i)=>`<tr><td>PC${i+1}</td><td class="nc">${(v*100).toFixed(2)}%</td><td class="nc">${cumVar[i].toFixed(2)}%</td></tr>`).join('')}
      </tbody></table></div>
      <div style="margin-top:10px"><div class="sec-lbl">Loadings (PC1)</div>
        <div class="tbl-wrap"><table><thead><tr><th>Feature</th><th>Loading</th></tr></thead><tbody>
          ${r.feature_names.map((f,i)=>`<tr><td class="mc">${escH(f)}</td><td class="nc">${(r.loadings[0][i]).toFixed(4)}</td></tr>`).join('')}
        </tbody></table></div>
      </div>
    </div>`;
    notify('PCA complete','ok');
  }
  async function anomalies(name){
    if(!S.mlReady){notify('ML engine loading…','err');return;}
    const cont=parseFloat(prompt('Contamination ratio (0.01–0.5):','0.05'))||0.05;
    notify(`Running IsolationForest (contamination=${cont})…`,'info');
    const d=S.datasets[name];
    const r=await ml('anomalies',{rows:d.rows,headers:d.headers,contamination:cont});
    if(r.error){notify(r.error,'err');return;}
    const flaggedRows=d.rows.map((row,i)=>[...row,r.flags[i].is_anomaly?'1':'0',String(r.flags[i].score)]);
    dsAdd(name+'_anomaly_flags',[...d.headers,'_is_anomaly','_anomaly_score'],flaggedRows,d.fileName);
    notify(`${r.n_anomalies} anomalies found. Saved as "${name}_anomaly_flags"`,'ok',6000);
  }
  function exportReport(name){
    if(!name||!S.datasets[name]){notify('No dataset','err');return;}
    const d=S.datasets[name];
    const lines=[`DataClean Pro Profile Report — ${name}`,`Generated: ${new Date().toISOString()}`,`Rows: ${d.rows.length} | Columns: ${d.headers.length}`,``,`Column,Type,Non-null,Null%,Unique,Min,Max,Mean,Median,Std,Outliers`];
    d.headers.forEach((h,ci)=>{
      const vals=d.rows.map(r=>r[ci]??'');
      const nullC=vals.filter(v=>_N(v)).length;
      const nonNull=vals.filter(v=>!_N(v));
      const numV=nonNull.map(v=>parseFloat(v)).filter(v=>!isNaN(v));
      const isN=numV.length>=nonNull.length*.7;
      const sorted=[...numV].sort((a,b)=>a-b);
      const mean=isN&&numV.length?+(numV.reduce((a,b)=>a+b)/numV.length).toFixed(4):'';
      const med=isN&&sorted.length?sorted[Math.floor(sorted.length/2)]:'';
      const std=mean&&isN?+(Math.sqrt(numV.reduce((s,v)=>s+(v-mean)**2,0)/numV.length)).toFixed(4):'';
      const q1=isN?sorted[Math.floor(sorted.length*.25)]:null,q3=isN?sorted[Math.floor(sorted.length*.75)]:null;
      const outliers=(q1&&q3)?numV.filter(v=>v<q1-1.5*(q3-q1)||v>q3+1.5*(q3-q1)).length:0;
      lines.push(`"${h}",${isN?'numeric':'text'},${vals.length-nullC},${Math.round(nullC/vals.length*100)}%,${new Set(nonNull).size},${isN?Math.min(...numV):''},${isN?Math.max(...numV):''},${mean},${med},${std},${outliers}`);
    });
    dl(lines.join('\n'),`${name}_profile.csv`,'text/csv');
    notify('Profile exported','ok');
  }
  return{render,correlations,pca,anomalies,exportReport};
})();
window.Profiler=Profiler;

// ══════════════════════════════════════════════════════════════
//  COMPARE
// ══════════════════════════════════════════════════════════════
const Compare = (() => {
  function init(){
    const names=dsNames();
    const o='<option value="">Select…</option>'+names.map(n=>`<option value="${n}">${n}</option>`).join('');
    ['#sel-cmp-a','#sel-cmp-b'].forEach(id=>{const el=qs(id);if(!el)return;const p=el.value;el.innerHTML=o;if(names.includes(p))el.value=p;});
  }
  function run(){
    const nA=qs('#sel-cmp-a').value,nB=qs('#sel-cmp-b').value;
    const body=qs('#cmp-body');
    if(!nA||!nB||nA===nB){body.innerHTML='<div class="empty-st" style="height:200px"><p>Select two different datasets</p></div>';return;}
    const a=S.datasets[nA],b=S.datasets[nB];
    const common=a.headers.filter(h=>b.headers.includes(h));
    const onlyA=a.headers.filter(h=>!b.headers.includes(h));
    const onlyB=b.headers.filter(h=>!a.headers.includes(h));
    const statsRows=common.map(h=>{
      const aci=a.headers.indexOf(h),bci=b.headers.indexOf(h);
      const aV=a.rows.map(r=>r[aci]??''),bV=b.rows.map(r=>r[bci]??'');
      const aN=Math.round(aV.filter(v=>_N(v)).length/aV.length*100);
      const bN=Math.round(bV.filter(v=>_N(v)).length/bV.length*100);
      const aNums=aV.filter(v=>!_N(v)).map(v=>parseFloat(v)).filter(v=>!isNaN(v));
      const bNums=bV.filter(v=>!_N(v)).map(v=>parseFloat(v)).filter(v=>!isNaN(v));
      const aM=aNums.length?+(aNums.reduce((a,b)=>a+b)/aNums.length).toFixed(4):null;
      const bM=bNums.length?+(bNums.reduce((a,b)=>a+b)/bNums.length).toFixed(4):null;
      const diff=aM!=null&&bM!=null?+(aM-bM).toFixed(4):null;
      return{h,aN,bN,aM,bM,diff};
    });
    function miniTbl(headers,rows){return`<table><thead><tr>${headers.map(h=>`<th>${escH(h)}</th>`).join('')}</tr></thead><tbody>${rows.map(r=>`<tr>${headers.map((_,ci)=>{const v=r[ci]??'';return _N(v)?`<td class="null-c">∅</td>`:`<td>${escH(String(v))}</td>`;}).join('')}</tr>`).join('')}</tbody></table>`;}
    body.innerHTML=`
      <div class="cmp2">
        <div class="cmp-half" style="border-color:rgba(34,197,94,.3)"><div style="color:var(--acl);font-weight:500">A: ${nA}</div><div class="t2 xs">${a.rows.length.toLocaleString()} rows · ${a.headers.length} cols</div></div>
        <div class="cmp-half" style="border-color:rgba(96,165,250,.3)"><div style="color:var(--il);font-weight:500">B: ${nB}</div><div class="t2 xs">${b.rows.length.toLocaleString()} rows · ${b.headers.length} cols</div></div>
      </div>
      <div class="mrow">
        <div class="metric"><div class="ml">Row Δ</div><div class="mv ${a.rows.length===b.rows.length?'ok':'warn'}">${a.rows.length>b.rows.length?'+':''}${a.rows.length-b.rows.length}</div></div>
        <div class="metric"><div class="ml">Col Δ</div><div class="mv ${a.headers.length===b.headers.length?'ok':'warn'}">${a.headers.length>=b.headers.length?'+':''}${a.headers.length-b.headers.length}</div></div>
        <div class="metric"><div class="ml">Shared Cols</div><div class="mv info">${common.length}</div></div>
        <div class="metric"><div class="ml">Only in A</div><div class="mv ${onlyA.length?'warn':'ok'}">${onlyA.length}</div></div>
        <div class="metric"><div class="ml">Only in B</div><div class="mv ${onlyB.length?'warn':'ok'}">${onlyB.length}</div></div>
      </div>
      ${onlyA.length||onlyB.length?`<div class="card" style="margin-bottom:12px"><div class="card-h"><div class="card-title">Column Differences</div></div><div class="card-b">
        ${onlyA.length?`<div style="margin-bottom:6px"><span class="t2 xs">Only in A: </span>${onlyA.map(h=>`<span class="badge bg-w" style="margin-right:4px">${escH(h)}</span>`).join('')}</div>`:''}
        ${onlyB.length?`<div><span class="t2 xs">Only in B: </span>${onlyB.map(h=>`<span class="badge bg-i" style="margin-right:4px">${escH(h)}</span>`).join('')}</div>`:''}
      </div></div>`:''}
      <div class="card" style="margin-bottom:12px">
        <div class="card-h"><div class="card-title">Shared Column Stats</div></div>
        <div class="tbl-wrap" style="border:none;border-radius:0"><table><thead><tr><th>Column</th><th>A Null%</th><th>B Null%</th><th>A Mean</th><th>B Mean</th><th>Δ Mean</th></tr></thead>
        <tbody>${statsRows.map(r=>`<tr>
          <td class="mc">${escH(r.h)}</td>
          <td class="nc ${r.aN>10?'warnc':''}">${r.aN}%</td><td class="nc ${r.bN>10?'warnc':''}">${r.bN}%</td>
          <td class="nc" style="color:var(--acl)">${r.aM??'—'}</td>
          <td class="nc" style="color:var(--il)">${r.bM??'—'}</td>
          <td class="nc ${r.diff!==null&&Math.abs(r.diff)>0.01?'warnc':''}">${r.diff??'—'}</td>
        </tr>`).join('')}</tbody></table></div>
      </div>
      <div class="cmp2">
        <div class="card"><div class="card-h" style="background:rgba(34,197,94,.06)"><div class="card-title xs" style="color:var(--acl)">A — first 10 rows</div></div><div class="tbl-wrap" style="border:none;max-height:260px;border-radius:0">${miniTbl(a.headers,a.rows.slice(0,10))}</div></div>
        <div class="card"><div class="card-h" style="background:rgba(96,165,250,.06)"><div class="card-title xs" style="color:var(--il)">B — first 10 rows</div></div><div class="tbl-wrap" style="border:none;max-height:260px;border-radius:0">${miniTbl(b.headers,b.rows.slice(0,10))}</div></div>
      </div>`;
  }
  function exportReport(){
    const nA=qs('#sel-cmp-a').value,nB=qs('#sel-cmp-b').value;
    if(!nA||!nB){notify('Select two datasets','err');return;}
    const a=S.datasets[nA],b=S.datasets[nB];
    const lines=[`DataClean Pro Compare Report`,`Generated: ${new Date().toISOString()}`,`A: ${nA} — ${a.rows.length} rows × ${a.headers.length} cols`,`B: ${nB} — ${b.rows.length} rows × ${b.headers.length} cols`,`Only in A: ${a.headers.filter(h=>!b.headers.includes(h)).join(', ')||'none'}`,`Only in B: ${b.headers.filter(h=>!a.headers.includes(h)).join(', ')||'none'}`];
    dl(lines.join('\n'),'compare_report.txt');notify('Exported','ok');
  }
  return{init,run,exportReport};
})();
window.Compare=Compare;

// Keyboard shortcut: Ctrl+Enter in SQL
document.addEventListener('keydown',e=>{
  if((e.ctrlKey||e.metaKey)&&e.key==='Enter'){
    const p=qs('#p-sql');if(p?.classList.contains('active')){e.preventDefault();SQL.run();}
  }
});
