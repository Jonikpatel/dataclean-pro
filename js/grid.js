'use strict';
const Grid = (() => {
  let ds=null,filtered=[],sc=null,sd='asc',filters={},hidden=new Set(),frozen=0,selCol=null,statsOpen=false;

  const isNum=(col)=>filtered.slice(0,30).filter(r=>!_N(r[col])).length>0&&filtered.slice(0,30).filter(r=>!_N(r[col])).every(r=>!isNaN(parseFloat(r[col])));

  function setDs(name){
    if(!name||!S.datasets[name])return;
    ds=name;filters={};sc=null;sd='asc';hidden.clear();selCol=null;
    qs('#sel-grid').value=name;
    S.active=name;_sidebarRender();render();
  }
  function render(){
    if(!ds||!S.datasets[ds]){qs('#grid-empty').style.display='flex';qs('#grid-body').classList.add('hidden');qs('#grid-rc').textContent='—';return;}
    applyFilter();renderTable();
  }
  function applyFilter(){
    const d=S.datasets[ds];
    const srch=(qs('#grid-srch')||{}).value?.toLowerCase()||'';
    let rows=[...d.rows];
    const fe=Object.entries(filters).filter(([,v])=>v);
    if(fe.length) rows=rows.filter(r=>fe.every(([col,fv])=>{
      const ci=d.headers.indexOf(col);if(ci<0)return true;
      const cv=String(r[ci]??'').toLowerCase(),f=fv.toLowerCase().trim();
      if(f==='null'||f==='empty') return _N(r[ci]);
      if(f==='!null'||f==='!empty') return !_N(r[ci]);
      if(f.startsWith('>=')) return parseFloat(cv)>=parseFloat(f.slice(2));
      if(f.startsWith('<=')) return parseFloat(cv)<=parseFloat(f.slice(2));
      if(f.startsWith('>')) return parseFloat(cv)>parseFloat(f.slice(1));
      if(f.startsWith('<')) return parseFloat(cv)<parseFloat(f.slice(1));
      if(f.startsWith('!=')) return !cv.includes(f.slice(2));
      if(f.startsWith('=')) return cv===f.slice(1);
      return cv.includes(f);
    }));
    if(srch) rows=rows.filter(r=>r.some(v=>String(v??'').toLowerCase().includes(srch)));
    if(sc!==null){
      const ci=sc;
      rows=[...rows].sort((a,b)=>{
        const av=a[ci]??'',bv=b[ci]??'';
        const an=parseFloat(av),bn=parseFloat(bv);
        const cmp=(!isNaN(an)&&!isNaN(bn)&&av!==''&&bv!=='')?an-bn:String(av).localeCompare(String(bv),undefined,{numeric:true});
        return sd==='asc'?cmp:-cmp;
      });
    }
    filtered=rows;
    qs('#grid-rc').textContent=`${filtered.length.toLocaleString()} / ${S.datasets[ds].rows.length.toLocaleString()} rows`;
  }
  function renderTable(){
    const d=S.datasets[ds];
    qs('#grid-empty').style.display='none';
    qs('#grid-body').classList.remove('hidden');
    const vis=d.headers.map((h,i)=>({h,i})).filter(({i})=>!hidden.has(i));
    const numIdx=d.headers.map((_,ci)=>isNum(ci));
    const lim=Math.min(filtered.length,2000);
    let html=`<table class="g-tbl"><thead><tr><th class="rn-th" rowspan="2">#</th>`;
    vis.forEach(({h,i})=>{
      const fr=i<frozen?'fr':'';
      const iss=sc===i;
      html+=`<th class="g-th ${fr}" data-ci="${i}"><div class="g-th-in" onclick="Grid.sort(${i})">
        <span class="g-th-nm" title="${escH(h)}">${escH(h)}</span>
        <span class="g-sort ${iss?sd:''}">${iss?(sd==='asc'?'↑':'↓'):'↕'}</span>
        <div class="g-th-acts">
          <button onclick="event.stopPropagation();Grid.colStats(${i})" title="Stats" class="g-act">≡</button>
          <button onclick="event.stopPropagation();Grid.hideCol(${i})" title="Hide" class="g-act">×</button>
        </div>
      </div></th>`;
    });
    html+=`</tr><tr class="flt-row">`;
    vis.forEach(({h,i})=>{
      html+=`<th class="flt-th"><input class="flt-in" placeholder="filter…" value="${escH(filters[h]||'')}" data-h="${escH(h)}" oninput="Grid.setFilter('${escH(h)}',this.value)" title="Ops: = != > < >= <= null !null"></th>`;
    });
    html+=`</tr></thead><tbody>`;
    for(let ri=0;ri<lim;ri++){
      const r=filtered[ri];
      html+=`<tr><td class="rn-td">${ri+1}</td>`;
      vis.forEach(({i})=>{
        const v=r[i]??'';const fr=i<frozen?'fr':'';
        if(_N(v)) html+=`<td class="gc null-c ${fr}">∅</td>`;
        else if(numIdx[i]) html+=`<td class="gc num-c ${fr}">${escH(String(v))}</td>`;
        else html+=`<td class="gc ${fr}">${escH(String(v))}</td>`;
      });
      html+=`</tr>`;
    }
    if(filtered.length>2000) html+=`<tr><td colspan="${vis.length+1}" class="trunc">Showing first 2,000 of ${filtered.length.toLocaleString()} — use filters to narrow down</td></tr>`;
    html+=`</tbody></table>`;
    qs('#grid-body').innerHTML=html;
    if(statsOpen&&selCol!==null) renderStats(selCol);
  }
  function sort(ci){sc===ci?(sd=sd==='asc'?'desc':'asc'):(sc=ci,sd='asc');render();}
  function setFilter(h,v){filters[h]=v;render();}
  function clearFilters(){filters={};qs('#grid-srch').value='';document.querySelectorAll('.flt-in').forEach(e=>e.value='');render();}
  function hideCol(ci){hidden.add(ci);render();}
  function toggleFreeze(){frozen=frozen?0:1;qs('#btn-freeze').classList.toggle('active-btn',!!frozen);render();}
  function toggleStats(){statsOpen=!statsOpen;qs('#stat-panel').classList.toggle('hidden',!statsOpen);qs('#btn-stats').classList.toggle('active-btn',statsOpen);if(statsOpen&&selCol!==null)renderStats(selCol);}
  function colStats(ci){statsOpen=true;selCol=ci;qs('#stat-panel').classList.remove('hidden');qs('#btn-stats').classList.add('active-btn');renderStats(ci);}
  function renderStats(ci){
    if(!ds||!S.datasets[ds])return;
    const d=S.datasets[ds];const h=d.headers[ci];
    const vals=filtered.map(r=>r[ci]??'');
    const nonNull=vals.filter(v=>!_N(v));
    const nullC=vals.length-nonNull.length;
    const numV=nonNull.map(v=>parseFloat(v)).filter(v=>!isNaN(v)).sort((a,b)=>a-b);
    const isN=numV.length>=nonNull.length*.7&&nonNull.length>0;
    const uniq=new Set(nonNull.map(v=>String(v).toLowerCase())).size;
    let html=`<div class="sc-name" title="${escH(h)}">${escH(h)}</div>
      <div class="sc-type">${isN?'numeric':'text'}</div>
      <div class="sc-rows">
        <div class="sc-row"><span>Total</span><b>${vals.length.toLocaleString()}</b></div>
        <div class="sc-row"><span>Non-null</span><b>${nonNull.length.toLocaleString()}</b></div>
        <div class="sc-row ${nullC>0?'warn':''}"><span>Null</span><b>${nullC} (${Math.round(nullC/vals.length*100)}%)</b></div>
        <div class="sc-row"><span>Unique</span><b>${uniq.toLocaleString()}</b></div>`;
    if(isN&&numV.length){
      const mean=numV.reduce((a,b)=>a+b)/numV.length;
      const q1=numV[Math.floor(numV.length*.25)],q3=numV[Math.floor(numV.length*.75)];
      const iqr=q3-q1;const outliers=numV.filter(v=>v<q1-1.5*iqr||v>q3+1.5*iqr).length;
      const std=Math.sqrt(numV.reduce((s,v)=>s+(v-mean)**2,0)/numV.length);
      const skew=numV.length>=3?numV.reduce((s,v)=>s+((v-mean)/std)**3,0)/numV.length:0;
      html+=`<div class="sc-row"><span>Min</span><b>${nFmt(numV[0])}</b></div>
        <div class="sc-row"><span>Max</span><b>${nFmt(numV[numV.length-1])}</b></div>
        <div class="sc-row"><span>Mean</span><b>${nFmt(mean)}</b></div>
        <div class="sc-row"><span>Median</span><b>${nFmt(numV[Math.floor(numV.length/2)])}</b></div>
        <div class="sc-row"><span>Std</span><b>${nFmt(std)}</b></div>
        <div class="sc-row"><span>Q1</span><b>${nFmt(q1)}</b></div>
        <div class="sc-row"><span>Q3</span><b>${nFmt(q3)}</b></div>
        <div class="sc-row ${outliers>0?'warn':''}"><span>Outliers</span><b>${outliers}</b></div>
        <div class="sc-row"><span>Sum</span><b>${nFmt(numV.reduce((a,b)=>a+b,0))}</b></div>`;
      // mini histogram SVG
      const bins=10,mn=numV[0],mx=numV[numV.length-1],rng=mx-mn||1;
      const counts=new Array(bins).fill(0);
      numV.forEach(v=>{const b=Math.min(bins-1,Math.floor((v-mn)/rng*bins));counts[b]++;});
      const mc=Math.max(...counts);
      html+=`</div><svg width="160" height="40" style="display:block;margin:8px auto">${counts.map((c,i)=>{
        const bh=mc?(c/mc)*36:0,bw=160/bins-1;
        return`<rect x="${i*(bw+1)}" y="${40-bh}" width="${bw}" height="${bh}" fill="#22c55e" opacity=".85"/>`;
      }).join('')}</svg><div class="sc-rows">`;
    } else {
      const freq={};nonNull.forEach(v=>{const k=String(v).slice(0,20);freq[k]=(freq[k]||0)+1;});
      const top=Object.entries(freq).sort((a,b)=>b[1]-a[1]).slice(0,8);
      html+=`</div><div class="sc-top-lbl">Top Values</div>`;
      top.forEach(([v,c])=>{
        const pct=Math.round(c/nonNull.length*100);
        html+=`<div class="sc-bar-row"><span class="sc-bar-lbl" title="${escH(v)}">${escH(v.slice(0,14))}</span><div class="sc-bar-trk"><div class="sc-bar-fill" style="width:${pct}%"></div></div><span>${c}</span></div>`;
      });
      html+=`<div class="sc-rows">`;
    }
    html+=`</div><div class="sc-acts">
      <button class="btn btn-xs" onclick="Chart.quick('${ds}','${ci}');showTab('chart')">Chart</button>
      <button class="btn btn-xs" onclick="Grid.copyStats(${ci})">Copy</button>
    </div>`;
    qs('#stat-panel').innerHTML=html;
  }
  function copyStats(ci){
    if(!ds)return;
    const d=S.datasets[ds];const h=d.headers[ci];
    const vals=filtered.map(r=>r[ci]??'');
    const nonNull=vals.filter(v=>!_N(v));
    const numV=nonNull.map(v=>parseFloat(v)).filter(v=>!isNaN(v));
    const isN=numV.length>=nonNull.length*.7;
    let t=`Column: ${h}\nType: ${isN?'numeric':'text'}\nTotal: ${vals.length}\nNon-null: ${nonNull.length}\nNull: ${vals.length-nonNull.length}`;
    if(isN&&numV.length){const mean=numV.reduce((a,b)=>a+b)/numV.length;t+=`\nMin:${Math.min(...numV)}\nMax:${Math.max(...numV)}\nMean:${+mean.toFixed(4)}\nMedian:${numV.sort((a,b)=>a-b)[Math.floor(numV.length/2)]}`;}
    navigator.clipboard?.writeText(t).then(()=>notify('Stats copied','ok'));
  }
  function openColVis(){
    if(!ds)return;
    const d=S.datasets[ds];
    const m=qs('#col-vis-modal');
    m.innerHTML=`<div class="cv-head">Columns <button class="btn btn-xs" onclick="qs('#col-vis-modal').style.display='none'">×</button></div>
      <div style="display:flex;gap:6px;margin-bottom:8px"><button class="btn btn-xs" onclick="Grid.showAll()">All</button><button class="btn btn-xs" onclick="Grid.hideAll()">None</button></div>`+
      d.headers.map((h,i)=>`<label class="cv-item"><input type="checkbox" ${!hidden.has(i)?'checked':''} onchange="Grid.toggleColVis(${i})">${escH(h)}</label>`).join('');
    m.style.display=m.style.display==='none'?'block':'none';
  }
  function toggleColVis(ci){hidden.has(ci)?hidden.delete(ci):hidden.add(ci);render();}
  function showAll(){hidden.clear();render();}
  function hideAll(){if(ds)S.datasets[ds].headers.forEach((_,i)=>hidden.add(i));render();}
  function exportView(fmt='csv'){
    if(!ds){notify('Select a dataset','err');return;}
    applyFilter();
    const d=S.datasets[ds];
    const vis=d.headers.map((h,i)=>({h,i})).filter(({i})=>!hidden.has(i));
    const heads=vis.map(({h})=>h);
    const rows=filtered.map(r=>vis.map(({i})=>r[i]));
    if(fmt==='csv'){dl([heads.join(','),...rows.map(r=>r.map(v=>`"${String(v??'').replace(/"/g,'""')}"`).join(','))].join('\n'),ds+'_export.csv','text/csv');}
    else dlJSON(rows.map(r=>Object.fromEntries(heads.map((h,i)=>[h,r[i]]))),ds+'_export.json');
    notify(`Exported ${rows.length.toLocaleString()} rows`,'ok');
  }
  return {render,setDs,sort,setFilter,clearFilters,hideCol,toggleFreeze,toggleStats,colStats,copyStats,openColVis,toggleColVis,showAll,hideAll,exportView};
})();
window.Grid=Grid;
