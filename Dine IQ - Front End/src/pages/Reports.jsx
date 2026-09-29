import { useState } from 'react';
import { download } from '../api/client.js';
import { useAuth } from '../context/AuthContext.jsx';
import { useFilters } from '../context/FiltersContext.jsx';
import { Card, Insights } from '../components/ui.jsx';

const REPORTS = [
  ['menu-performance', 'Menu performance', 'Every item with class, margin, wastage and trend'], ['profitability', 'Profitability', 'Revenue, cost, contribution margin and profit'],
  ['customer-segmentation', 'Customer segmentation', 'Segments, RFM and strategy'], ['market-basket', 'Market-basket analysis', 'Support, confidence and lift'],
  ['demand-forecast', 'Demand forecast', 'History, forecast and range'], ['wastage', 'Wastage', 'By item, location and reason'],
  ['promotions', 'Promotions', 'Effectiveness and trap signals'], ['pricing', 'Pricing', 'Price sensitivity and elasticity'],
  ['location-performance', 'Location performance', 'Standardised location KPIs'], ['anomalies', 'Anomalies', 'Sales and rating anomalies'],
  ['peak-period', 'Peak periods', 'Orders by hour, weekday and location peaks'], ['recommendations', 'Recommendations', 'Actions, priority and evidence'], ['model-comparison', 'Spark vs Python comparison', 'Metrics for the two models in use'],
];
export default function Reports() {
  const { canExport } = useAuth(); const { query } = useFilters();
  const [busy, setBusy] = useState(''); const [err, setErr] = useState('');
  const go = async (k, name, fmt) => { setBusy(k + fmt); setErr(''); try { await download(k, `${k}_${new Date().toISOString().slice(0, 10)}`, fmt, query); } catch (e) { setErr(e.message); } setBusy(''); };
  return (
    <div className="stack">
      <Insights items={[canExport ? 'Reports use the filters currently set in the top bar. Downloads are recorded in the audit trail.' : 'Your role can view reports but not export data. Ask an administrator for export access.']} />
      {err && <div className="state err" role="alert">{err}</div>}
      <div className="grid g3">
        {REPORTS.map(([k, name, desc]) => (
          <Card key={k} title={name} sub={desc}>
            <div className="row"><button className="btn sm" disabled={!canExport || busy === k + 'csv'} onClick={() => go(k, name, 'csv')}>{busy === k + 'csv' ? 'Preparing…' : 'Download CSV'}</button>
              <button className="btn sm" disabled={!canExport || busy === k + 'xlsx'} onClick={() => go(k, name, 'xlsx')}>{busy === k + 'xlsx' ? 'Preparing…' : 'Download Excel'}</button></div>
          </Card>
        ))}
      </div>
    </div>
  );
}
