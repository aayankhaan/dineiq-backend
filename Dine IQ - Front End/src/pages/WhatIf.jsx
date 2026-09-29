import { useState } from 'react';
import { api } from '../api/client.js';
import { useApi } from '../hooks.js';
import { Card, DataTable, Estimate, Field, Insights, money, num, signed } from '../components/ui.jsx';

const SCEN = {
  price: { label: 'Change menu price', unit: '%', def: 5, hint: 'Positive raises the price' },
  discount: { label: 'Change discount percentage', unit: '%', def: 10, hint: 'Discount applied to this item' },
  promo_freq: { label: 'Increase promotion frequency', unit: '% more promotion-active days', def: 25 },
  remove: { label: 'Remove the menu item', unit: '', def: 0 },
  prep: { label: 'Change preparation quantity', unit: '%', def: -10, hint: 'Negative reduces preparation' },
  demand: { label: 'Change predicted demand', unit: '%', def: 10 },
  waste: { label: 'Change wastage assumption', unit: '% wastage', def: 5, hint: 'New wastage rate to assume' },
};
export default function WhatIf() {
  const meta = useApi('/meta/filters', {}, { filtered: false });
  const [f, setF] = useState({ scenario: 'price', itemId: '', value: SCEN.price.def });
  const [res, setRes] = useState(null); const [err, setErr] = useState(''); const [busy, setBusy] = useState(false);
  const s = SCEN[f.scenario];
  const run = async () => {
    setBusy(true); setErr(''); setRes(null);
    try { setRes(await api('/whatif', { method: 'POST', body: { ...f, itemId: f.itemId || meta.data.items[0].id } })); } catch (e) { setErr(e.message); }
    setBusy(false);
  };
  const rows = res ? [['revenue', 'Revenue', money], ['contribution', 'Contribution margin', money], ['demand', 'Demand (units)', num], ['wastage', 'Wastage cost', money], ['profit', 'Profit after wastage', money]].map(([k, label, fmt]) => ({
    id: k, label, base: fmt(res.baseline[k]), sim: fmt(res.simulated[k]), delta: res.baseline[k] ? ((res.simulated[k] - res.baseline[k]) / Math.abs(res.baseline[k])) * 100 : 0, raw: res.simulated[k] - res.baseline[k], k })) : [];
  return (
    <div className="stack">
      <Card title="Build a scenario" sub="Change one thing and see the estimated effect on a single menu item">
        <div className="grid g3">
          <Field label="Scenario"><select className="input" value={f.scenario} onChange={(e) => setF({ ...f, scenario: e.target.value, value: SCEN[e.target.value].def })}>{Object.entries(SCEN).map(([k, v]) => <option key={k} value={k}>{v.label}</option>)}</select></Field>
          <Field label="Menu item"><select className="input" value={f.itemId} onChange={(e) => setF({ ...f, itemId: e.target.value })}><option value="">First item</option>{(meta.data?.items || []).map((i) => <option key={i.id} value={i.id}>{i.name}</option>)}</select></Field>
          {f.scenario !== 'remove' && <Field label={`Value (${s.unit})${s.hint ? ' – ' + s.hint : ''}`}><input className="input" type="number" value={f.value} onChange={(e) => setF({ ...f, value: e.target.value })} /></Field>}
        </div>
        <div className="row" style={{ marginTop: 14 }}><button className="btn primary" onClick={run} disabled={busy || !meta.data}>{busy ? 'Running…' : 'Run scenario'}</button></div>
        {err && <div className="state err" role="alert" style={{ marginTop: 12 }}>{err}</div>}
      </Card>
      {res && (<>
        <Insights items={[`Estimated profit after wastage moves from ${money(res.baseline.profit)} to ${money(res.simulated.profit)} per month (${signed(rows.find((r) => r.k === 'profit').delta)}).`, ...res.notes]} />
        <Card title={res.item} sub={res.period} actions={<Estimate />}>
          <DataTable rows={rows} columns={[{ key: 'label', label: 'Indicator' }, { key: 'base', label: 'Current', num: true }, { key: 'sim', label: 'Simulated (estimate)', num: true }, { key: 'delta', label: 'Change', num: true, render: (v, r) => <span className={(r.k === 'wastage' ? -v : v) >= 0 ? 'up' : 'down'}>{signed(v)}</span> }]} />
          <p className="small muted" style={{ marginBottom: 0 }}>These are model-based estimates, not actual results. Real outcomes depend on customer behaviour.</p>
        </Card>
      </>)}
    </div>
  );
}
