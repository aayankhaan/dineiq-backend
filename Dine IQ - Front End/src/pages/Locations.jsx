import { useState } from 'react';
import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import { useApi } from '../hooks.js';
import { Async, Card, DataTable, Insights, Tabs, chartTip, money, num, pct } from '../components/ui.jsx';

const METRICS = [['revenue', 'Revenue'], ['profit', 'Profit'], ['aov', 'Average order value'], ['customers', 'Customers'], ['repeatRate', 'Repeat rate'], ['wastePct', 'Wastage %'], ['rating', 'Rating'], ['promoEffectiveness', 'Effective promotions %']];
export default function Locations() {
  const [tab, setTab] = useState('loc'); const [metric, setMetric] = useState('revenue');
  const l = useApi('/locations'); const c = useApi('/channels');
  return (
    <div className="stack">
      <Tabs value={tab} onChange={setTab} tabs={[['loc', 'Locations'], ['ch', 'Ordering channels']]} />
      {tab === 'loc' && <Async state={l}>{(rows) => {
        const best = [...rows].sort((a, b) => b.health - a.health)[0]; const worst = [...rows].sort((a, b) => a.health - b.health)[0];
        return (<>
          <Insights items={[best && `${best.name} scores highest on overall health (${best.health}/100); ${worst.name} is lowest (${worst.health}/100) – wastage ${pct(worst.wastePct)}, rating ${worst.rating}.`]} />
          <Card title="Compare locations" actions={<select className="input" value={metric} onChange={(e) => setMetric(e.target.value)} aria-label="Metric">{METRICS.map(([k, v]) => <option key={k} value={k}>{v}</option>)}</select>}>
            <ResponsiveContainer width="100%" height={280}><BarChart data={rows}><CartesianGrid stroke="#e6ebe8" vertical={false} /><XAxis dataKey="name" tick={{ fontSize: 12 }} /><YAxis width={56} /><Tooltip {...chartTip} /><Bar dataKey={metric} fill="#2e7d5b" /></BarChart></ResponsiveContainer>
          </Card>
          <Card title="Location scorecard"><DataTable rows={rows} defaultSort={{ key: 'health', dir: 'desc' }} columns={[{ key: 'name', label: 'Location' }, { key: 'health', label: 'Health', num: true }, { key: 'revenue', label: 'Revenue', num: true, render: (v) => money(v) }, { key: 'profit', label: 'Profit', num: true, render: (v) => money(v) }, { key: 'aov', label: 'AOV', num: true, render: (v) => money(v, 2) }, { key: 'customers', label: 'Customers', num: true, render: num }, { key: 'repeatRate', label: 'Repeat', num: true, render: (v) => pct(v * 100, 0) }, { key: 'wastePct', label: 'Waste', num: true, render: pct }, { key: 'rating', label: 'Rating', num: true }, { key: 'promoEffectiveness', label: 'Effective promotions %', num: true }]} /></Card>
        </>);
      }}</Async>}
      {tab === 'ch' && <Card title="Ordering channels"><Async state={c}>{(rows) => <DataTable rows={rows.map((r) => ({ ...r, id: r.name }))} columns={[{ key: 'name', label: 'Channel' }, { key: 'orders', label: 'Orders', num: true, render: num }, { key: 'basket', label: 'Basket size', num: true }, { key: 'aov', label: 'AOV', num: true, render: (v) => money(v, 2) }, { key: 'discountPct', label: 'Discount', num: true, render: pct }, { key: 'promoShare', label: 'Promo share', num: true, render: pct }, { key: 'marginPct', label: 'Margin', num: true, render: pct }, { key: 'peak', label: 'Peak' }, { key: 'topCategory', label: 'Top category' }]} />}</Async></Card>}
    </div>
  );
}
