import { useState } from 'react';
import { Bar, BarChart, CartesianGrid, Legend, Line, LineChart, ResponsiveContainer, Scatter, ScatterChart, Tooltip, XAxis, YAxis } from 'recharts';
import { useApi } from '../hooks.js';
import { Async, Card, Chip, DataTable, Insights, Tabs, chartTip, num, pct } from '../components/ui.jsx';
import { DishThumb } from '../components/Visuals.jsx';

const SEV = { High: 'Critical', Medium: 'High', Low: 'Low' };
export default function Ratings() {
  const [tab, setTab] = useState('r');
  const r = useApi('/ratings'); const a = useApi('/anomalies');
  const cols = [{ key: 'date', label: 'Date' }, { key: 'type', label: 'Type' }, { key: 'entity', label: 'Where' }, { key: 'detail', label: 'Detail' }, { key: 'severity', label: 'Severity', render: (v) => <Chip>{v}</Chip> }];
  return (
    <div className="stack">
      <Tabs value={tab} onChange={setTab} tabs={[['r', 'Ratings & satisfaction'], ['ra', 'Rating anomalies'], ['sa', 'Sales anomalies']]} />
      {tab === 'r' && <Async state={r}>{(d) => {
        const byItem = d.byItem || []; const low = byItem.filter((i) => i.rating < 3.5 && i.qty > 4000); const best = byItem[0];
        return (<>
          <Insights items={[low.length ? `${low.length} poorly rated items still sell in volume (${low.map((x) => x.name).join(', ')}) – quality risk.` : 'No high-volume items have poor ratings.', best ? `Best-rated dish: ${best.name} (${best.rating}).` : 'No rating records match the current filters.']} />
          <div className="grid g2">
            <Card title="Rating vs margin" sub="Highly rated dishes with low margin are repricing candidates"><ResponsiveContainer width="100%" height={300}><ScatterChart><CartesianGrid stroke="#e6ebe8" /><XAxis type="number" dataKey="rating" name="Rating" domain={[3, 5]} /><YAxis type="number" dataKey="marginPct" name="Margin" unit="%" width={50} /><Tooltip {...chartTip} /><Scatter data={byItem} fill="#2e7d5b" /></ScatterChart></ResponsiveContainer></Card>
            <Card title="Rating trend: all vs promotion orders"><ResponsiveContainer width="100%" height={300}><LineChart data={d.trend}><CartesianGrid stroke="#e6ebe8" vertical={false} /><XAxis dataKey="week" /><YAxis domain={[3.5, 5]} width={40} /><Tooltip {...chartTip} /><Legend /><Line dataKey="rating" name="All orders" stroke="#2e7d5b" strokeWidth={2} /><Line dataKey="promo" name="Promotion orders" stroke="#c9a03c" strokeWidth={2} /></LineChart></ResponsiveContainer></Card>
            <Card title="Rating by location"><ResponsiveContainer width="100%" height={260}><BarChart data={d.byLocation}><CartesianGrid stroke="#e6ebe8" vertical={false} /><XAxis dataKey="name" tick={{ fontSize: 12 }} /><YAxis domain={[3, 5]} width={40} /><Tooltip {...chartTip} /><Bar dataKey="rating" fill="#4f7cac" /></BarChart></ResponsiveContainer></Card>
            <Card title="Items by rating"><DataTable pageSize={8} rows={byItem} columns={[{ key: 'name', label: 'Item', render: (v) => <div className="dish-cell"><DishThumb name={v} size={32}/><b>{v}</b></div> }, { key: 'rating', label: 'Rating', num: true }, { key: 'marginPct', label: 'Margin', num: true, render: pct }, { key: 'qty', label: 'Units', num: true, render: num }, { key: 'repeatRate', label: 'Repeat', num: true, render: (v) => pct(v * 100, 0) }]} /></Card>
          </div>
        </>);
      }}</Async>}
      {tab === 'ra' && <Card title="Unusual rating patterns"><Async state={a}>{(d) => <DataTable rows={d.ratings} columns={cols} />}</Async></Card>}
      {tab === 'sa' && <Card title="Unusual sales and transactions"><Async state={a}>{(d) => <DataTable rows={d.sales} columns={cols} />}</Async></Card>}
    </div>
  );
}
