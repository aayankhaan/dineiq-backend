import { Bar, BarChart, CartesianGrid, Cell, Legend, Line, LineChart, Pie, PieChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import { useApi } from '../hooks.js';
import { Async, Card, Chip, DataTable, Insights, Kpi, PALETTE, Tabs, chartTip, money, num, pct } from '../components/ui.jsx';
import { useState } from 'react';

export default function Wastage() {
  const [tab, setTab] = useState('overview');
  const w = useApi('/wastage');
  return (
    <Async state={w}>{(d) => {
      const worst = d.byItem[0]; const wl = [...d.byLocation].sort((a, b) => b.pct - a.pct)[0]; const over = d.byReason[0];
      return (
        <div className="stack">
          <Insights items={[worst && `${worst.name} costs the most in wastage (${money(worst.cost)}, ${pct(worst.wastePct)} of what is prepared).`, wl && `${wl.name} has the highest wastage rate at ${pct(wl.pct)}.`, over && `${over.name} accounts for ${pct(over.value)} of allocated wastage cost.`, `${d.risk.filter((r) => r.risk > 0.8).length} item-and-day combinations carry a wastage risk above 80% – see the predictions below.`]} />
          <div className="kpis"><Kpi label="Estimated wasted servings" value={`${num(d.kpis.totalUnits)} units`} tone="warn" /><Kpi label="Wastage cost" value={money(d.kpis.cost)} tone="bad" /><Kpi label="Wastage rate" value={pct(d.kpis.pct)} /></div>
          <Tabs value={tab} onChange={setTab} tabs={[["overview", "Overview"], ["risk", "Risk predictions"], ["records", "Detailed records"]]} />
          {tab === 'overview' && <div className="grid g2">
            <Card title="Highest-wastage items (cost)"><ResponsiveContainer width="100%" height={300}><BarChart data={d.byItem.slice(0, 10)} layout="vertical" margin={{ left: 90 }}><CartesianGrid stroke="#e6ebe8" horizontal={false} /><XAxis type="number" /><YAxis type="category" dataKey="name" width={150} tick={{ fontSize: 12 }} /><Tooltip {...chartTip} /><Bar dataKey="cost" fill="#c4574a" /></BarChart></ResponsiveContainer></Card>
            <Card title="Wastage by reason"><ResponsiveContainer width="100%" height={300}><PieChart><Pie data={d.byReason} dataKey="value" nameKey="name" innerRadius={55} outerRadius={100}>{d.byReason.map((_, i) => <Cell key={i} fill={PALETTE[i]} />)}</Pie><Tooltip {...chartTip} /><Legend /></PieChart></ResponsiveContainer></Card>
            <Card title="Wastage rate by location"><ResponsiveContainer width="100%" height={260}><BarChart data={d.byLocation}><CartesianGrid stroke="#e6ebe8" vertical={false} /><XAxis dataKey="name" tick={{ fontSize: 12 }} /><YAxis unit="%" width={44} /><Tooltip {...chartTip} /><Bar dataKey="pct" name="Wastage %" fill="#c9a03c" /></BarChart></ResponsiveContainer></Card>
            <Card title="Daily wastage cost"><ResponsiveContainer width="100%" height={260}><LineChart data={d.trend}><CartesianGrid stroke="#e6ebe8" vertical={false} /><XAxis dataKey="date" minTickGap={30} tick={{ fontSize: 12 }} /><YAxis width={48} /><Tooltip {...chartTip} /><Line dataKey="cost" stroke="#2e7d5b" dot={false} strokeWidth={2} /></LineChart></ResponsiveContainer></Card>
          </div>}
          {tab === 'risk' && <Card title="Wastage-risk predictions" sub="Saved held-out risk predictions; preparation and cost columns are historical inputs">
            <DataTable rows={d.risk.map((r, i) => ({ ...r, id: i }))} columns={[{ key: 'item', label: 'Item' }, { key: 'location', label: 'Location' }, { key: 'day', label: 'Day' }, { key: 'risk', label: 'Risk', num: true, render: (v) => <Chip tone={v > 0.8 ? 'c-bad' : 'c-warn'}>{pct(v * 100, 0)}</Chip> }, { key: 'prepQty', label: 'Previous prep qty', num: true }, { key: 'forecastDemand', label: 'Forecast demand', num: true }, { key: 'expectedCost', label: 'Historical avg waste cost', num: true, render: (v) => money(v) }]} />
          </Card>}
          {tab === 'records' && <Card title="Wastage records" sub="Latest 500 allocated records matching the selected date, location, item and category filters">
            <DataTable search pageSize={12} rows={d.records} columns={[{ key: 'date', label: 'Date' }, { key: 'item', label: 'Menu item' }, { key: 'ingredient', label: 'Ingredient' }, { key: 'location', label: 'Location' }, { key: 'quantity', label: 'Quantity', num: true, render: (v, row) => `${num(v, 2)} ${row.unit}` }, { key: 'cost', label: 'Cost', num: true, render: (v) => money(v, 2) }, { key: 'reason', label: 'Reason' }]} />
          </Card>}
        </div>
      );
    }}</Async>
  );
}
