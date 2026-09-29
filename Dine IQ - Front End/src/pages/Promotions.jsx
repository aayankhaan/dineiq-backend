import { Bar, BarChart, CartesianGrid, Legend, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import { useApi } from '../hooks.js';
import { Async, Card, Chip, DataTable, Insights, chartTip, num, pct, signed } from '../components/ui.jsx';

export default function Promotions() {
  const p = useApi('/promotions');
  return (
    <Async state={p}>{(rows) => {
      const traps = rows.filter((r) => r.verdict === 'Promotion trap');
      return (
        <div className="stack">
          <Insights items={[`${traps.length} of ${rows.length} promotions have recorded trap signals: ${traps.slice(0, 5).map((t) => t.name).join(', ')}.`, `A sales increase alone is not success – each promotion is judged on profit, margin, wastage, new customers and what happens after it ends.`]} />
          <Card title="Sales uplift vs profit change" sub="A trap shows a positive revenue bar with a negative profit bar"><ResponsiveContainer width="100%" height={320}><BarChart data={rows.slice(0, 12)}><CartesianGrid stroke="#e6ebe8" vertical={false} /><XAxis dataKey="name" tick={{ fontSize: 11 }} interval={0} angle={-15} textAnchor="end" height={60} /><YAxis unit="%" width={50} /><Tooltip {...chartTip} /><Legend /><ReferenceLine y={0} stroke="#5e6c66" /><Bar dataKey="revenueChange" name="Revenue change" fill="#2e7d5b" /><Bar dataKey="profitChange" name="Profit change" fill="#c9a03c" /></BarChart></ResponsiveContainer></Card>
          <Card title="Promotion scorecard"><DataTable rows={rows} columns={[{ key: 'name', label: 'Promotion', render: (v, r) => <><b>{v}</b><div className="small muted">{r.period}</div></> }, { key: 'verdict', label: 'Verdict', render: (v) => <Chip>{v}</Chip> }, { key: 'orderUplift', label: 'Demand uplift', num: true, render: signed }, { key: 'revenueChange', label: 'Revenue', num: true, render: signed }, { key: 'marginChange', label: 'Margin', num: true, render: signed }, { key: 'profitChange', label: 'Profit', num: true, render: signed }, { key: 'newCustomers', label: 'New customers', num: true, render: num }, { key: 'repeatRate', label: 'Repeat', num: true, render: (v) => pct(v * 100, 0) }, { key: 'wasteChange', label: 'Wastage', num: true, render: signed }, { key: 'postPromoDrop', label: 'Post-promo drop', num: true, render: pct }, { key: 'flags', label: 'Trap signals', render: (v) => v.map((x) => <span key={x} className="tag">{x}</span>) }]} /></Card>
        </div>
      );
    }}</Async>
  );
}
