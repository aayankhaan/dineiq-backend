import { useState } from 'react';
import { Bar, BarChart, CartesianGrid, Cell, Legend, Pie, PieChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import { useApi } from '../hooks.js';
import { Async, Card, Chip, DataTable, Insights, PALETTE, Tabs, chartTip, money, num, pct } from '../components/ui.jsx';

export default function Customers() {
  const [tab, setTab] = useState('seg');
  const seg = useApi('/customers/segments');
  const rfm = useApi('/customers/rfm');
  const churn = useApi('/customers/churn');
  return (
    <div className="stack">
      <Async state={seg}>{(s) => {
        const total = s.reduce((a, b) => a + b.count, 0); const risk = s.find((x) => x.name.startsWith('At-Risk')); const top = s.find((x) => x.name.startsWith('High-Value Loyal'));
        return <Insights items={[`${num(total)} customers segmented.`, top && `${pct((top.count / total) * 100)} are High-Value Loyal, with ${num(top.frequency, 1)} orders per customer in the saved period.`, risk && `${num(risk.count)} customers are At-Risk – average ${risk.recencyDays} days since their last visit.`, 'The strategies below come from the saved segmentation analysis.']} />;
      }}</Async>
      <Tabs value={tab} onChange={setTab} tabs={[['seg', 'Segments'], ['rfm', 'RFM'], ['churn', 'Churn risk']]} />
      {tab === 'seg' && (
        <div className="grid g12">
          <Card title="Segment size"><Async state={seg}>{(s) => <ResponsiveContainer width="100%" height={300}><PieChart><Pie data={s} dataKey="count" nameKey="name" innerRadius={55} outerRadius={100} paddingAngle={2}>{s.map((_, i) => <Cell key={i} fill={PALETTE[i]} />)}</Pie><Tooltip {...chartTip} /><Legend /></PieChart></ResponsiveContainer>}</Async></Card>
          <Card title="Segments and how to treat them"><Async state={seg}>{(s) => <DataTable rows={s.map((x) => ({ ...x, id: x.name }))} pageSize={8} columns={[{ key: 'name', label: 'Segment', render: (v) => <b>{v}</b> }, { key: 'count', label: 'Customers', num: true, render: num }, { key: 'avgOrder', label: 'Avg order', num: true, render: (v) => money(v, 2) }, { key: 'recencyDays', label: 'Recency (days)', num: true }, { key: 'frequency', label: 'Frequency', num: true }, { key: 'strategy', label: 'Suggested strategy' }]} />}</Async></Card>
        </div>
      )}
      {tab === 'rfm' && (
        <Async state={rfm}>{({ bands, top }) => (<>
          <Card title="RFM score distribution" sub="Customers per score band (5 = best)"><ResponsiveContainer width="100%" height={280}><BarChart data={bands}><CartesianGrid stroke="#e6ebe8" vertical={false} /><XAxis dataKey="score" /><YAxis width={50} /><Tooltip {...chartTip} /><Legend /><Bar dataKey="recency" name="Recency" fill="#143229" /><Bar dataKey="frequency" name="Frequency" fill="#c9a03c" /><Bar dataKey="monetary" name="Monetary" fill="#4f7cac" /></BarChart></ResponsiveContainer></Card>
          <Card title="Top high-value customers"><DataTable rows={top} columns={[{ key: 'id', label: 'Customer' }, { key: 'recency', label: 'Recency (days)', num: true }, { key: 'frequency', label: 'Frequency', num: true }, { key: 'monetary', label: 'Monetary', num: true, render: (v) => money(v) }, { key: 'favCategory', label: 'Favourite category' }, { key: 'channel', label: 'Channel' }]} /></Card>
        </>)}</Async>
      )}
      {tab === 'churn' && (
        <Card title="Customers showing reduced engagement" sub="Sorted by the share of rule-based churn signals present; this is not a probability">
          <Async state={churn}>{(c) => <DataTable rows={c} columns={[{ key: 'id', label: 'Customer' }, { key: 'risk', label: 'Risk', num: true, render: (v) => <Chip tone={v > 0.85 ? 'c-bad' : 'c-warn'}>{pct(v * 100, 0)}</Chip> }, { key: 'recency', label: 'Days since visit', num: true }, { key: 'frequencyChange', label: 'Frequency', num: true, render: (v) => <span className="down">{v}%</span> }, { key: 'spendChange', label: 'Spend', num: true, render: (v) => <span className="down">{v}%</span> }, { key: 'reasons', label: 'Why', render: (v) => v.map((x) => <span className="tag" key={x}>{x}</span>) }]} />}</Async>
        </Card>
      )}
    </div>
  );
}
