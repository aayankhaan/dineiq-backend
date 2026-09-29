import { useState } from 'react';
import { CartesianGrid, ComposedChart, Legend, Line, ResponsiveContainer, Scatter, ScatterChart, Tooltip, XAxis, YAxis, ReferenceLine } from 'recharts';
import { useApi } from '../hooks.js';
import { Async, Card, Chip, DataTable, Insights, chartTip, money, signed } from '../components/ui.jsx';

export default function Pricing() {
  const [sel, setSel] = useState('');
  const p = useApi('/pricing/sensitivity');
  const h = useApi('/pricing/history', { item: sel }, { filtered: false, skip: !sel });
  return (
    <Async state={p}>{(rows) => {
      const hi = rows.filter((r) => r.sensitivity === 'Highly Price Sensitive');
      return (
        <div className="stack">
          <Insights items={[`${hi.length} items are highly price sensitive: ${hi.slice(0, 3).map((r) => r.name).join(', ')}. Small price rises here cost volume.`, `${rows.filter((r) => r.sensitivity === 'Low Price Sensitivity').length} items are low sensitivity and are candidates for a price increase.`]} />
          <div className="grid g2">
            <Card title="Price change vs demand change" sub="Each dot is an item average across evaluable price changes"><ResponsiveContainer width="100%" height={320}><ScatterChart><CartesianGrid stroke="#e6ebe8" /><XAxis type="number" dataKey="priceChangePct" name="Price change" unit="%" /><YAxis type="number" dataKey="demandChangePct" name="Demand change" unit="%" width={50} /><ReferenceLine x={0} stroke="#8a9a92" /><ReferenceLine y={0} stroke="#8a9a92" /><Tooltip {...chartTip} cursor={{ strokeDasharray: '3 3' }} /><Scatter data={rows} fill="#2e7d5b" /></ScatterChart></ResponsiveContainer></Card>
            <Card title={sel ? 'Price and demand history' : 'Price history'} sub="Click an item in the table">
              {!sel ? <div className="state">Select an item to see how demand moved with its price.</div> : <Async state={h}>{(hist) => <ResponsiveContainer width="100%" height={320}><ComposedChart data={hist}><CartesianGrid stroke="#e6ebe8" vertical={false} /><XAxis dataKey="month" /><YAxis yAxisId="l" width={44} /><YAxis yAxisId="r" orientation="right" width={44} /><Tooltip {...chartTip} /><Legend /><Line yAxisId="l" dataKey="price" name="Price" stroke="#c9a03c" strokeWidth={2} /><Line yAxisId="r" dataKey="demand" name="Demand" stroke="#2e7d5b" strokeWidth={2} /></ComposedChart></ResponsiveContainer>}</Async>}
            </Card>
          </div>
          <Card title="Price sensitivity by item"><DataTable rows={rows} defaultSort={{ key: 'elasticity', dir: 'asc' }} onRowClick={(r) => setSel(r.id)} columns={[{ key: 'name', label: 'Item' }, { key: 'category', label: 'Category' }, { key: 'price', label: 'Price', num: true, render: (v) => money(v, 2) }, { key: 'priceChangePct', label: 'Price change', num: true, render: signed }, { key: 'demandChangePct', label: 'Demand change', num: true, render: signed }, { key: 'elasticity', label: 'Elasticity', num: true }, { key: 'sensitivity', label: 'Sensitivity', render: (v) => <Chip>{v}</Chip> }]} /></Card>
        </div>
      );
    }}</Async>
  );
}
