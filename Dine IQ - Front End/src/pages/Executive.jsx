import { Area, AreaChart, Bar, BarChart, CartesianGrid, Cell, Pie, PieChart, ResponsiveContainer, Tooltip, XAxis, YAxis, Legend } from 'recharts';
import { useApi } from '../hooks.js';
import { Async, Card, Chip, DataTable, Insights, Kpi, PALETTE, chartTip, money, num } from '../components/ui.jsx';
import { Heatmap } from './Peak.jsx';

export default function Executive() {
  const k = useApi('/kpis/executive');
  const recs = useApi('/recommendations');
  const an = useApi('/anomalies');
  return (
    <div className="stack">
      <Async state={k}>{({ kpis, deltas, trend, channelMix, heat }) => {
        const crit = (recs.data || []).filter((r) => r.priority === 'Critical');
        return (<>
          <Insights items={[
            `Revenue is ${money(kpis.revenue)} with ${kpis.revenue ? ((kpis.profit / kpis.revenue) * 100).toFixed(1) : 0}% contribution margin.`,
            kpis.wastageCost == null ? 'Wastage is unavailable for this filter combination.' : `Estimated wastage cost is ${money(kpis.wastageCost)}.`,
            crit.length ? `${crit.length} critical recommendation${crit.length > 1 ? 's need' : ' needs'} attention: ${crit.slice(0, 3).map((c) => c.action).join('; ')}.` : 'No critical recommendations right now.',
            `${num(kpis.repeatCustomers)} of ${num(kpis.activeCustomers)} active customers are repeat buyers (${(kpis.activeCustomers ? (kpis.repeatCustomers / kpis.activeCustomers) * 100 : 0).toFixed(0)}%).`,
          ]} />
          <div className="kpis">
            <Kpi label="Total revenue" value={money(kpis.revenue)} delta={deltas.revenue} />
            <Kpi label="Contribution margin" value={money(kpis.profit)} delta={deltas.profit} tone="good" />
            <Kpi label="Total orders" value={num(kpis.orders)} delta={deltas.orders} />
            <Kpi label="Average order value" value={money(kpis.aov, 2)} delta={deltas.aov} />
            <Kpi label="Active customers" value={num(kpis.activeCustomers)} delta={deltas.activeCustomers} />
            <Kpi label="Repeat customers" value={num(kpis.repeatCustomers)} delta={deltas.repeatCustomers} />
            <Kpi label="Wastage cost" value={money(kpis.wastageCost)} delta={deltas.wastageCost} invert tone="warn" />
            <Kpi label="Forecast (first 14 saved days)" value={num(kpis.forecastDemand)} delta={deltas.forecastDemand} />
          </div>
          <div className="grid g21">
            <Card title="Revenue and profit" sub="Daily, current filter range">
              <ResponsiveContainer width="100%" height={280}>
                <AreaChart data={trend}><CartesianGrid stroke="#e6ebe8" vertical={false} /><XAxis dataKey="date" tick={{ fontSize: 12 }} minTickGap={40} /><YAxis tick={{ fontSize: 12 }} width={54} /><Tooltip {...chartTip} /><Legend />
                  <Area dataKey="revenue" name="Revenue" stroke="#2e7d5b" fill="#2e7d5b" fillOpacity={0.12} />
                  <Area dataKey="profit" name="Profit" stroke="#c9a03c" fill="#c9a03c" fillOpacity={0.25} /></AreaChart>
              </ResponsiveContainer>
            </Card>
            <Card title="Orders by channel">
              <ResponsiveContainer width="100%" height={280}>
                <PieChart><Pie data={channelMix} dataKey="value" nameKey="name" innerRadius={55} outerRadius={95} paddingAngle={2}>{channelMix.map((_, i) => <Cell key={i} fill={PALETTE[i]} />)}</Pie><Tooltip {...chartTip} /><Legend /></PieChart>
              </ResponsiveContainer>
            </Card>
          </div>
          <div className="grid g2">
            <Card title="When guests order" sub="Orders by weekday and hour"><Heatmap heat={heat} /></Card>
            <Card title="Critical and high-priority actions">
              <Async state={recs}>{(r) => (
                <div className="stack">{r.filter((x) => ['Critical', 'High'].includes(x.priority)).slice(0, 5).map((x) => (
                  <div key={x.id} className="row"><Chip>{x.priority}</Chip><span>{x.action}</span><span className="spacer" /><b className="num">{num(x.impact)} {x.impactUnit || ''}</b></div>
                ))}</div>
              )}</Async>
            </Card>
          </div>
          <Card title="Latest anomalies" sub="Sales and rating events that need a second look">
            <Async state={an}>{(a) => <DataTable pageSize={5} rows={[...a.sales, ...a.ratings]} columns={[{ key: 'date', label: 'Date' }, { key: 'type', label: 'Type' }, { key: 'entity', label: 'Where' }, { key: 'detail', label: 'Detail' }, { key: 'severity', label: 'Severity', render: (v) => <Chip>{v}</Chip> }]} />}</Async>
          </Card>
        </>);
      }}</Async>
    </div>
  );
}
