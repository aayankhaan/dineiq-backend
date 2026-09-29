import { Bar, BarChart, CartesianGrid, Legend, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import { useApi } from '../hooks.js';
import { Async, Card, DataTable, Insights, chartTip, num } from '../components/ui.jsx';

const DAYS = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];
export function Heatmap({ heat }) {
  const max = Math.max(1, ...heat.flat());
  return (
    <div className="heat" role="img" aria-label="Orders by weekday and hour">
      <div />{Array.from({ length: 24 }, (_, h) => <div key={h} className="lbl" style={{ justifyContent: 'center' }}>{h % 3 === 0 ? h : ''}</div>)}
      {heat.map((row, d) => [<div key={`l${d}`} className="lbl">{DAYS[d]}</div>, ...row.map((v, h) => <div key={`${d}-${h}`} title={`${DAYS[d]} ${h}:00 – ${v} orders`} style={{ background: `rgba(20,50,41,${0.06 + (v / max) * 0.94})` }} />)])}
    </div>
  );
}

export default function Peak() {
  const s = useApi('/peak');
  return (
    <Async state={s}>{(p) => {
      const hr = [...p.byHour].sort((a, b) => b.orders - a.orders)[0]; const day = [...p.byDay].sort((a, b) => b.orders - a.orders)[0];
      return (
        <div className="stack">
          <Insights items={[`Busiest hour is ${hr.hour}:00 (about ${num(hr.orders)} orders in the selected period); busiest day is ${day.day}.`, `Weekends carry ${(p.weekendRatio * 100).toFixed(0)}% of all orders.`, 'Compare the observed dine-in and delivery peaks below before scheduling preparation.']} />
          <Card title="Weekday × hour heatmap"><Heatmap heat={p.heat} /></Card>
          <div className="grid g2">
            <Card title="Orders by hour"><ResponsiveContainer width="100%" height={240}><BarChart data={p.byHour}><CartesianGrid stroke="#e6ebe8" vertical={false} /><XAxis dataKey="hour" /><YAxis width={44} /><Tooltip {...chartTip} /><Bar dataKey="orders" fill="#2e7d5b" /></BarChart></ResponsiveContainer></Card>
            <Card title="Orders by weekday"><ResponsiveContainer width="100%" height={240}><BarChart data={p.byDay}><CartesianGrid stroke="#e6ebe8" vertical={false} /><XAxis dataKey="day" /><YAxis width={44} /><Tooltip {...chartTip} /><Bar dataKey="orders" fill="#c9a03c" /></BarChart></ResponsiveContainer></Card>
            <Card title="Dine-in vs delivery by hour"><ResponsiveContainer width="100%" height={240}><LineChart data={p.dineVsDelivery}><CartesianGrid stroke="#e6ebe8" vertical={false} /><XAxis dataKey="hour" /><YAxis width={44} /><Tooltip {...chartTip} /><Legend /><Line dataKey="dineIn" name="Dine-in" stroke="#2e7d5b" dot={false} strokeWidth={2} /><Line dataKey="delivery" name="Delivery" stroke="#c4574a" dot={false} strokeWidth={2} /></LineChart></ResponsiveContainer></Card>
            <Card title="Monthly trend"><ResponsiveContainer width="100%" height={240}><BarChart data={p.monthly}><CartesianGrid stroke="#e6ebe8" vertical={false} /><XAxis dataKey="month" /><YAxis width={44} /><Tooltip {...chartTip} /><Bar dataKey="orders" fill="#4f7cac" /></BarChart></ResponsiveContainer></Card>
          </div>
          <div className="grid g2">
            <Card title="Seasonal index" sub="1.00 = average season"><DataTable rows={p.seasonal} columns={[{ key: 'season', label: 'Season' }, { key: 'index', label: 'Demand index', num: true }]} /></Card>
            <Card title="Peaks by location"><DataTable rows={p.byLocation} columns={[{ key: 'location', label: 'Location' }, { key: 'peakHour', label: 'Peak hour' }, { key: 'peakDay', label: 'Peak day' }]} /></Card>
          </div>
        </div>
      );
    }}</Async>
  );
}
