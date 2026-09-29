import { useState } from 'react';
import { CartesianGrid, ResponsiveContainer, Scatter, ScatterChart, Tooltip, XAxis, YAxis, ZAxis, Legend, ReferenceLine } from 'recharts';
import { useApi } from '../hooks.js';
import { Async, Card, CLASS_COLOR, Chip, DataTable, Insights, Select, Tabs, chartTip, money, num, pct, signed } from '../components/ui.jsx';
import { DishThumb } from '../components/Visuals.jsx';

const DEFS = {
  'Profit Driver': 'High demand and high profitability with acceptable wastage.',
  'Volume Driver': 'High demand but comparatively lower profitability.',
  'Hidden Opportunity': 'Good profitability, ratings or repeat purchase, but low visibility or sales.',
  'Low Performer': 'Weak demand or profitability, excessive wastage, or poor ratings.',
};

export default function Menu() {
  const [tab, setTab] = useState('matrix');
  const [sel, setSel] = useState('');
  const items = useApi('/menu/items');
  const slow = useApi('/menu/slow-moving');
  const locs = useApi(`/menu/items/${sel}/locations`, {}, { filtered: false, skip: !sel });
  return (
    <div className="stack">
      <Async state={items}>{(rows) => {
        const by = (c) => rows.filter((r) => r.cls === c);
        const hidden = [...by('Hidden Opportunity')].sort((a, b) => b.marginPct - a.marginPct)[0];
        const loss = rows.filter((r) => r.profit < 0);
        return (<>
          <Insights items={[
            `${by('Profit Driver').length} Profit Drivers earn ${money(by('Profit Driver').reduce((s, r) => s + r.profit, 0))} of the ${money(rows.reduce((s, r) => s + r.profit, 0))} total profit.`,
            hidden && `Best Hidden Opportunity: ${hidden.name} – ${pct(hidden.marginPct)} margin, ${hidden.rating} rating, but only ${num(hidden.qty)} units sold.`,
            loss.length ? `${loss.length} item${loss.length > 1 ? 's sell' : ' sells'} at a loss (${loss.map((l) => l.name).join(', ')}) – high volume does not mean success.` : 'No items are selling at a loss.',
            `${rows.filter((r) => r.tags.length).length} items have a special case flagged (promotion-dependent, wastage, seasonal and others).`,
          ]} />
          <div className="grid g2" style={{ gridTemplateColumns: 'repeat(auto-fit,minmax(220px,1fr))' }}>
            {Object.keys(DEFS).map((c) => (
              <div key={c} className="card" style={{ borderTop: `4px solid ${CLASS_COLOR[c]}` }}>
                <div className="row"><h2 style={{ fontSize: 17 }}>{c}</h2><span className="spacer" /><b className="num" style={{ fontSize: 24 }}>{by(c).length}</b></div>
                <p className="small muted" style={{ margin: '4px 0 0' }}>{DEFS[c]}</p>
              </div>
            ))}
          </div>
          <Tabs value={tab} onChange={setTab} tabs={[['matrix', 'Menu matrix'], ['table', 'All items'], ['slow', 'Slow-moving'], ['loc', 'By location']]} />
          {tab === 'matrix' && (
            <Card title="Demand vs profitability" sub="Each dot is a menu item; dot size is revenue. Lines mark the medians.">
              <ResponsiveContainer width="100%" height={420}>
                <ScatterChart margin={{ left: 8, right: 16 }}><CartesianGrid stroke="#e6ebe8" /><XAxis type="number" dataKey="qty" name="Units sold" tick={{ fontSize: 12 }} /><YAxis type="number" dataKey="marginPct" name="Margin %" unit="%" tick={{ fontSize: 12 }} width={50} /><ZAxis dataKey="revenue" range={[60, 500]} name="Revenue" />
                  <Tooltip {...chartTip} content={({ payload }) => payload?.[0] ? <div className="card small" style={{ padding: 10 }}><b>{payload[0].payload.name}</b><br />{payload[0].payload.cls}<br />{num(payload[0].payload.qty)} units · {pct(payload[0].payload.marginPct)} margin</div> : null} />
                  <Legend />
                  {Object.keys(DEFS).map((c) => <Scatter key={c} name={c} data={by(c)} fill={CLASS_COLOR[c]} fillOpacity={0.85} />)}
                </ScatterChart>
              </ResponsiveContainer>
            </Card>
          )}
          {tab === 'table' && (
            <Card title="Menu-item performance" sub="Click a row to see how it performs by location">
              <DataTable search pageSize={12} rows={rows} defaultSort={{ key: 'profit', dir: 'desc' }} onRowClick={(r) => { setSel(r.id); setTab('loc'); }} columns={[
                { key: 'name', label: 'Item', render: (v, r) => <div className="dish-cell"><DishThumb name={v}/><div><b>{v}</b><div>{r.tags.map((t) => <span className="tag" key={t}>{t}</span>)}</div></div></div> },
                { key: 'category', label: 'Category' }, { key: 'cls', label: 'Class', render: (v) => <Chip>{v}</Chip> },
                { key: 'qty', label: 'Units', num: true, render: num }, { key: 'revenue', label: 'Revenue', num: true, render: (v) => money(v) }, { key: 'profit', label: 'Profit', num: true, render: (v) => <span className={v < 0 ? 'down' : ''}>{money(v)}</span> },
                { key: 'marginPct', label: 'Margin', num: true, render: pct }, { key: 'rating', label: 'Rating', num: true }, { key: 'repeatRate', label: 'Repeat', num: true, render: (v) => pct(v * 100, 0) },
                { key: 'wastePct', label: 'Waste', num: true, render: pct }, { key: 'promoDep', label: 'Promo dep.', num: true, render: (v) => pct(v * 100, 0) }, { key: 'trend', label: 'Trend', num: true, render: (v) => <span className={v < 0 ? 'down' : 'up'}>{signed(v)}</span> },
              ]} />
            </Card>
          )}
          {tab === 'slow' && (
            <Card title="Slow-moving dishes" sub="Scored from low sales, purchase gaps, low repeat purchase, wastage, weak margin and falling trend">
              <Async state={slow}>{(s) => <DataTable rows={s} defaultSort={{ key: 'slowScore', dir: 'desc' }} columns={[{ key: 'name', label: 'Item' }, { key: 'slowScore', label: 'Slow score', num: true }, { key: 'qty', label: 'Units', num: true, render: num }, { key: 'orderFreq', label: 'Purchase count', num: true }, { key: 'daysSinceLast', label: 'Days since last order', num: true }, { key: 'repeatRate', label: 'Repeat', num: true, render: (v) => pct(v * 100, 0) }, { key: 'wastePct', label: 'Waste', num: true, render: pct }, { key: 'trend', label: 'Trend', num: true, render: signed }]} />}</Async>
            </Card>
          )}
          {tab === 'loc' && (
            <Card title="Same dish, different locations" sub="Each location is classified on its own"
              actions={<div style={{ minWidth: 220 }}><Select value={sel} onChange={setSel} options={rows.map((r) => ({ id: r.id, name: r.name }))} placeholder="Choose an item" /></div>}>
              {!sel ? <div className="state">Choose an item to compare its locations.</div> : (
                <Async state={locs}>{(l) => <DataTable rows={l} columns={[{ key: 'location', label: 'Location' }, { key: 'cls', label: 'Class here', render: (v) => <Chip>{v}</Chip> }, { key: 'qty', label: 'Units', num: true, render: num }, { key: 'marginPct', label: 'Margin', num: true, render: pct }, { key: 'wastePct', label: 'Waste', num: true, render: pct }]} />}</Async>
              )}
            </Card>
          )}
        </>);
      }}</Async>
    </div>
  );
}
