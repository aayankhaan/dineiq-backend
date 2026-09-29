import { useState } from 'react';
import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import { useApi } from '../hooks.js';
import { Async, Card, DataTable, Insights, chartTip, pct } from '../components/ui.jsx';

export default function Basket() {
  const [minSupport, setS] = useState(0.001);
  const [minLift, setL] = useState(1.2);
  const r = useApi('/basket/rules', { minSupport, minLift });
  return (
    <div className="stack">
      <Card title="Rule thresholds">
        <div className="grid g2">
          <label className="field">Minimum support: {(minSupport * 100).toFixed(2)}%<input type="range" min="0.0001" max="0.002" step="0.0001" value={minSupport} onChange={(e) => setS(+e.target.value)} /></label>
          <label className="field">Minimum lift: {minLift.toFixed(1)}<input type="range" min="1" max="4" step="0.1" value={minLift} onChange={(e) => setL(+e.target.value)} /></label>
        </div>
      </Card>
      <Async state={r} empty="No item pairs pass these thresholds. Lower the minimum support or lift.">{(rules) => {
        const top = rules.slice(0, 4); const chart = rules.slice(0, 10).map((x) => ({ pair: `${x.antecedent} + ${x.consequent}`, lift: x.lift }));
        return (<>
          <Insights items={[`${rules.length} item pairs are bought together more often than chance.`, top[0] && `Strongest pair: ${top[0].antecedent} + ${top[0].consequent} (lift ${top[0].lift}) – guests who order the first also order the second ${(top[0].confidence * 100).toFixed(0)}% of the time.`]} />
          <div className="grid g2" style={{ gridTemplateColumns: 'repeat(auto-fit,minmax(260px,1fr))' }}>
            {top.map((x, i) => (
              <div key={i} className="rec"><h3>Bundle: {x.antecedent} + {x.consequent}</h3>
                <ul><li>Support {pct(x.support * 100, 2)} – share of orders containing both</li><li>Confidence {pct(x.confidence * 100, 1)}</li><li>Lift {x.lift.toFixed(2)}</li></ul></div>
            ))}
          </div>
          <Card title="Top rules by lift"><ResponsiveContainer width="100%" height={320}><BarChart data={chart} layout="vertical" margin={{ left: 140 }}><CartesianGrid stroke="#e6ebe8" horizontal={false} /><XAxis type="number" /><YAxis type="category" dataKey="pair" width={270} tick={{ fontSize: 11 }} /><Tooltip {...chartTip} /><Bar dataKey="lift" fill="#143229" /></BarChart></ResponsiveContainer></Card>
          <Card title="All association rules"><DataTable search rows={rules.map((x, i) => ({ ...x, id: i }))} columns={[{ key: 'antecedent', label: 'If a guest orders' }, { key: 'consequent', label: 'They also order' }, { key: 'support', label: 'Support', num: true, render: (v) => pct(v * 100, 2) }, { key: 'confidence', label: 'Confidence', num: true, render: (v) => pct(v * 100, 1) }, { key: 'lift', label: 'Lift', num: true, render: (v) => Number(v).toFixed(2) }]} /></Card>
        </>);
      }}</Async>
    </div>
  );
}
