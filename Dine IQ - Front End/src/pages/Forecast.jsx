import { useState } from 'react';
import { Area, CartesianGrid, ComposedChart, Legend, Line, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import { useApi } from '../hooks.js';
import { Async, Card, DataTable, Field, Insights, chartTip } from '../components/ui.jsx';

export default function Forecast() {
  const [level, setLevel] = useState('category');
  const [horizon, setHorizon] = useState(14);
  const f = useApi('/forecast', { level, horizon });
  return (
    <div className="stack">
      <Card title="Forecast settings" sub="Item, category and location filters in the top bar narrow the forecast">
        <div className="row">
          <Field label="Forecast level"><select className="input" value={level} onChange={(e) => setLevel(e.target.value)}><option value="item">Menu item</option><option value="category">Category</option><option value="location">Location</option></select></Field>
          <Field label="Days ahead"><input className="input" type="number" min="1" max={f.data?.maxHorizon || 30} value={horizon} onChange={(e) => setHorizon(Math.min(f.data?.maxHorizon || 30, Math.max(1, +e.target.value || 1)))} /></Field>
        </div>
      </Card>
      <Async state={f}>{(d) => {
        const data = d.series.map((r) => ({ ...r, band: r.lo != null ? [r.lo, r.hi] : null }));
        const m = d.metrics; const better = m.baselineMAE ? ((1 - m.MAE / m.baselineMAE) * 100) : null;
        return (<>
          <Insights items={[better == null ? 'No baseline comparison is available for this selection.' : `The model's average error (MAE ${m.MAE}) is ${Math.abs(better).toFixed(1)}% ${better >= 0 ? 'lower' : 'higher'} than the baseline "${m.baselineName}".`, `Trained on earlier weeks up to ${d.split.trainEnd}, tested on later unseen weeks from ${d.split.testStart} – no future data leaks into training.`, d.risk.length ? `${d.risk.length} upcoming days are forecast well above average: ${d.risk.map((r) => r.date).join(', ')}.` : 'No unusually high-demand days in the forecast window.']} />
          <Card title="Historical, tested and forecast demand" sub={`${d.modelName || 'Forecast model'} · ${d.versions.spark}. Accuracy metrics cover the selected entity series.`}>
            <ResponsiveContainer width="100%" height={380}>
              <ComposedChart data={data}><CartesianGrid stroke="#e6ebe8" vertical={false} /><XAxis dataKey="date" minTickGap={40} tick={{ fontSize: 12 }} /><YAxis width={52} /><Tooltip {...chartTip} /><Legend />
                <Area dataKey="band" name="Forecast range" stroke="none" fill="#c9a03c" fillOpacity={0.22} />
                <Line dataKey="actual" name="Actual" stroke="#2e7d5b" dot={false} strokeWidth={2} />
                <Line dataKey="predicted" name="Predicted" stroke="#c4574a" dot={false} strokeWidth={2} strokeDasharray="5 3" />
                <ReferenceLine x={d.split.testStart} stroke="#5e6c66" strokeDasharray="3 3" label={{ value: 'Test period starts', fontSize: 12 }} /></ComposedChart>
            </ResponsiveContainer>
          </Card>
          <div className="grid g2">
            <Card title="Accuracy vs baseline"><DataTable rows={[{ id: 1, metric: 'MAE', model: m.MAE, base: m.baselineMAE }, { id: 2, metric: 'MAPE', model: m.MAPE, base: m.baselineMAPE }, { id: 3, metric: 'RMSE', model: m.RMSE, base: '—' }, { id: 4, metric: 'R²', model: m.R2, base: '—' }]} columns={[{ key: 'metric', label: 'Measure' }, { key: 'model', label: 'Model', num: true }, { key: 'base', label: 'Baseline', num: true }]} /></Card>
            <Card title="High-risk demand periods"><DataTable rows={d.risk.map((r) => ({ ...r, id: r.date }))} columns={[{ key: 'date', label: 'Date' }, { key: 'predicted', label: 'Forecast', num: true }, { key: 'note', label: 'Action' }]} /></Card>
          </div>
        </>);
      }}</Async>
    </div>
  );
}
