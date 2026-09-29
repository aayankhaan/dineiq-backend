import { useMemo, useState } from 'react';

// Digits must be an integer 0-20. Table columns call render(value, row), so a bare
// `render: num` used to receive the row object as `d` and crash Intl with
// "maximumFractionDigits value is out of range".
const digits = (d, fallback) => (Number.isInteger(d) && d >= 0 && d <= 20 ? d : fallback);
const valid = (n) => n != null && n !== '' && Number.isFinite(Number(n));
export const money = (n, d) => { const k = digits(d, 0); return valid(n) ? (import.meta.env.VITE_CURRENCY || '$') + Number(n).toLocaleString(undefined, { maximumFractionDigits: k, minimumFractionDigits: k }) : '—'; };
export const num = (n, d) => (valid(n) ? Number(n).toLocaleString(undefined, { maximumFractionDigits: digits(d, 0) }) : '—');
export const pct = (n, d) => (valid(n) ? `${Number(n).toFixed(digits(d, 1))}%` : '—');
export const signed = (n, d = 1) => (n == null ? '—' : `${n > 0 ? '+' : ''}${Number(n).toFixed(digits(d, 1))}%`);

export const CLASS_TONE = { 'Profit Driver': 'c-good', 'Volume Driver': 'c-blue', 'Hidden Opportunity': 'c-warn', 'Low Performer': 'c-bad' };
export const CLASS_COLOR = { 'Profit Driver': '#2e7d5b', 'Volume Driver': '#4f7cac', 'Hidden Opportunity': '#c9a03c', 'Low Performer': '#c4574a' };
export const PALETTE = ['#2e7d5b', '#4f7cac', '#c9a03c', '#c4574a', '#7f6a9b', '#8a9a92', '#143229'];
const TONES = { Critical: 'c-bad', High: 'c-warn', Medium: 'c-blue', Low: '', Effective: 'c-good', 'Promotion trap': 'c-bad', Neutral: '', 'Highly Price Sensitive': 'c-bad', 'Moderately Price Sensitive': 'c-warn', 'Low Price Sensitivity': 'c-good', Completed: 'c-good', Running: 'c-blue', Queued: '', Failed: 'c-bad' };
import { EmptyArt, ErrorArt } from './StateArt.jsx';

export function Chip({ children, tone }) {
  const t = tone ?? CLASS_TONE[children] ?? TONES[children] ?? '';
  return <span className={`chip ${t}`}>{children}</span>;
}
export const Estimate = () => <span className="est" title="Simulated value – not an actual result">Estimate</span>;

export function Card({ title, sub, actions, children, className = '' }) {
  return (
    <section className={`card ${className}`}>
      {(title || actions) && (
        <div className="card-h">
          <div><h2>{title}</h2>{sub && <p>{sub}</p>}</div>
          <div className="spacer" />{actions}
        </div>
      )}
      {children}
    </section>
  );
}

export function Kpi({ label, value, delta, tone = '', invert = false, hint }) {
  const good = delta == null ? null : invert ? delta < 0 : delta > 0;
  return (
    <div className={`kpi ${tone}`} title={hint}>
      <div className="kpi-l">{label}</div>
      <div className="kpi-v num">{value}</div>
      {delta != null && <div className={`kpi-d ${good ? 'up' : 'down'}`}>{delta > 0 ? '▲' : '▼'} {Math.abs(delta)}% vs previous period</div>}
    </div>
  );
}

// SRS: "insights rather than only visualizations" – every page starts with plain-language findings.
export function Insights({ items }) {
  const list = (items || []).filter(Boolean);
  if (!list.length) return null;
  return (
    <div className="insights">
      <h2>What the data says</h2>
      <ul>{list.map((t, i) => <li key={i}>{t}</li>)}</ul>
    </div>
  );
}

export function Async({ state, children, empty }) {
  if (state.loading && !state.data) return <div className="skeleton" role="status" aria-label="Loading" />;
  if (state.error) {
    return (
      <div className="state err" role="alert">
        <ErrorArt />
        <p style={{ margin: '0 0 10px' }}>{state.error.message}</p>
        <button className="btn sm" onClick={state.reload}>Try again</button>
      </div>
    );
  }
  if (!state.data || (Array.isArray(state.data) && !state.data.length)) return <div className="state"><EmptyArt />{empty || 'No records match the current filters. Widen the date range or clear a filter.'}</div>;
  return children(state.data);
}

export function Tabs({ tabs, value, onChange }) {
  return (
    <div className="tabs" role="tablist">
      {tabs.map(([k, l]) => (
        <button key={k} role="tab" aria-selected={value === k} className={`tab ${value === k ? 'on' : ''}`} onClick={() => onChange(k)}>{l}</button>
      ))}
    </div>
  );
}

export function DataTable({ columns, rows, pageSize = 10, onRowClick, search = false, rowClass, defaultSort }) {
  const [sort, setSort] = useState(defaultSort || null);
  const [page, setPage] = useState(0);
  const [q, setQ] = useState('');
  const view = useMemo(() => {
    let r = rows;
    if (q) r = r.filter((x) => JSON.stringify(x).toLowerCase().includes(q.toLowerCase()));
    if (sort) r = [...r].sort((a, b) => { const x = a[sort.key], y = b[sort.key]; const c = typeof x === 'number' ? x - y : String(x ?? '').localeCompare(String(y ?? '')); return sort.dir === 'asc' ? c : -c; });
    return r;
  }, [rows, sort, q]);
  const pages = Math.max(1, Math.ceil(view.length / pageSize));
  const cur = Math.min(page, pages - 1);
  return (
    <div>
      {search && <input className="input" style={{ maxWidth: 280, marginBottom: 10 }} placeholder="Search this table" value={q} onChange={(e) => { setQ(e.target.value); setPage(0); }} aria-label="Search table" />}
      <div className="tw">
        <table className="tbl">
          <thead>
            <tr>{columns.map((c) => (
              <th key={c.key} className={c.num ? 'r' : ''} onClick={() => setSort((s) => (s?.key === c.key ? { key: c.key, dir: s.dir === 'asc' ? 'desc' : 'asc' } : { key: c.key, dir: c.num ? 'desc' : 'asc' }))}>
                {c.label}{sort?.key === c.key ? (sort.dir === 'asc' ? ' ▲' : ' ▼') : ''}
              </th>
            ))}</tr>
          </thead>
          <tbody>
            {view.slice(cur * pageSize, (cur + 1) * pageSize).map((r, i) => (
              <tr key={r.id ?? r.recordId ?? i} className={`${onRowClick ? 'click' : ''} ${rowClass ? rowClass(r) : ''}`} onClick={() => onRowClick?.(r)}>
                {columns.map((c) => <td key={c.key} className={c.num ? 'r num' : ''}>{c.render ? c.render(r[c.key], r) : r[c.key]}</td>)}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {pages > 1 && (
        <div className="pager">
          <span className="muted">{view.length} rows</span>
          <button className="btn sm" disabled={cur === 0} onClick={() => setPage(cur - 1)}>Previous</button>
          <span>Page {cur + 1} of {pages}</span>
          <button className="btn sm" disabled={cur >= pages - 1} onClick={() => setPage(cur + 1)}>Next</button>
        </div>
      )}
    </div>
  );
}

export function Field({ label, children }) { return <label className="field">{label}{children}</label>; }
export function Select({ value, onChange, options, placeholder = 'All' }) {
  return (
    <select className="input" value={value} onChange={(e) => onChange(e.target.value)}>
      <option value="">{placeholder}</option>
      {options.map((o) => (typeof o === 'string' ? <option key={o} value={o}>{o}</option> : <option key={o.id} value={o.id}>{o.name}</option>))}
    </select>
  );
}
export const chartTip = { cursor: { fill: 'rgba(31,118,87,.045)' }, contentStyle: { borderRadius: 12, border: '1px solid #e1e9e5', fontSize: 12, boxShadow: '0 12px 34px rgba(24,54,44,.12)', padding: '9px 11px' }, labelStyle: { color: '#17322a', fontWeight: 700 }, itemStyle: { padding: '2px 0' } };
