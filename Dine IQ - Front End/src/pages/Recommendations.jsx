import { useState } from 'react';
import { useApi } from '../hooks.js';
import { Async, Card, Chip, Insights, money } from '../components/ui.jsx';

const ORDER = { Critical: 0, High: 1, Medium: 2, Low: 3 };
export default function Recommendations() {
  const [pr, setPr] = useState('');
  const r = useApi('/recommendations');
  return (
    <Async state={r}>{(all) => {
      const list = all.filter((x) => !pr || x.priority === pr).sort((a, b) => ORDER[a.priority] - ORDER[b.priority] || b.impact - a.impact);
      return (
        <div className="stack">
          <Insights items={[`${all.filter((x) => x.priority === 'Critical').length} critical and ${all.filter((x) => x.priority === 'High').length} high-priority actions. Each impact uses the unit shown on its card.`, 'Every action lists the evidence behind it. Priority reflects estimated business impact.']} />
          <div className="row">{['', 'Critical', 'High', 'Medium', 'Low'].map((p) => <button key={p} className={`btn sm ${pr === p ? 'primary' : ''}`} onClick={() => setPr(p)}>{p || 'All'}</button>)}</div>
          <div className="stack">
            {list.map((x) => (
              <article key={x.id} className={`rec ${x.priority}`}>
                <div className="row"><Chip>{x.priority}</Chip><Chip tone="c-blue">{x.type}</Chip><span className="spacer" /><span className="muted small">Estimated impact</span><b className="num">{Number(x.impact).toLocaleString()} {x.impactUnit || ''}</b></div>
                <h3 style={{ marginTop: 6 }}>{x.action}</h3><p className="small muted">{x.impactBasis}</p>
                <div className="small muted" style={{ marginTop: 6 }}>Why:</div>
                <ul>{x.evidence.map((e) => <li key={e}>{e}</li>)}</ul>
              </article>
            ))}
          </div>
        </div>
      );
    }}</Async>
  );
}
