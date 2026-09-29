import { useEffect, useState } from 'react';
import { useApi } from '../hooks.js';
import { Async, Card, Chip, DataTable, Insights, Tabs, num } from '../components/ui.jsx';

export default function Jobs() {
  const [tab, setTab] = useState('jobs');
  const j = useApi('/jobs', {}, { filtered: false });
  const running = j.data?.jobs.some((x) => x.status === 'Running');
  useEffect(() => { if (!running) return undefined; const t = setInterval(j.reload, 5000); return () => clearInterval(t); }, [running]); // eslint-disable-line
  return (
    <div className="stack">
      <Async state={j}>{({ jobs, dq, note }) => {
        const failed = jobs.filter((x) => x.status === 'Failed');
        return (<>
          <Insights items={[failed.length ? `${failed.length} job failed: ${failed[0].name}. ${failed[0].error}` : (note || 'No recorded job failures.'), `${num(dq.reduce((s, x) => s + x.found, 0))} quality findings and cleaning actions are listed below; counts can overlap.`, running && 'A job is running – this page refreshes every 5 seconds.']} />
          <Tabs value={tab} onChange={setTab} tabs={[['jobs', 'Spark jobs'], ['dq', 'Data-quality report']]} />
          {tab === 'jobs' && <Card title="Processing jobs"><DataTable rows={jobs} columns={[{ key: 'id', label: 'Job' }, { key: 'name', label: 'Name', render: (v, r) => <>{v}{r.error && <div className="small down">{r.error}</div>}</> }, { key: 'engine', label: 'Engine' }, { key: 'status', label: 'Status', render: (v) => <Chip>{v}</Chip> }, { key: 'progress', label: 'Progress', render: (v) => <div className="bar"><i style={{ width: `${v}%` }} /></div> }, { key: 'rows', label: 'Rows', num: true, render: num }, { key: 'duration', label: 'Duration' }, { key: 'started', label: 'Output updated' }]} /></Card>}
          {tab === 'dq' && <Card title="Data-quality report" sub="What was found before analysis and what was done about it"><DataTable rows={dq.map((x) => ({ ...x, id: x.rule }))} columns={[{ key: 'rule', label: 'Rule' }, { key: 'issue', label: 'Issue' }, { key: 'found', label: 'Records', num: true, render: num }, { key: 'action', label: 'Action taken' }]} /></Card>}
        </>);
      }}</Async>
    </div>
  );
}
