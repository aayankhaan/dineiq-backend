import { useState } from 'react';
import { useApi } from '../hooks.js';
import { Async, Card, Chip, DataTable, Kpi, pct } from '../components/ui.jsx';

const percent = (value) => pct(Number(value) * 100, 1);

export default function DualPipeline() {
  const [onlyDiff, setOnlyDiff] = useState(false);
  const comparison = useApi('/models/comparison', { task: 'menu_class' }, { filtered: false });
  return (
    <Async state={comparison}>{(result) => {
      const rows = [
        { id: 'spark', pipeline: 'Spark MLlib', model: result.models.spark, ...result.metrics.spark },
        { id: 'python', pipeline: 'Python', model: result.models.python, ...result.metrics.python },
      ];
      const comparisons = (onlyDiff ? result.rows.filter((row) => !row.match) : result.rows)
        .map((row) => ({ ...row, id: row.recordId }));
      const sparkAccuracy = result.metrics.spark.Accuracy;
      const pythonAccuracy = result.metrics.python.Accuracy;
      const pythonWins = pythonAccuracy >= sparkAccuracy;
      const agreed = result.total - result.disagreements;
      return (
        <div className="stack">
          <div className="kpis">
            <Kpi
              label="Final result"
              value={pythonWins ? result.models.python : result.models.spark}
              tone="good"
              hint={`${pythonWins ? 'Python' : 'Spark'} produced the higher held-out accuracy.`}
            />
            <Kpi
              label="Predictions agreed"
              value={`${agreed} of ${result.total}`}
              hint={`${result.agreement.toFixed(1)}% agreement between Spark and Python.`}
            />
            <Kpi
              label="Best accuracy"
              value={percent(Math.max(sparkAccuracy, pythonAccuracy))}
              tone="good"
              hint={`${pythonWins ? 'Python Random Forest' : 'Spark Decision Tree'} held-out accuracy.`}
            />
          </div>
          <Card
            title="Spark and Python model comparison"
            sub={`${result.total} held-out records · ${result.agreement.toFixed(1)}% prediction agreement`}
          >
            <DataTable rows={rows} columns={[
              { key: 'pipeline', label: 'Pipeline' },
              { key: 'model', label: 'Model used' },
              { key: 'Accuracy', label: 'Accuracy', num: true, render: percent },
              { key: 'Macro precision', label: 'Precision', num: true, render: percent },
              { key: 'Macro recall', label: 'Recall', num: true, render: percent },
              { key: 'Macro F1', label: 'F1 score', num: true, render: percent },
            ]} />
          </Card>
          <Card
            title="Prediction comparison"
            sub={`${result.disagreements} disagreements across ${result.total} held-out records`}
            actions={<label className="row small"><input type="checkbox" checked={onlyDiff} onChange={(e) => setOnlyDiff(e.target.checked)} /> Show disagreements only</label>}
          >
            <DataTable search pageSize={12} rows={comparisons} rowClass={(row) => (row.match ? '' : 'mismatch')} columns={[
              { key: 'recordId', label: 'Record' },
              { key: 'actual', label: 'Actual' },
              { key: 'spark', label: 'Spark prediction' },
              { key: 'python', label: 'Python prediction' },
              { key: 'sparkConf', label: 'Spark confidence', num: true, render: percent },
              { key: 'pythonConf', label: 'Python confidence', num: true, render: percent },
              { key: 'match', label: 'Status', render: (value) => <Chip tone={value ? 'c-good' : 'c-bad'}>{value ? 'Match' : 'Mismatch'}</Chip> },
              { key: 'explanation', label: 'Explanation' },
            ]} />
          </Card>
        </div>
      );
    }}</Async>
  );
}
