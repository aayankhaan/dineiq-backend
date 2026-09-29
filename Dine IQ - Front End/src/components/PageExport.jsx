import { useState } from 'react';
import { download } from '../api/client.js';
import { useAuth } from '../context/AuthContext.jsx';
import { useFilters } from '../context/FiltersContext.jsx';

const PAGE_REPORTS = {
  '/recommendations': ['recommendations', 'recommendations'],
  '/menu': ['menu-performance', 'menu_performance'],
  '/pricing': ['pricing', 'price_intelligence'],
  '/promotions': ['promotions', 'promotions'],
  '/basket': ['market-basket', 'market_basket'],
  '/ratings': ['anomalies', 'ratings_and_anomalies'],
  '/customers': ['customer-segmentation', 'customer_segments'],
  '/peak': ['peak-period', 'peak_periods'],
  '/forecast': ['demand-forecast', 'demand_forecast'],
  '/wastage': ['wastage', 'wastage'],
  '/locations': ['location-performance', 'location_performance'],
  '/models': ['model-comparison', 'spark_python_comparison'],
};

export default function PageExport({ path }) {
  const report = PAGE_REPORTS[path];
  const { canExport } = useAuth();
  const { query } = useFilters();
  const [busy, setBusy] = useState('');
  const [error, setError] = useState('');
  if (!report) return null;

  const run = async (format) => {
    setBusy(format); setError('');
    try {
      await download(report[0], `${report[1]}_${new Date().toISOString().slice(0, 10)}`, format, query);
    } catch (e) {
      setError(e.message);
    } finally {
      setBusy('');
    }
  };

  return (
    <div className="page-export">
      <div><b>Download this report</b><span>Uses the filters selected above.</span></div>
      {error && <span className="page-export-error" role="alert">{error}</span>}
      <button className="btn sm" disabled={!canExport || !!busy} onClick={() => run('csv')}>{busy === 'csv' ? 'Preparing…' : 'CSV'}</button>
      <button className="btn sm primary" disabled={!canExport || !!busy} onClick={() => run('xlsx')}>{busy === 'xlsx' ? 'Preparing…' : 'Excel'}</button>
    </div>
  );
}
