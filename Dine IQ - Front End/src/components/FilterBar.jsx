import { useLocation } from 'react-router-dom';
import { useApi } from '../hooks.js';
import { useFilters } from '../context/FiltersContext.jsx';
import { PAGE_FILTERS } from '../filterConfig.js';
import { Field, Select } from './ui.jsx';

export default function FilterBar() {
  const { pathname } = useLocation();
  const visible = PAGE_FILTERS[pathname] || [];
  const { filters, setFilter, reset } = useFilters();
  const { data: meta } = useApi('/meta/filters', {}, { filtered: false });
  const set = (key) => (value) => setFilter(key, value);
  const input = (key, type = 'text', placeholder = '') => (
    <input className="input" type={type} placeholder={placeholder} value={filters[key]} onChange={(e) => setFilter(key, e.target.value)} />
  );
  const controls = {
    dateFrom: <Field label="From date">{input('dateFrom', 'date')}</Field>,
    dateTo: <Field label="To date">{input('dateTo', 'date')}</Field>,
    location: <Field label="Location"><Select value={filters.location} onChange={set('location')} options={meta?.locations || []} /></Field>,
    item: <Field label="Menu item"><Select value={filters.item} onChange={set('item')} options={meta?.items || []} /></Field>,
    category: <Field label="Category"><Select value={filters.category} onChange={set('category')} options={meta?.categories || []} /></Field>,
    segment: <Field label="Customer segment"><Select value={filters.segment} onChange={set('segment')} options={meta?.segments || []} /></Field>,
    channel: <Field label="Ordering channel"><Select value={filters.channel} onChange={set('channel')} options={meta?.channels || []} /></Field>,
    promotion: <Field label="Promotion"><Select value={filters.promotion} onChange={set('promotion')} options={meta?.promotions || []} /></Field>,
    cls: <Field label="Performance class"><Select value={filters.cls} onChange={set('cls')} options={meta?.classes || []} /></Field>,
    priceMin: <Field label="Minimum price">{input('priceMin', 'number', '0')}</Field>,
    priceMax: <Field label="Maximum price">{input('priceMax', 'number', '1000')}</Field>,
    ratingMin: <Field label="Minimum rating">{input('ratingMin', 'number', '1-5')}</Field>,
    wasteMax: <Field label="Maximum wastage %">{input('wasteMax', 'number', '100')}</Field>,
  };
  return (
    <div className="filters">
      {visible.map((key) => <div key={key}>{controls[key]}</div>)}
      <button className="btn" onClick={reset}>Clear page filters</button>
    </div>
  );
}
