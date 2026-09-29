import { createContext, useContext, useMemo, useState } from 'react';

// SRS Step 48 – date range, location, item, category, segment, channel,
// promotion, performance class, price range, rating, wastage range.
export const EMPTY = {
  dateFrom: '', dateTo: '', location: '', item: '', category: '', segment: '',
  channel: '', promotion: '', cls: '', priceMin: '', priceMax: '', ratingMin: '', wasteMax: '',
};
const Ctx = createContext(null);

export function FiltersProvider({ children }) {
  const [filters, setFilters] = useState(EMPTY);
  const value = useMemo(() => {
    const query = Object.fromEntries(Object.entries(filters).filter(([, v]) => v !== '' && v != null));
    return {
      filters, query, activeCount: Object.keys(query).length,
      setFilter: (k, v) => setFilters((f) => ({ ...f, [k]: v })),
      reset: () => setFilters(EMPTY),
    };
  }, [filters]);
  return <Ctx.Provider value={value}>{children}</Ctx.Provider>;
}
export const useFilters = () => useContext(Ctx);
