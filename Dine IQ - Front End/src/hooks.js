import { useEffect, useMemo, useState } from 'react';
import { api } from './api/client.js';
import { useFilters } from './context/FiltersContext.jsx';
import { endpointFilters, pickFilters } from './filterConfig.js';

// Loads GET `path` with the global filters merged into the query string.
export function useApi(path, params = {}, { filtered = true, skip = false } = {}) {
  const { query } = useFilters();
  const merged = useMemo(() => ({ ...(filtered ? pickFilters(query, endpointFilters(path, params)) : {}), ...params }), [query, JSON.stringify(params), filtered, path]); // eslint-disable-line
  const key = path + JSON.stringify(merged);
  const [state, setState] = useState({ data: null, loading: !skip, error: null });
  const [tick, setTick] = useState(0);
  useEffect(() => {
    if (skip) return undefined;
    let live = true;
    setState((s) => ({ ...s, loading: true, error: null }));
    api(path, { params: merged })
      .then((data) => live && setState({ data, loading: false, error: null }))
      .catch((error) => live && setState({ data: null, loading: false, error }));
    return () => { live = false; };
  }, [key, tick, skip]); // eslint-disable-line
  return { ...state, reload: () => setTick((t) => t + 1) };
}
