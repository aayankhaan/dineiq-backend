import { mockApi } from './mock.js';
import { REPORT_FILTERS, pickFilters } from '../filterConfig.js';

const BASE = import.meta.env.VITE_API_URL || '/api';
export const USE_MOCK = import.meta.env.VITE_USE_MOCK === 'true';

let token = localStorage.getItem('dineiq_token');
export const getToken = () => token;
export function setToken(t) {
  token = t;
  t ? localStorage.setItem('dineiq_token', t) : localStorage.removeItem('dineiq_token');
}

export class ApiError extends Error {
  constructor(message, status) { super(message); this.status = status; }
}

// SRS lxiv – understandable errors for processing / model / Spark / database failures.
function friendly(status, body, path) {
  const detail = body?.detail || body?.message || body?.error;
  const text = typeof detail === 'string' ? detail : '';
  if (status === 401) return path === '/auth/login' ? 'Email or password is incorrect.' : 'Your session has expired. Please sign in again.';
  if (status === 403) return 'Your role does not have permission for this action.';
  if (status === 404) return 'This result has not been generated yet. Run the related processing job first.';
  if (status === 422 || status === 400) return text || 'Some of the values sent were not valid. Check the filters and try again.';
  if (status === 503 || /spark/i.test(text)) return text || 'The Spark cluster is not responding. Check the Spark job monitor and try again.';
  if (/model/i.test(path)) return text || 'The model service failed. Check the model version and logs.';
  if (status >= 500) return text || 'The server hit a database or processing error. Try again, or check the audit log.';
  return text || 'Something went wrong while loading this data.';
}

export async function api(path, { method = 'GET', params, body, blob = false } = {}) {
  if (USE_MOCK) return mockApi(path, { method, params, body: path === '/auth/login' ? { ...body, username: body.email } : body });
  const clean = params ? Object.fromEntries(Object.entries(params).filter(([, v]) => v !== '' && v != null)) : null;
  const qs = clean && Object.keys(clean).length ? '?' + new URLSearchParams(clean) : '';
  let res;
  try {
    res = await fetch(BASE + path + qs, {
      method,
      credentials: 'include',
      headers: { 'Content-Type': 'application/json', ...(token ? { Authorization: `Bearer ${token}` } : {}) },
      body: body ? JSON.stringify(body) : undefined,
    });
  } catch {
    throw new ApiError('Cannot reach the server. Check that the backend is running and VITE_API_URL is correct.', 0);
  }
  if (res.status === 401 && !path.startsWith('/auth/')) {
    setToken(null); localStorage.removeItem('dineiq_user'); window.location.href = '/login';
  }
  if (!res.ok) {
    let b = null; try { b = await res.json(); } catch { /* not json */ }
    throw new ApiError(friendly(res.status, b, path), res.status);
  }
  return blob ? res.blob() : res.json();
}

// Download a report or export. format: 'csv' | 'xlsx'
export async function download(kind, name, format = 'csv', params = {}) {
  params = pickFilters(params, REPORT_FILTERS[kind] || []);
  let blob;
  if (USE_MOCK) {
    const rows = await mockApi(`/reports/${kind}/rows`, { params });
    const cols = Object.keys(rows[0] || {});
    const esc = (v) => `"${String(v ?? '').replace(/"/g, '""')}"`;
    const csv = [cols.join(','), ...rows.map((r) => cols.map((c) => esc(r[c])).join(','))].join('\n');
    blob = new Blob([csv], { type: 'text/csv' });
    format = 'csv';
  } else {
    blob = await api(`/reports/${kind}/download`, { params: { ...params, format }, blob: true });
  }
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = `${name}.${format}`;
  a.click();
  URL.revokeObjectURL(a.href);
}
