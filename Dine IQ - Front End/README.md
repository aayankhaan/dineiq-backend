# DineIQ Analytics – React frontend

Vite + React 18 + React Router + Recharts. No UI framework; all styling is in `src/styles.css`.

## Run
```bash
npm install
npm run dev        # http://localhost:5173
```
- `VITE_USE_MOCK=true` (in `.env`) runs with built-in demo data. Demo logins: admin/admin123, sara.analyst/analyst123, reza.regional/regional123, omar.mgr/manager123.
- When your endpoints are ready: set `VITE_USE_MOCK=false` and `VITE_PROXY_TARGET` to your backend URL. Endpoint list: `src/api/endpoints.md`.

## Where things are
- `src/nav.js` – all pages, sidebar groups and which roles see them (RBAC).
- `src/api/client.js` – fetch wrapper, token, friendly error messages, report downloads.
- `src/hooks.js` – `useApi(path, params)`; merges the global filters automatically.
- `src/pages/*` – one file per dashboard. `src/components/ui.jsx` – cards, KPIs, sortable/paginated table, chips.
- Build for deploy: `npm run build` → `dist/`.
