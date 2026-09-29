export const PAGE_FILTERS = {
  '/': ['dateFrom', 'dateTo', 'location', 'item', 'category', 'channel', 'promotion'],
  '/recommendations': ['location', 'item', 'segment', 'promotion'],
  '/menu': ['location', 'item', 'category', 'cls', 'priceMin', 'priceMax', 'ratingMin', 'wasteMax'],
  '/pricing': ['item', 'category'],
  '/promotions': ['location', 'item', 'category', 'promotion'],
  '/basket': ['item'],
  '/ratings': ['dateFrom', 'dateTo', 'location', 'item'],
  '/customers': ['segment', 'channel'],
  '/peak': ['dateFrom', 'dateTo', 'location', 'channel', 'promotion'],
  '/forecast': ['location', 'item', 'category'],
  '/wastage': ['dateFrom', 'dateTo', 'location', 'item', 'category'],
  '/locations': ['location', 'channel'],
  '/reports': ['dateFrom', 'dateTo', 'location', 'item', 'category', 'segment', 'channel', 'promotion', 'cls', 'priceMin', 'priceMax', 'ratingMin', 'wasteMax'],
};

export const REPORT_FILTERS = {
  'menu-performance': PAGE_FILTERS['/menu'],
  profitability: PAGE_FILTERS['/menu'],
  'customer-segmentation': ['segment'],
  'market-basket': ['item'],
  'demand-forecast': ['category'],
  wastage: PAGE_FILTERS['/wastage'],
  promotions: PAGE_FILTERS['/promotions'],
  pricing: PAGE_FILTERS['/pricing'],
  'location-performance': ['location'],
  anomalies: PAGE_FILTERS['/ratings'],
  recommendations: PAGE_FILTERS['/recommendations'],
  'peak-period': PAGE_FILTERS['/peak'],
  'model-comparison': [],
};

export function endpointFilters(path, params = {}) {
  if (path === '/kpis/executive') return PAGE_FILTERS['/'];
  if (path === '/menu/items') return PAGE_FILTERS['/menu'];
  if (path === '/menu/slow-moving') return ['location', 'item', 'category'];
  if (path === '/customers/churn') return ['segment', 'channel'];
  if (path.startsWith('/customers/')) return ['segment'];
  if (path === '/basket/rules') return ['item'];
  if (path === '/peak') return PAGE_FILTERS['/peak'];
  if (path === '/forecast') return [params.level === 'item' ? 'item' : params.level === 'location' ? 'location' : 'category'];
  if (path === '/wastage') return PAGE_FILTERS['/wastage'];
  if (path === '/pricing/sensitivity') return PAGE_FILTERS['/pricing'];
  if (path === '/promotions') return PAGE_FILTERS['/promotions'];
  if (path === '/anomalies') return PAGE_FILTERS['/ratings'];
  if (path === '/locations') return ['location'];
  if (path === '/channels') return ['channel'];
  if (path === '/recommendations') return PAGE_FILTERS['/recommendations'];
  return [];
}

export function pickFilters(query, keys = []) {
  return Object.fromEntries(Object.entries(query || {}).filter(([key]) => keys.includes(key)));
}
