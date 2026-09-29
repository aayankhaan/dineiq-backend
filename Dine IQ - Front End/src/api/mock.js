// Demo data so the UI runs without a backend (VITE_USE_MOCK=true).
// Every shape here documents the JSON your real endpoints should return – see src/api/endpoints.md.
function rng(seed) { let s = seed; return () => { s = (s * 1664525 + 1013904223) % 4294967296; return s / 4294967296; }; }
const R = rng(2026);
const rnd = (a, b) => a + R() * (b - a);
const ri = (a, b) => Math.floor(rnd(a, b + 1));
const pick = (a) => a[Math.floor(R() * a.length)];
const r1 = (n) => Math.round(n * 10) / 10;
const r2 = (n) => Math.round(n * 100) / 100;
const median = (a) => { const s = [...a].sort((x, y) => x - y); return s[Math.floor(s.length / 2)]; };

const MODEL_VERSION = { spark: 'spark-rf-v1.3.0', python: 'sk-xgb-v1.2.1' };
const END = new Date('2026-09-27');
const dayStr = (back) => { const d = new Date(END); d.setDate(d.getDate() - back); return d.toISOString().slice(0, 10); };

export const CATEGORIES = ['Starters', 'Soups', 'Burgers', 'Pizza', 'Pasta', 'Salads', 'Mains', 'Sides', 'Desserts', 'Beverages'];
export const LOCATIONS = ['Downtown', 'Harbor Point', 'Uptown', 'Airport Road', 'Old Town', 'Lakeside'];
export const CHANNELS = ['Dine-in', 'Takeaway', 'Website / App', 'Delivery platform'];
const NAMES = {
  Starters: ['Crispy Calamari', 'Garlic Bread', 'Stuffed Mushrooms', 'Loaded Nachos'],
  Soups: ['Tomato Basil Soup', 'Mushroom Bisque', 'Chicken Corn Soup'],
  Burgers: ['Classic Beef Burger', 'Crispy Chicken Burger', 'Smoked BBQ Burger', 'Veggie Burger'],
  Pizza: ['Margherita Pizza', 'Pepperoni Pizza', 'Truffle Mushroom Pizza', 'Four Cheese Pizza'],
  Pasta: ['Creamy Risotto', 'Pasta Alfredo', 'Truffle Pasta', 'Penne Arrabbiata'],
  Salads: ['Caesar Salad', 'Greek Salad', 'Quinoa Bowl'],
  Mains: ['Grilled Salmon', 'Paneer Tikka', 'Steak Frites', 'Stuffed Bell Peppers'],
  Sides: ['Truffle Fries', 'Onion Rings', 'Coleslaw'],
  Desserts: ['Chocolate Lava Cake', 'Cheesecake', 'Tiramisu'],
  Beverages: ['Fresh Lemonade', 'Cold Coffee', 'Mango Smoothie'],
};
const SEGMENTS = ['High-Value Loyal', 'Frequent', 'Promotion-Driven', 'At-Risk', 'New', 'Occasional'];
const PROMOS = ['Weekend 20% Off', 'Buy 1 Get 1 Pizza', 'Happy Hour Drinks', 'Student Combo', 'Free Dessert Over 30', 'Delivery Free Week', 'Lunch Express -15%', 'Festival Feast'];

// ---------- menu items ----------
let n = 0;
const items = [];
CATEGORIES.forEach((cat) => NAMES[cat].forEach((name) => {
  n += 1;
  const price = r2(cat === 'Beverages' ? rnd(2.5, 6) : cat === 'Mains' ? rnd(14, 28) : rnd(5, 18));
  const unitCost = r2(price * rnd(0.26, 0.6));
  const qty = ri(400, 9000);
  const promoDep = r2(rnd(0.02, 0.45));
  const disc = promoDep * 0.18;
  items.push({
    id: `M${String(n).padStart(3, '0')}`, name, category: cat, price, unitCost, qty,
    revenue: r2(qty * price * (1 - disc)), rating: r1(rnd(3.1, 4.8)), repeatRate: r2(rnd(0.08, 0.6)),
    wastePct: r1(rnd(1.5, 14)), promoDep, trend: r1(rnd(-18, 22)), orderFreq: r2(rnd(0.5, 14)),
    daysSinceLast: ri(0, 6), elasticity: r2(-rnd(0.2, 2.4)), tags: [],
  });
}));
// engineered "tricky" cases the SRS asks the app to surface
const set = (i, patch) => Object.assign(items[i], patch);
set(0, { qty: 9400, price: 9, unitCost: 9.8, rating: 4.1 });                          // high-selling, loss-making
set(8, { qty: 380, price: 16, unitCost: 5, rating: 4.7, repeatRate: 0.58, wastePct: 2.2 }); // profitable, rarely bought
set(15, { qty: 8200, wastePct: 21 });                                                     // popular, excessive wastage
set(20, { rating: 4.8, price: 7, unitCost: 5.6 });                                        // highly rated, poor profitability
set(24, { rating: 3.0, qty: 8800 });                                                      // low rated, high sales
set(30, { promoDep: 0.71 });                                                              // promotion dependent
set(34, { qty: 210, orderFreq: 0.4, daysSinceLast: 19, trend: -24 });                     // slow mover
items.forEach((it) => {
  it.cost = r2(it.qty * it.unitCost);
  it.revenue = r2(it.qty * it.price * (1 - it.promoDep * 0.18));
  it.profit = r2(it.revenue - it.cost);
  it.marginPct = r1((it.profit / it.revenue) * 100);
});
const medQty = median(items.map((i) => i.qty));
const medMargin = median(items.map((i) => i.marginPct));
const q75 = [...items].map((i) => i.qty).sort((a, b) => a - b)[Math.floor(items.length * 0.75)];
items.forEach((it, i) => {
  const hiD = it.qty >= medQty; const hiM = it.marginPct >= medMargin;
  if (it.wastePct > 18 || it.rating < 3.3 || (!hiD && !hiM && it.repeatRate < 0.3)) it.cls = 'Low Performer';
  else if (hiD && hiM) it.cls = 'Profit Driver';
  else if (hiD) it.cls = 'Volume Driver';
  else if (hiM || it.rating >= 4.3 || it.repeatRate >= 0.45) it.cls = 'Hidden Opportunity';
  else it.cls = 'Low Performer';
  it.score = r2(rnd(0.4, 0.98));
  if (it.qty >= q75 && it.profit < 0) it.tags.push('High-selling, loss-making');
  if (it.marginPct >= medMargin * 1.3 && it.qty < medQty * 0.5) it.tags.push('Profitable but rarely purchased');
  if (it.qty >= medQty && it.wastePct > 15) it.tags.push('Popular with excessive wastage');
  if (it.rating >= 4.5 && it.marginPct < medMargin * 0.7) it.tags.push('Highly rated, poor profitability');
  if (it.rating < 3.4 && it.qty >= medQty) it.tags.push('Low-rated, high sales');
  if (it.promoDep > 0.5) it.tags.push('Promotion-dependent');
  if (i === 5) it.tags.push('New item, insufficient history');
  if (i === 12 || i === 27) it.tags.push('Seasonal item');
  if (i === 18) it.tags.push('Performs well only on weekends');
  if (i === 3 || i === 22) it.tags.push('Performs differently across locations');
});
const sparkOf = (it) => it.cls;
const pyOf = (it) => (R() < 0.08 ? pick(['Profit Driver', 'Volume Driver', 'Hidden Opportunity', 'Low Performer']) : it.cls);

// ---------- helpers for series ----------
const wk = (d) => { const g = new Date(d).getDay(); return g === 0 || g === 5 || g === 6 ? 1.28 : 1; };
const series = (base, days = 120) => Array.from({ length: days }, (_, k) => {
  const back = days - 1 - k; const d = dayStr(back);
  return { date: d, v: Math.round(base * wk(d) * (1 + 0.004 * k) * rnd(0.86, 1.14)) };
});
const revSeries = series(4200);
const trend = revSeries.map((p) => ({ date: p.date, revenue: p.v, profit: Math.round(p.v * rnd(0.36, 0.44)), orders: Math.round(p.v / 22) }));
const sum = (a, f) => a.reduce((s, x) => s + f(x), 0);

const heat = Array.from({ length: 7 }, (_, d) => Array.from({ length: 24 }, (_, h) => {
  const lunch = Math.exp(-((h - 13) ** 2) / 4); const dinner = Math.exp(-((h - 20) ** 2) / 5);
  return Math.round((lunch * 60 + dinner * 100) * (d >= 4 ? 1.35 : 1) * rnd(0.85, 1.15));
}));

const locs = LOCATIONS.map((name, i) => {
  const revenue = ri(210, 480) * 1000;
  return {
    id: `L${i + 1}`, name, revenue, profit: Math.round(revenue * rnd(0.3, 0.46)), aov: r2(rnd(19, 31)),
    customers: ri(6, 14) * 1000, repeatRate: r2(rnd(0.34, 0.66)), wastePct: r1(rnd(3.2, 10.5)), rating: r1(rnd(3.8, 4.7)),
    promoRoi: r2(rnd(-0.2, 1.6)), health: ri(52, 94),
  };
});
const channels = CHANNELS.map((name) => ({
  name, orders: ri(12, 60) * 1000, basket: r1(rnd(2.1, 4.6)), aov: r2(rnd(16, 34)), discountPct: r1(rnd(2, 18)),
  promoShare: r1(rnd(8, 38)), marginPct: r1(rnd(24, 52)), peak: pick(['12–14h', '13–15h', '19–21h', '20–22h']),
  topCategory: pick(CATEGORIES),
}));

const rules = [];
const ids = items.map((i) => i.id);
for (let k = 0; k < 22; k++) {
  const a = pick(items); let b = pick(items); if (b.id === a.id) b = items[(items.indexOf(a) + 3) % items.length];
  rules.push({ antecedent: a.name, consequent: b.name, aId: a.id, bId: b.id, support: r2(rnd(0.008, 0.06)), confidence: r2(rnd(0.18, 0.72)), lift: r2(rnd(0.9, 4.2)) });
}
rules.sort((x, y) => y.lift - x.lift);

const customers = {
  segments: SEGMENTS.map((name, i) => ({
    name, count: [4200, 9800, 12400, 6100, 8300, 15200][i], avgOrder: r2(rnd(14, 48)), recencyDays: [9, 14, 21, 74, 6, 39][i],
    frequency: r1([9.4, 6.1, 3.8, 1.9, 1.2, 1.7][i]), monetary: Math.round(rnd(60, 780)),
    strategy: ['Reward with early access and tiered loyalty perks', 'Encourage a higher basket with combos', 'Use targeted, capped offers; watch margin', 'Send win-back offers within 7 days', 'Onboard with a second-visit incentive', 'Nudge with time-of-day recommendations'][i],
  })),
  rfmBands: [1, 2, 3, 4, 5].map((s) => ({ score: s, recency: ri(4, 20) * 1000, frequency: ri(4, 20) * 1000, monetary: ri(4, 20) * 1000 })),
  top: Array.from({ length: 10 }, (_, i) => ({ id: `C${10420 + i * 37}`, segment: 'High-Value Loyal', recency: ri(1, 8), frequency: ri(18, 44), monetary: ri(900, 3200), favCategory: pick(CATEGORIES), channel: pick(CHANNELS) })),
  churn: Array.from({ length: 12 }, (_, i) => ({
    id: `C${20110 + i * 53}`, risk: r2(rnd(0.62, 0.97)), recency: ri(45, 130), frequencyChange: -ri(20, 70), spendChange: -ri(15, 60), categoryDiversityChange: -ri(0, 4),
    reasons: pick([['Recency up 3x', 'Visits halved'], ['Spend falling 4 months', 'Fewer categories'], ['No visit in 90+ days'], ['Only orders during promotions', 'Frequency falling']]),
  })).sort((a, b) => b.risk - a.risk),
};

const wastageItems = [...items].sort((a, b) => b.wastePct * b.cost - a.wastePct * a.cost).slice(0, 10).map((i) => ({ id: i.id, name: i.name, category: i.category, wastePct: i.wastePct, units: Math.round((i.qty * i.wastePct) / 100), cost: Math.round(i.unitCost * i.qty * i.wastePct / 100) }));
const wastage = {
  kpis: { totalUnits: sum(wastageItems, (x) => x.units) * 3, cost: sum(wastageItems, (x) => x.cost) * 3, pct: 6.8 },
  byItem: wastageItems,
  byLocation: locs.map((l) => ({ name: l.name, pct: l.wastePct, cost: Math.round(l.revenue * l.wastePct / 100 * 0.3) })),
  byReason: [{ name: 'Overproduction', value: 38 }, { name: 'Spoilage', value: 27 }, { name: 'Food prep', value: 21 }, { name: 'Customer return', value: 9 }, { name: 'Other', value: 5 }],
  trend: Array.from({ length: 30 }, (_, k) => ({ date: dayStr(29 - k), cost: Math.round(rnd(280, 520) * wk(dayStr(29 - k))) })),
  risk: Array.from({ length: 12 }, () => { const it = pick(items); return { item: it.name, itemId: it.id, location: pick(LOCATIONS), day: pick(['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']), risk: r2(rnd(0.55, 0.96)), prepQty: ri(40, 160), forecastDemand: ri(25, 120), expectedCost: ri(40, 380) }; }).sort((a, b) => b.risk - a.risk),
};

const pricing = [...items].sort(() => R() - 0.5).slice(0, 14).map((it) => {
  const pc = r1(rnd(-12, 15)); const dc = r1(pc * it.elasticity + rnd(-2, 2));
  const el = r2(dc / (pc || 1));
  return { id: it.id, name: it.name, category: it.category, price: it.price, priceChangePct: pc, demandChangePct: dc, elasticity: el, sensitivity: Math.abs(el) > 1.2 ? 'Highly Price Sensitive' : Math.abs(el) > 0.6 ? 'Moderately Price Sensitive' : 'Low Price Sensitivity' };
});
const priceHistory = (id) => { const it = items.find((x) => x.id === id) || items[0]; let p = it.price * 0.9; return Array.from({ length: 12 }, (_, m) => { if (m % 4 === 2) p *= 1 + rnd(0.02, 0.09); return { month: `M${m + 1}`, price: r2(p), demand: Math.round(it.qty / 12 * (1 + (it.price - p) / it.price * 2.2) * rnd(0.9, 1.1)) }; }); };

const promos = PROMOS.map((name, i) => {
  const uplift = r1(rnd(-4, 42)); const marginDelta = r1(rnd(-22, 8)); const profitDelta = r1(uplift * 0.4 + marginDelta * 1.2);
  const flags = []; if (uplift > 10 && profitDelta < 0) flags.push('Sales up, profit down');
  if (marginDelta < -14) flags.push('Average margin collapsed'); if (i === 2) flags.push('Increased wastage');
  if (i === 5) flags.push('Customers buy only while discount is active'); if (i === 1) flags.push('Cannibalised a higher-margin item');
  return { id: `P${i + 1}`, name, period: `${dayStr(120 - i * 12)} to ${dayStr(105 - i * 12)}`, orderUplift: uplift, revenueChange: r1(uplift * 0.8), marginChange: marginDelta, profitChange: profitDelta, newCustomers: ri(80, 1900), repeatRate: r2(rnd(0.1, 0.5)), aovChange: r1(rnd(-9, 12)), wasteChange: r1(rnd(-2, 9)), postPromoDrop: r1(rnd(0, 26)), flags, verdict: flags.length ? 'Promotion trap' : profitDelta > 3 ? 'Effective' : 'Neutral' };
});

const anomalies = {
  ratings: [
    { id: 'RA1', date: dayStr(4), entity: items[3].name, type: 'Sudden rating spike', detail: '38 five-star ratings in 2 hours; avg jumped 3.6 → 4.7', severity: 'High' },
    { id: 'RA2', date: dayStr(11), entity: items[12].name, type: 'Sudden rating drop', detail: 'Average fell 4.3 → 3.4 after a recipe change', severity: 'Medium' },
    { id: 'RA3', date: dayStr(18), entity: items[9].name, type: 'Excessive identical ratings', detail: '92% of ratings are exactly 5.0 (expected ≈ 41%)', severity: 'High' },
    { id: 'RA4', date: dayStr(7), entity: items[21].name, type: 'Rating inconsistent with purchases', detail: '214 ratings but only 61 recorded purchases', severity: 'High' },
  ],
  sales: [
    { id: 'SA1', date: dayStr(2), entity: 'Harbor Point', type: 'Sudden sales spike', detail: 'Revenue 2.9× the 4-week Saturday average', severity: 'Medium' },
    { id: 'SA2', date: dayStr(9), entity: 'Order O-884213', type: 'Abnormally high order value', detail: `Order value ${'$'}1,940 vs median ${'$'}22`, severity: 'High' },
    { id: 'SA3', date: dayStr(15), entity: 'Uptown', type: 'Sudden sales drop', detail: 'Orders −46% vs previous week, no closure recorded', severity: 'High' },
    { id: 'SA4', date: dayStr(21), entity: 'Order O-771902', type: 'Unusual discount', detail: '78% discount applied, no matching promotion', severity: 'Medium' },
    { id: 'SA5', date: dayStr(26), entity: 'Order O-703318', type: 'Duplicate transaction', detail: 'Identical order lines posted twice within 40 seconds', severity: 'Low' },
  ],
};

const recs = [
  { id: 'R1', priority: 'Critical', type: 'Reprice', action: `Review price of ${items[0].name}`, entity: items[0].id, impact: 8400, evidence: [`Unit cost ${'$'}${items[0].unitCost} exceeds price ${'$'}${items[0].price}`, `Sold ${items[0].qty.toLocaleString()} units (top 5% volume)`, 'Loss grows with every sale'] },
  { id: 'R2', priority: 'High', type: 'Promote', action: `Promote ${items[8].name}`, entity: items[8].id, impact: 3100, evidence: [`Margin ${items[8].marginPct}% (top quartile)`, `${items[8].rating} average rating`, 'Low wastage', 'Low order frequency', 'Strong repeat purchase among existing buyers'] },
  { id: 'R3', priority: 'High', type: 'Reduce prep', action: `Reduce preparation quantity of ${items[15].name}`, entity: items[15].id, impact: 2600, evidence: [`Wastage ${items[15].wastePct}% vs 6.8% average`, 'Forecast demand 12% below current prep quantity', 'Weekday overproduction is the main reason'] },
  { id: 'R4', priority: 'High', type: 'Review promotion', action: `Stop or redesign "${promos[5].name}"`, entity: promos[5].id, impact: 4700, evidence: [`Profit ${promos[5].profitChange}% vs baseline`, `${promos[5].postPromoDrop}% order drop after the promotion ends`, 'Buyers return only while discount is active'] },
  { id: 'R5', priority: 'Medium', type: 'Bundle', action: `Bundle ${rules[0].antecedent} + ${rules[0].consequent}`, entity: rules[0].aId, impact: 1900, evidence: [`Lift ${rules[0].lift}`, `Confidence ${rules[0].confidence}`, `Support ${rules[0].support}`] },
  { id: 'R6', priority: 'Medium', type: 'Stock up', action: 'Increase stock before Friday 19:00–22:00 peak at Harbor Point', entity: 'L2', impact: 2200, evidence: ['Forecast demand +31% vs weekly average', 'Two stock-outs on the last 3 Fridays'] },
  { id: 'R7', priority: 'Critical', type: 'Investigate', action: 'Investigate Uptown sales drop', entity: 'L3', impact: 6100, evidence: ['Orders −46% week on week', 'No closure or price change recorded', 'Rating unchanged, pointing to a data or operations issue'] },
  { id: 'R8', priority: 'Medium', type: 'Target segment', action: 'Send win-back offer to 6,100 At-Risk customers', entity: 'At-Risk', impact: 3800, evidence: ['Average recency 74 days', 'Frequency down 48% in 90 days', 'Historic average order value 31% above the mean'] },
  { id: 'R9', priority: 'Low', type: 'Redesign', action: `Redesign or remove ${items[34].name}`, entity: items[34].id, impact: 700, evidence: ['210 units sold, 19 days since last order', 'Trend −24%', 'Classified Low Performer by both pipelines'] },
];

// ---------- model comparison ----------
function comparison(task) {
  const numeric = task === 'demand';
  const labels = { menu_class: ['Profit Driver', 'Volume Driver', 'Hidden Opportunity', 'Low Performer'], customer_segment: SEGMENTS, wastage_risk: ['Low', 'Medium', 'High'] }[task];
  const rows = Array.from({ length: 120 }, (_, i) => {
    if (numeric) {
      const actual = ri(20, 220); const s = Math.round(actual * rnd(0.88, 1.12)); const p = Math.round(actual * rnd(0.86, 1.14));
      const diff = Math.abs(s - p); return { recordId: `D-${String(i + 1).padStart(5, '0')}`, actual, spark: s, python: p, sparkConf: null, pythonConf: null, match: diff <= Math.max(3, actual * 0.05), diff, explanation: diff > actual * 0.05 ? 'Random Forest smooths peak-day spikes; XGBoost follows lagged features more closely' : '' };
    }
    const actual = pick(labels); const s = R() < 0.9 ? actual : pick(labels); const p = R() < 0.9 ? actual : pick(labels);
    const match = s === p;
    return { recordId: `${task === 'menu_class' ? 'M' : 'C'}-${String(i + 1).padStart(5, '0')}`, actual, spark: s, python: p, sparkConf: r2(rnd(0.5, 0.99)), pythonConf: r2(rnd(0.5, 0.99)), match, diff: null, explanation: match ? '' : pick(['Record sits on a class boundary; each model places the threshold differently', 'Spark bins continuous features differently to sklearn', 'Item has few training samples for this class', 'Promotion-dependency feature scaled differently in the two pipelines']) };
  });
  const agree = rows.filter((r) => r.match).length;
  return {
    task, versions: MODEL_VERSION, total: rows.length, agreement: r1((agree / rows.length) * 100), disagreements: rows.length - agree, rows,
    metrics: numeric
      ? { spark: { MAE: 8.4, RMSE: 11.9, MAPE: '7.1%', R2: 0.88 }, python: { MAE: 7.6, RMSE: 10.8, MAPE: '6.4%', R2: 0.9 }, baseline: { MAE: 15.2, RMSE: 20.3, MAPE: '13.8%', R2: 0.62 } }
      : { spark: { Accuracy: 0.884, Precision: 0.879, Recall: 0.871, F1: 0.874 }, python: { Accuracy: 0.903, Precision: 0.898, Recall: 0.894, F1: 0.896 } },
    sparkCandidates: [{ name: 'Logistic Regression', score: 0.812 }, { name: 'Decision Tree', score: 0.846 }, { name: 'Random Forest (selected)', score: 0.874 }, { name: 'Gradient-Boosted Trees', score: 0.869 }],
  };
}

const jobs = [
  { id: 'J-2091', name: 'Ingest orders + order_items (14 files)', engine: 'Spark', status: 'Completed', progress: 100, duration: '2m 41s', rows: 1_284_310, started: `${dayStr(0)} 02:00` },
  { id: 'J-2092', name: 'Data quality assessment', engine: 'Spark', status: 'Completed', progress: 100, duration: '1m 12s', rows: 1_284_310, started: `${dayStr(0)} 02:04` },
  { id: 'J-2093', name: 'Clean + write Parquet (partition: month, location)', engine: 'Spark', status: 'Completed', progress: 100, duration: '3m 05s', rows: 1_251_880, started: `${dayStr(0)} 02:06` },
  { id: 'J-2094', name: 'Feature engineering', engine: 'Spark SQL', status: 'Running', progress: 64, duration: '1m 48s', rows: 812_400, started: `${dayStr(0)} 02:10` },
  { id: 'J-2095', name: 'Train MLlib classifiers (3 algorithms)', engine: 'Spark MLlib', status: 'Queued', progress: 0, duration: '—', rows: 0, started: '—' },
  { id: 'J-2088', name: 'Python XGBoost demand model', engine: 'Python', status: 'Completed', progress: 100, duration: '54s', rows: 1_251_880, started: `${dayStr(1)} 02:30` },
  { id: 'J-2087', name: 'Basket analysis (FP-Growth)', engine: 'Spark MLlib', status: 'Failed', progress: 37, duration: '4m 02s', rows: 0, started: `${dayStr(1)} 02:20`, error: 'Executor lost: out of memory. Lower minSupport batch size or raise spark.executor.memory.' },
];
const dq = [
  { issue: 'Missing customer IDs', found: 18420, action: 'Quarantined', rule: 'DQ-01' }, { issue: 'Duplicate orders', found: 6210, action: 'Removed (kept latest)', rule: 'DQ-02' },
  { issue: 'Duplicate order lines', found: 9880, action: 'Removed', rule: 'DQ-03' }, { issue: 'Negative quantities', found: 1342, action: 'Quarantined', rule: 'DQ-04' },
  { issue: 'Invalid menu prices', found: 764, action: 'Corrected from pricing history', rule: 'DQ-05' }, { issue: 'Invalid dates', found: 402, action: 'Quarantined', rule: 'DQ-06' },
  { issue: 'Ratings outside 1–5', found: 2310, action: 'Removed', rule: 'DQ-07' }, { issue: 'Impossible wastage quantity', found: 511, action: 'Capped at prepared qty', rule: 'DQ-08' },
  { issue: 'Incorrect discounts (>100% or <0)', found: 288, action: 'Set to 0, flagged', rule: 'DQ-09' }, { issue: 'Cancelled transactions', found: 21870, action: 'Excluded from revenue', rule: 'DQ-10' },
  { issue: 'Inconsistent units (g / kg)', found: 1904, action: 'Converted to kg', rule: 'DQ-11' }, { issue: 'Invalid location references', found: 97, action: 'Quarantined', rule: 'DQ-12' },
];
const audit = Array.from({ length: 14 }, (_, i) => ({ time: `${dayStr(Math.floor(i / 3))} ${String(9 + (i % 9)).padStart(2, '0')}:${String(ri(0, 59)).padStart(2, '0')}`, user: pick(['admin', 'sara.analyst', 'omar.mgr']), action: pick(['Ran Spark job J-2091', 'Exported menu-performance.csv', 'Generated demand forecast', 'Created user account', 'Changed location L4', 'Ran what-if scenario', 'Trained model spark-rf-v1.3.0']), area: pick(['Job', 'Export', 'Prediction', 'Admin']) }));
const users = [
  { id: 1, username: 'admin', name: 'Aisha Admin', role: 'admin', location: 'All' }, { id: 2, username: 'sara.analyst', name: 'Sara Khan', role: 'analyst', location: 'All' },
  { id: 3, username: 'reza.regional', name: 'Reza Malik', role: 'regional_manager', location: 'Downtown, Uptown' }, { id: 4, username: 'omar.mgr', name: 'Omar Sheikh', role: 'manager', location: 'Harbor Point' },
];
const demoLogins = { admin: ['admin123', 0], analyst: ['analyst123', 1], regional: ['regional123', 2], manager: ['manager123', 3] };

// ---------- what-if ----------
function whatIf({ scenario, itemId, value }) {
  const it = items.find((x) => x.id === itemId) || items[0];
  const v = Number(value) || 0;
  const demand0 = it.qty / 4; const price0 = it.revenue / it.qty; const cost = it.unitCost;
  const wasteUnits0 = (demand0 * it.wastePct) / 100;
  let demand = demand0; let price = price0; let wastePct = it.wastePct; const notes = [];
  if (scenario === 'price') { price = price0 * (1 + v / 100); demand = demand0 * (1 + (it.elasticity * v) / 100); notes.push(`Uses this item's estimated price elasticity (${it.elasticity}).`); }
  if (scenario === 'discount') { price = price0 * (1 - v / 100); demand = demand0 * (1 + (Math.abs(it.elasticity) * v) / 100); notes.push('Assumes the discount applies to all orders of this item.'); }
  if (scenario === 'promo_freq') { demand = demand0 * (1 + 0.04 * v); price = price0 * (1 - 0.02 * v); wastePct += 0.6 * v; notes.push('Each extra promotion per month: +4% demand, −2% average price, +0.6 pts wastage.'); }
  if (scenario === 'remove') { demand = 0; notes.push('Assumes no sales transfer to other items; real cannibalisation would recover part of the loss.'); }
  if (scenario === 'prep') { wastePct = Math.max(0, it.wastePct * (1 + (v / 100) * 1.4)); if (v < -15) { demand = demand0 * (1 + ((v + 15) / 100) * 0.5); notes.push('Cutting preparation beyond 15% risks lost sales.'); } }
  if (scenario === 'demand') demand = demand0 * (1 + v / 100);
  if (scenario === 'waste') wastePct = Math.max(0, v);
  const calc = (d, p, w) => { const revenue = d * p; const contribution = revenue - d * cost; const wasteCost = ((d * w) / 100) * cost; return { revenue: Math.round(revenue), contribution: Math.round(contribution), demand: Math.round(d), wastage: Math.round(wasteCost), profit: Math.round(contribution - wasteCost) }; };
  return { estimate: true, item: it.name, period: 'per month', baseline: calc(demand0, price0, it.wastePct), simulated: calc(demand, price, wastePct), notes, wasteUnitsBaseline: Math.round(wasteUnits0) };
}

// ---------- forecasting ----------
function forecast({ horizon = 14, level = 'category' }) {
  const H = Number(horizon) || 14; const base = level === 'item' ? 120 : level === 'location' ? 900 : 480;
  const hist = series(base, 60); const testStart = hist.length - 14;
  const rows = hist.map((p, i) => ({ date: p.date, actual: p.v, predicted: i >= testStart ? Math.round(p.v * rnd(0.92, 1.08)) : null, phase: i >= testStart ? 'test' : 'train' }));
  for (let k = 1; k <= H; k++) { const d = new Date(END); d.setDate(d.getDate() + k); const ds = d.toISOString().slice(0, 10); const f = Math.round(base * wk(ds) * (1 + 0.004 * (60 + k))); rows.push({ date: ds, actual: null, predicted: f, lo: Math.round(f * 0.85), hi: Math.round(f * 1.15), phase: 'future' }); }
  return {
    versions: MODEL_VERSION, split: { trainEnd: hist[testStart - 1].date, testStart: hist[testStart].date, method: 'Chronological – train on earlier weeks, test on later unseen weeks' },
    series: rows, metrics: { MAE: r1(base * 0.06), RMSE: r1(base * 0.08), MAPE: '6.2%', R2: 0.89, baselineMAE: r1(base * 0.12), baselineMAPE: '12.4%', baselineName: 'Seasonal naive (same weekday last week)' },
    risk: rows.filter((r) => r.phase === 'future' && r.predicted > base * 1.3).slice(0, 5).map((r) => ({ date: r.date, predicted: r.predicted, note: 'Forecast well above weekly average – stock up' })),
  };
}
const peak = () => ({
  heat, byHour: Array.from({ length: 24 }, (_, h) => ({ hour: h, orders: Math.round(sum(heat, (row) => row[h]) / 7) })),
  byDay: ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'].map((d, i) => ({ day: d, orders: Math.round(sum(heat[i], (x) => x)) })),
  monthly: ['Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep'].map((m) => ({ month: m, orders: ri(28, 46) * 1000 })),
  seasonal: [{ season: 'Winter', index: 1.12 }, { season: 'Spring', index: 0.94 }, { season: 'Summer', index: 0.88 }, { season: 'Autumn', index: 1.06 }],
  byLocation: locs.map((l) => ({ location: l.name, peakHour: pick(['13:00', '14:00', '20:00', '21:00']), peakDay: pick(['Fri', 'Sat', 'Sun']) })),
  dineVsDelivery: Array.from({ length: 24 }, (_, h) => ({ hour: h, dineIn: Math.round(sum(heat, (row) => row[h]) / 7 * 0.6), delivery: Math.round(sum(heat, (row) => row[h]) / 7 * 0.4 * (h > 18 ? 1.4 : 0.8)) })),
  weekendRatio: 0.41,
});

const reportRows = (kind) => ({
  'menu-performance': items.map(({ tags, ...i }) => ({ ...i, tags: tags.join('; ') })), profitability: items.map((i) => ({ id: i.id, name: i.name, revenue: i.revenue, cost: i.cost, profit: i.profit, marginPct: i.marginPct })),
  'customer-segmentation': customers.segments, 'market-basket': rules, 'demand-forecast': forecast({}).series, wastage: wastage.byItem, promotions: promos.map((p) => ({ ...p, flags: p.flags.join('; ') })),
  pricing, 'location-performance': locs, anomalies: [...anomalies.sales, ...anomalies.ratings], recommendations: recs.map((r) => ({ ...r, evidence: r.evidence.join('; ') })), 'model-comparison': comparison('menu_class').rows,
}[kind] || []);

const applyItemFilters = (list, p = {}) => list.filter((i) =>
  (!p.category || i.category === p.category) && (!p.cls || i.cls === p.cls) && (!p.item || i.id === p.item) &&
  (!p.priceMin || i.price >= +p.priceMin) && (!p.priceMax || i.price <= +p.priceMax) && (!p.ratingMin || i.rating >= +p.ratingMin) && (!p.wasteMax || i.wastePct <= +p.wasteMax));

export function mockApi(path, { method = 'GET', params = {}, body } = {}) {
  const p = params || {};
  const out = (() => {
    if (path === '/auth/login') {
      const key = Object.keys(demoLogins).find((k) => body.username === (k === 'admin' ? 'admin' : k) || users.some((u) => u.username === body.username && u.role.startsWith(k.slice(0, 5))));
      const u = users.find((x) => x.username === body.username) || users[0];
      const role = u.role; const expected = demoLogins[role === 'regional_manager' ? 'regional' : role]?.[0];
      if (!body.username || body.password !== expected) { const e = new Error('Username or password is incorrect.'); e.status = 401; throw e; }
      return { token: 'demo-token', must_change_password: false, user: { name: u.name, username: u.username, role } };
    }
    if (path === '/auth/change-password' && method === 'POST') return { success: true };
    if (path === '/meta/filters') return { locations: LOCATIONS.map((n, i) => ({ id: `L${i + 1}`, name: n })), categories: CATEGORIES, items: items.map((i) => ({ id: i.id, name: i.name })), segments: SEGMENTS, channels: CHANNELS, promotions: promos.map((x) => ({ id: x.id, name: x.name })), classes: ['Profit Driver', 'Volume Driver', 'Hidden Opportunity', 'Low Performer'] };
    if (path === '/kpis/executive') {
      const rev = sum(trend, (t) => t.revenue);
      return { kpis: { revenue: rev, profit: sum(trend, (t) => t.profit), orders: sum(trend, (t) => t.orders), aov: 22.4, activeCustomers: 31240, repeatCustomers: 18960, wastageCost: 41850, forecastDemand: 18240 }, deltas: { revenue: 8.3, profit: 5.1, orders: 6.7, aov: 1.4, activeCustomers: 3.2, repeatCustomers: 2.1, wastageCost: -4.6, forecastDemand: 5.8 }, trend, channelMix: CHANNELS.map((n, i) => ({ name: n, value: [42, 24, 19, 15][i] })), heat };
    }
    if (path === '/menu/items') return applyItemFilters(items, p).map((i) => ({ ...i, classSpark: sparkOf(i), classPython: pyOf(i) }));
    if (path === '/menu/slow-moving') return items.filter((i) => i.orderFreq < 3 || i.daysSinceLast > 10 || i.trend < -15).slice(0, 12).map((i) => ({ id: i.id, name: i.name, qty: i.qty, orderFreq: i.orderFreq, daysSinceLast: i.daysSinceLast, repeatRate: i.repeatRate, wastePct: i.wastePct, marginPct: i.marginPct, trend: i.trend, slowScore: r2(rnd(0.6, 0.97)) }));
    if (path.startsWith('/menu/items/') && path.endsWith('/locations')) { const id = path.split('/')[3]; const it = items.find((x) => x.id === id) || items[0]; return LOCATIONS.map((name) => { const q = Math.round(it.qty / 6 * rnd(0.4, 1.7)); const m = r1(it.marginPct + rnd(-14, 14)); return { location: name, qty: q, marginPct: m, wastePct: r1(it.wastePct + rnd(-4, 6)), cls: q > medQty / 6 ? (m > medMargin ? 'Profit Driver' : 'Volume Driver') : (m > medMargin ? 'Hidden Opportunity' : 'Low Performer') }; }); }
    if (path === '/customers/segments') return customers.segments;
    if (path === '/customers/rfm') return { bands: customers.rfmBands, top: customers.top };
    if (path === '/customers/churn') return customers.churn;
    if (path === '/basket/rules') return rules.filter((r) => r.support >= (+p.minSupport || 0) && r.lift >= (+p.minLift || 0));
    if (path === '/peak') return peak();
    if (path === '/forecast') return forecast(p);
    if (path === '/wastage') return wastage;
    if (path === '/pricing/sensitivity') return pricing;
    if (path === '/pricing/history') return priceHistory(p.item);
    if (path === '/promotions') return promos;
    if (path === '/ratings') return { byItem: [...items].sort((a, b) => b.rating - a.rating).map((i) => ({ id: i.id, name: i.name, rating: i.rating, marginPct: i.marginPct, qty: i.qty, repeatRate: i.repeatRate })), byLocation: locs.map((l) => ({ name: l.name, rating: l.rating })), trend: Array.from({ length: 12 }, (_, k) => ({ week: `W${k + 1}`, rating: r2(4.1 + Math.sin(k / 2) * 0.15 + rnd(-0.05, 0.05)), promo: r2(4.2 + rnd(-0.1, 0.1)) })) };
    if (path === '/anomalies') return anomalies;
    if (path === '/locations') return locs;
    if (path === '/channels') return channels;
    if (path === '/models/comparison') return comparison(p.task || 'menu_class');
    if (path === '/recommendations') return recs.filter((r) => (!p.priority || r.priority === p.priority));
    if (path === '/whatif') return whatIf(body);
    if (path === '/jobs') return { jobs, dq };
    if (path === '/audit') return audit;
    if (path === '/admin/users' && method === 'GET') return users;
    if (path === '/admin/users' && method === 'POST') { const u = { id: users.length + 1, location: 'All', ...body }; users.push(u); return u; }
    if (path === '/users/' && method === 'GET') return users.map((u) => ({ ...u, email: u.username, is_active: true, must_change_password: false }));
    if (path === '/users/' && method === 'POST') { const u = { id: users.length + 1, username: body.email, email: body.email, name: body.name, role: body.role, location: 'All', is_active: true, must_change_password: true }; users.push(u); return u; }
    if (path === '/admin/locations') return locs.map((l) => ({ id: l.id, name: l.name, status: 'Active' }));
    if (path.startsWith('/reports/') && path.endsWith('/rows')) return reportRows(path.split('/')[2]);
    const e = new Error('This endpoint is not available in demo mode.'); e.status = 404; throw e;
  })();
  return new Promise((res, rej) => setTimeout(() => (out instanceof Error ? rej(out) : res(JSON.parse(JSON.stringify(out)))), 250));
}
