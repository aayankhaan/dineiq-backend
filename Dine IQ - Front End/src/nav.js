import Executive from './pages/Executive.jsx';
import Menu from './pages/Menu.jsx';
import Customers from './pages/Customers.jsx';
import Basket from './pages/Basket.jsx';
import Peak from './pages/Peak.jsx';
import Forecast from './pages/Forecast.jsx';
import Wastage from './pages/Wastage.jsx';
import Pricing from './pages/Pricing.jsx';
import Promotions from './pages/Promotions.jsx';
import Ratings from './pages/Ratings.jsx';
import Locations from './pages/Locations.jsx';
import DualPipeline from './pages/DualPipeline.jsx';
import Recommendations from './pages/Recommendations.jsx';
import WhatIf from './pages/WhatIf.jsx';
import Reports from './pages/Reports.jsx';
import Jobs from './pages/Jobs.jsx';
import Admin from './pages/Admin.jsx';

const ALL = undefined;
const TEAM = ['admin', 'analyst', 'regional_manager'];
export const NAV = [
  { group: 'Overview', items: [
    { to: '/', label: 'Executive dashboard', short: 'Overview', icon: 'dashboard', el: Executive, roles: ALL, desc: 'A live command center for revenue, demand, wastage, anomalies and actions.' },
    { to: '/recommendations', label: 'Recommendations', icon: 'sparkles', el: Recommendations, roles: ALL, desc: 'Evidence-backed actions prioritized by potential business impact.' },
  ] },
  { group: 'Menu intelligence', items: [
    { to: '/menu', label: 'Menu performance', icon: 'menu', el: Menu, roles: ALL, desc: 'Find profit drivers, hidden opportunities, slow movers and risky dishes.' },
    { to: '/pricing', label: 'Price intelligence', icon: 'tag', el: Pricing, roles: ALL, desc: 'Understand price sensitivity and how changes affect demand and margin.' },
    { to: '/promotions', label: 'Promotions', icon: 'promo', el: Promotions, roles: ALL, desc: 'Separate true promotion wins from margin-destroying promotion traps.' },
    { to: '/basket', label: 'Market basket', icon: 'basket', el: Basket, roles: ALL, desc: 'Discover strong item pairings, bundles and cross-sell opportunities.' },
    { to: '/ratings', label: 'Ratings & anomalies', icon: 'star', el: Ratings, roles: ALL, desc: 'Connect guest satisfaction to sales, margin and unusual rating behavior.' },
  ] },
  { group: 'Customers & demand', items: [
    { to: '/customers', label: 'Customer intelligence', icon: 'users', el: Customers, roles: ALL, desc: 'Explore behavioral segments, RFM signals, loyal guests and churn risk.' },
    { to: '/peak', label: 'Peak periods', icon: 'clock', el: Peak, roles: ALL, desc: 'See when demand concentrates by hour, day, channel and location.' },
    { to: '/forecast', label: 'Demand forecast', icon: 'forecast', el: Forecast, roles: ALL, desc: 'Compare historical and predicted demand with time-aware model validation.' },
    { to: '/wastage', label: 'Wastage', icon: 'waste', el: Wastage, roles: ALL, desc: 'Pinpoint costly waste patterns and high-risk preparation periods.' },
  ] },
  { group: 'Operations', items: [
    { to: '/locations', label: 'Locations & channels', icon: 'pin', el: Locations, roles: TEAM, desc: 'Benchmark restaurant locations and ordering channels on consistent KPIs.' },
    { to: '/whatif', label: 'What-if simulator', icon: 'sliders', el: WhatIf, roles: ALL, desc: 'Explore estimated outcomes before changing price, demand, waste or promotions.' },
  ] },
  { group: 'Data science', items: [
    { to: '/models', label: 'Spark vs Python', icon: 'models', el: DualPipeline, roles: TEAM, desc: 'Inspect independent model agreement, differences and evaluation metrics.' },
    { to: '/reports', label: 'Reports & export', icon: 'report', el: Reports, roles: ALL, desc: 'Generate filtered analytical reports and evaluator-ready exports.' },
    { to: '/jobs', label: 'Spark jobs & quality', icon: 'activity', el: Jobs, roles: TEAM, desc: 'Monitor processing jobs, data-quality findings and pipeline health.' },
  ] },
  { group: 'Administration', items: [
    { to: '/admin', label: 'Admin & audit', icon: 'settings', el: Admin, roles: ['admin'], desc: 'Manage users, locations and the audit trail.' },
  ] },
];
