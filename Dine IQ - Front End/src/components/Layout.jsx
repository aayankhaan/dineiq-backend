import { useState } from 'react';
import { USE_MOCK } from '../api/client.js';
import { NavLink, Outlet, useLocation } from 'react-router-dom';
import { useAuth, ROLES } from '../context/AuthContext.jsx';
import { useFilters } from '../context/FiltersContext.jsx';
import { NAV } from '../nav.js';
import FilterBar from './FilterBar.jsx';
import { Icon } from './icons.jsx';
import { DishIllustration, MiniSparkline } from './Visuals.jsx';
import { PAGE_ART } from './PageArt.jsx';
import { LogoMark } from './Logo.jsx';
import ErrorBoundary from './ErrorBoundary.jsx';
import PageExport from './PageExport.jsx';
import { PAGE_FILTERS } from '../filterConfig.js';

export default function Layout() {
  const { user, logout, hasRole } = useAuth();
  const { filters } = useFilters();
  const [open, setOpen] = useState(false);
  const [showFilters, setShowFilters] = useState(false);
  const { pathname } = useLocation();
  const Art = PAGE_ART[pathname];
  const current = NAV.flatMap((g) => g.items).find((i) => i.to === pathname) || NAV[0].items[0];
  const pageFilters = PAGE_FILTERS[pathname] || [];
  const activeCount = pageFilters.filter((key) => filters[key] !== '' && filters[key] != null).length;

  return (
    <div className="shell">
      <aside className={`side ${open ? 'open' : ''}`} onClick={(e) => { if (e.target.closest('a')) setOpen(false); }}>
        <div className="brand-wrap">
          <LogoMark size={42} />
          <div><div className="brand">Dine<span className="brand-iq">IQ</span></div><div className="brand-sub">Dining intelligence</div></div>
        </div>
        <div className="side-status"><span className="status-dot" /> {USE_MOCK ? 'Demo intelligence' : 'Saved analytics'} <b>{USE_MOCK ? 'Demo' : 'API'}</b></div>
        <div className="side-scroll">
          {NAV.map((g) => {
            const items = g.items.filter((i) => hasRole(i.roles));
            if (!items.length) return null;
            return (
              <nav className="nav-group" key={g.group} aria-label={g.group}>
                <div className="nav-title">{g.group}</div>
                {items.map((i) => <NavLink key={i.to} to={i.to} end className={({ isActive }) => `nav-link ${isActive ? 'active' : ''}`}><Icon name={i.icon} size={18}/><span>{i.label}</span><Icon name="chevron" size={14} className="nav-chevron"/></NavLink>)}
              </nav>
            );
          })}
        </div>
        <div className="side-profile"><div className="avatar">{user?.name?.split(' ').map((x) => x[0]).join('').slice(0, 2)}</div><div><b>{user?.name}</b><span>{ROLES[user?.role]}</span></div><button onClick={logout} aria-label="Sign out"><Icon name="logout" size={18}/></button></div>
      </aside>
      {open && <button className="side-backdrop" aria-label="Close navigation" onClick={() => setOpen(false)} />}
      <div className="main">
        <header className="top">
          <button className="icon-btn menu-btn" onClick={() => setOpen(true)} aria-label="Open navigation"><Icon name="menu" /></button>
          <div className="top-title"><span>DineIQ Analytics</span><h1>{current.short || current.label}</h1></div>
          <div className="top-actions">
            {!!pageFilters.length && <button className={`filter-btn ${activeCount ? 'has-filters' : ''}`} onClick={() => setShowFilters((s) => !s)} aria-expanded={showFilters}><Icon name="filter" size={17}/> Filters {activeCount ? <b>{activeCount}</b> : null}</button>}
            <div className="top-avatar">{user?.name?.[0] || 'D'}</div>
          </div>
        </header>
        {showFilters && <FilterBar />}
        <main className="content">
          <section className={`page-hero`}>
            <div className="hero-copy"><div className="eyebrow"><Icon name={current.icon} size={15}/> {current.group || 'Intelligence workspace'}</div><h2>{current.label}</h2><p>{current.desc}</p><div className="hero-meta"><span><i className="pulse"/> {USE_MOCK ? 'Demo data' : 'Saved pipeline results'}</span></div></div>
            {Art ? (
              <div className="hero-visual"><Art /></div>
            ) : (
              <div className="hero-visual"><div className="hero-stat"><span>Signal health</span><b>94%</b><MiniSparkline /></div><DishIllustration compact /></div>
            )}
          </section>
          <PageExport path={pathname} />
          <ErrorBoundary key={pathname}><Outlet /></ErrorBoundary>
        </main>
      </div>
    </div>
  );
}
