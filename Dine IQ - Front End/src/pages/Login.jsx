import { useState } from 'react';
import { useAuth } from '../context/AuthContext.jsx';
import { USE_MOCK } from '../api/client.js';
import { RestaurantAnalyticsIllustration } from '../components/Visuals.jsx';
import { Icon } from '../components/icons.jsx';
import { LogoMark } from '../components/Logo.jsx';

const FEATURES = [
  { icon: 'menu', title: 'Menu intelligence', text: 'See profitable dishes, hidden opportunities and low performers at a glance.' },
  { icon: 'users', title: 'Customer intelligence', text: 'Track segments, RFM, loyalty and churn-risk with cleaner storytelling.' },
  { icon: 'forecast', title: 'Operational planning', text: 'Forecast demand, watch peak periods and act on recommendation signals.' },
];

const MINI_STATS = [
  { label: 'Insights surfaced', value: '42+' },
  { label: 'Dashboards', value: '7' },
  { label: 'Decision-ready KPIs', value: '120+' },
];

const CALLOUTS = [
  { kicker: 'Peak-period analysis', text: 'Explore ordering patterns by hour, weekday and location.' },
  { kicker: 'Evidence and actions', text: 'Review the analytical evidence behind each recommendation.' },
  { kicker: 'Restaurant analytics', text: 'Sign in to review saved pipeline results and model comparisons.' },
];

export default function Login() {
  const { login } = useAuth();
  const [f, setF] = useState({ username: '', password: '' });
  const [err, setErr] = useState('');
  const [busy, setBusy] = useState(false);

  const submit = async (e) => {
    e.preventDefault();
    setBusy(true);
    setErr('');
    try {
      await login(f.username.trim(), f.password);
      window.location.href = '/';
    } catch (x) {
      setErr(x.message);
      setBusy(false);
    }
  };

  return (
    <div className="login">
      <div className="login-art">
        <div className="login-art-grid">
          <div className="login-copy">
            <div className="login-copy-top">
              <div className="eyebrow">MenuMatrix dining intelligence</div>
              <h1>Turn restaurant data into better menu decisions.</h1>
              <p>
                A cleaner, more premium analytics workspace for menu profitability,
                customer behavior, forecasts, promotions and wastage intelligence.
              </p>
            </div>

            <div className="login-feature-list" aria-label="Key platform capabilities">
              {FEATURES.map((item) => (
                <div className="login-feature" key={item.title}>
                  <span className="login-feature-icon"><Icon name={item.icon} size={18} /></span>
                  <div>
                    <strong>{item.title}</strong>
                    <span>{item.text}</span>
                  </div>
                </div>
              ))}
            </div>

            <div className="login-metric-strip" aria-label="Product highlights">
              {MINI_STATS.map((item) => (
                <div className="login-metric" key={item.label}>
                  <strong>{item.value}</strong>
                  <span>{item.label}</span>
                </div>
              ))}
            </div>
          </div>

          <div className="login-art-stage">
            <div className="login-art-frame">
              <RestaurantAnalyticsIllustration />
            </div>
            <div className="login-callout-grid" aria-label="Supporting interface notes">
              {CALLOUTS.map((item) => (
                <div className="login-callout" key={item.kicker}>
                  <span className="login-callout-kicker">{item.kicker}</span>
                  <strong>{item.text}</strong>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>

      <div className="login-form">
        <form onSubmit={submit}>
          <div>
            <div className="login-brand"><LogoMark size={46} /><b>Dine<span>IQ</span></b></div>
            <div className="login-kicker">Welcome back</div>
            <h2>Sign in to DineIQ</h2>
            <p className="small muted" style={{ margin: '5px 0 0' }}>
              Continue to your restaurant intelligence dashboard.
            </p>
          </div>
          <label className="field">{USE_MOCK ? 'Username' : 'Email'}
            <input className="input" autoFocus placeholder={USE_MOCK ? 'e.g. admin' : 'Your account email'} value={f.username} onChange={(e) => setF({ ...f, username: e.target.value })} required />
          </label>
          <label className="field">Password
            <input className="input" type="password" placeholder="Enter password" value={f.password} onChange={(e) => setF({ ...f, password: e.target.value })} required />
          </label>
          {err && <div className="state err" role="alert" style={{ padding: 10 }}>{err}</div>}
          <button className="btn primary" disabled={busy}>{busy ? 'Signing in…' : 'Open dashboard'}</button>
          {USE_MOCK && (
            <div className="small muted" style={{ background: '#f7faf8', padding: 10, borderRadius: 10, border: '1px solid #e9efec' }}>
              Demo: <b>admin / admin123</b><br />
              Also supports analyst, regional and manager demo accounts.
            </div>
          )}
        </form>
      </div>
    </div>
  );
}
