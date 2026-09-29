import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useAuth } from '../context/AuthContext.jsx';
import { LogoMark } from '../components/Logo.jsx';

export default function ChangePassword() {
  const { changePassword, logout } = useAuth();
  const navigate = useNavigate();
  const [form, setForm] = useState({ currentPassword: '', newPassword: '', confirmPassword: '' });
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);

  const submit = async (event) => {
    event.preventDefault();
    setError('');

    if (form.newPassword.length < 8) {
      setError('Your new password must be at least 8 characters.');
      return;
    }

    if (form.newPassword !== form.confirmPassword) {
      setError('The new passwords do not match.');
      return;
    }

    if (form.currentPassword === form.newPassword) {
      setError('Choose a password different from your temporary password.');
      return;
    }

    setBusy(true);
    try {
      await changePassword(form.currentPassword, form.newPassword);
      navigate('/', { replace: true });
    } catch (requestError) {
      setError(requestError.message);
      setBusy(false);
    }
  };

  return (
    <div className="login password-change">
      <div className="login-form">
        <form onSubmit={submit}>
          <div>
            <div className="login-brand"><LogoMark size={46} /><b>Dine<span>IQ</span></b></div>
            <div className="login-kicker">First sign-in</div>
            <h2>Create a new password</h2>
            <p className="small muted" style={{ margin: '5px 0 0' }}>
              Replace your temporary password before opening the dashboard.
            </p>
          </div>
          <label className="field">Temporary password
            <input className="input" type="password" autoFocus autoComplete="current-password" value={form.currentPassword} onChange={(event) => setForm({ ...form, currentPassword: event.target.value })} required />
          </label>
          <label className="field">New password
            <input className="input" type="password" autoComplete="new-password" minLength={8} value={form.newPassword} onChange={(event) => setForm({ ...form, newPassword: event.target.value })} required />
          </label>
          <label className="field">Confirm new password
            <input className="input" type="password" autoComplete="new-password" minLength={8} value={form.confirmPassword} onChange={(event) => setForm({ ...form, confirmPassword: event.target.value })} required />
          </label>
          {error && <div className="state err" role="alert" style={{ padding: 10 }}>{error}</div>}
          <button className="btn primary" disabled={busy}>{busy ? 'Updating password…' : 'Set password and continue'}</button>
          <button className="btn" type="button" disabled={busy} onClick={logout}>Sign out</button>
        </form>
      </div>
    </div>
  );
}
