import { NotFoundArt } from './components/StateArt.jsx';
import { Navigate, Route, Routes } from 'react-router-dom';
import { useAuth } from './context/AuthContext.jsx';
import Layout from './components/Layout.jsx';
import Login from './pages/Login.jsx';
import ChangePassword from './pages/ChangePassword.jsx';
import { NAV } from './nav.js';
import { Icon } from './components/icons.jsx';

function Guard({ roles, children }) {
  const { hasRole } = useAuth();
  return hasRole(roles) ? children : <div className="card state access-state"><Icon name="alert" size={28}/><h2>Access restricted</h2><p>Your role does not have access to this workspace.</p></div>;
}

export default function App() {
  const { authed, mustChangePassword } = useAuth();
  if (!authed) return <Routes><Route path="/login" element={<Login />} /><Route path="*" element={<Navigate to="/login" replace />} /></Routes>;
  if (mustChangePassword) return <Routes><Route path="/change-password" element={<ChangePassword />} /><Route path="*" element={<Navigate to="/change-password" replace />} /></Routes>;
  return (
    <Routes>
      <Route path="/change-password" element={<Navigate to="/" replace />} />
      <Route element={<Layout />}>
        {NAV.flatMap((g) => g.items).map(({ to, el: El, roles }) => <Route key={to} path={to} element={<Guard roles={roles}><El /></Guard>} />)}
        <Route path="/login" element={<Navigate to="/" replace />} />
        <Route path="*" element={<div className="card state"><NotFoundArt /><h2>Page not found</h2><p>Use the navigation to return to a DineIQ workspace.</p></div>} />
      </Route>
    </Routes>
  );
}
