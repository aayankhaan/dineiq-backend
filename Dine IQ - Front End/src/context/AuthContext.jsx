import { createContext, useContext, useState, useCallback } from 'react';
import { api, setToken, getToken } from '../api/client.js';

const AuthCtx = createContext(null);
export const ROLES = {
  admin: 'Administrator',
  regional_manager: 'Regional manager',
  manager: 'Restaurant manager',
  analyst: 'Analyst',
};
// Who may export data (SRS Step 50: "users with suitable permissions").
const EXPORT_ROLES = ['admin', 'analyst', 'regional_manager'];

export function AuthProvider({ children }) {
  const [user, setUser] = useState(() => {
    try { return JSON.parse(localStorage.getItem('dineiq_user')); } catch { return null; }
  });

  const login = useCallback(async (username, password) => {
    const res = await api('/auth/login', { method: 'POST', body: { email: username, password } });
    const nextUser = { ...res.user, must_change_password: !!res.must_change_password };
    setToken(res.token);
    localStorage.setItem('dineiq_user', JSON.stringify(nextUser));
    setUser(nextUser);
    return res;
  }, []);

  const changePassword = useCallback(async (currentPassword, newPassword) => {
    await api('/auth/change-password', {
      method: 'POST',
      body: { current_password: currentPassword, new_password: newPassword },
    });
    setUser((currentUser) => {
      const nextUser = { ...currentUser, must_change_password: false };
      localStorage.setItem('dineiq_user', JSON.stringify(nextUser));
      return nextUser;
    });
  }, []);

  const logout = useCallback(async () => {
    try { await api('/auth/logout', { method: 'POST' }); } catch { /* Clear the local session even if offline. */ }
    setToken(null);
    localStorage.removeItem('dineiq_user');
    setUser(null);
  }, []);

  const value = {
    user, login, logout, changePassword, authed: !!user && !!getToken(),
    mustChangePassword: !!user?.must_change_password,
    hasRole: (roles) => !roles || (user && roles.includes(user.role)),
    canExport: !!user && EXPORT_ROLES.includes(user.role),
  };
  return <AuthCtx.Provider value={value}>{children}</AuthCtx.Provider>;
}
export const useAuth = () => useContext(AuthCtx);
