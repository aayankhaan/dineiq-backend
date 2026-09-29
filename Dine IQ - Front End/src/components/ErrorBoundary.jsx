import { Component } from 'react';
import { Icon } from './icons.jsx';

export default class ErrorBoundary extends Component {
  constructor(props) { super(props); this.state = { error: null }; }
  static getDerivedStateFromError(error) { return { error }; }
  componentDidCatch(error, info) { console.error('DineIQ page error', error, info); }
  render() {
    if (!this.state.error) return this.props.children;
    return (
      <div className="fatal-state card">
        <div className="fatal-icon"><Icon name="alert" size={28} /></div>
        <div><h2>This view hit a display error</h2><p>Reload this page or switch to another section. If the problem continues, check that the API is running and the related pipeline result exists.</p><code>{this.state.error?.message}</code></div>
        <button className="btn primary" onClick={() => window.location.reload()}>Reload view</button>
      </div>
    );
  }
}
