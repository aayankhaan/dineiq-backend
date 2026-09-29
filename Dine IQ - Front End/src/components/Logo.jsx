/* DineIQ logo. The D's counter holds a rising trend line (the "IQ"),
   and the sparkle sits above the bowl. Scales cleanly from 16px to 200px. */
export function LogoMark({ size = 40, className = '' }) {
  return (
    <svg className={className} width={size} height={size} viewBox="0 0 48 48" role="img" aria-label="DineIQ" style={{ display: 'block', flex: 'none' }}>
      <defs>
        <linearGradient id="lmGold" x1="0" y1="0" x2="1" y2="1"><stop offset="0" stopColor="#f7c85f" /><stop offset="1" stopColor="#d98510" /></linearGradient>
        <linearGradient id="lmD" x1="0" y1="0" x2="0" y2="1"><stop stopColor="#1f4d3d" /><stop offset="1" stopColor="#0f2a21" /></linearGradient>
        <linearGradient id="lmGloss" x1="0" y1="0" x2="0" y2="1"><stop stopColor="#fff" stopOpacity=".4" /><stop offset="1" stopColor="#fff" stopOpacity="0" /></linearGradient>
      </defs>
      <rect width="48" height="48" rx="14" fill="url(#lmGold)" />
      <path d="M0 14A14 14 0 0 1 14 0h20a14 14 0 0 1 14 14v8C36 29 12 29 0 22Z" fill="url(#lmGloss)" />
      <path fillRule="evenodd" fill="url(#lmD)" d="M14 11h11a13 13 0 0 1 0 26H14ZM21 18v12h4a6 6 0 0 0 0-12Z" />
      <path d="M22.8 27l2.2-2.8 2 1.6 2.4-4.4" fill="none" stroke="#fff" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
      <circle cx="29.4" cy="21.4" r="1.6" fill="#fff" />
      <path d="M38 6.4Q38.5 9.5 41.6 10Q38.5 10.5 38 13.6Q37.5 10.5 34.4 10Q37.5 9.5 38 6.4Z" fill="#fff" opacity=".95" />
      <rect x=".5" y=".5" width="47" height="47" rx="13.5" fill="none" stroke="#fff" strokeOpacity=".3" />
    </svg>
  );
}

export function Wordmark({ light = true }) {
  return <span style={{ color: light ? '#fff' : '#17322a' }}>Dine<span style={{ color: light ? '#8fd0b0' : '#1f7657' }}>IQ</span></span>;
}
