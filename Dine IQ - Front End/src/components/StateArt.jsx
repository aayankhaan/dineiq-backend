/* Friendly, text-free art for empty / error / not-found states (light backgrounds). */
const wrap = (children, label) => (
  <svg className="state-art" viewBox="0 0 160 110" role="img" aria-label={label}>{children}</svg>
);

export const EmptyArt = () => wrap(
  <>
    <ellipse cx="78" cy="96" rx="50" ry="6" fill="#143229" opacity=".07" />
    <ellipse cx="78" cy="72" rx="54" ry="26" fill="#fff" stroke="#dbe7e1" strokeWidth="2" />
    <ellipse cx="78" cy="72" rx="38" ry="17" fill="#f3f8f5" stroke="#e3ede8" strokeWidth="2" />
    <circle cx="62" cy="74" r="2.5" fill="#bcd3c8" /><circle cx="84" cy="78" r="2" fill="#bcd3c8" /><circle cx="94" cy="68" r="2.5" fill="#bcd3c8" />
    <circle cx="106" cy="40" r="17" fill="#f2b74d" fillOpacity=".14" stroke="#e3a008" strokeWidth="5" />
    <path d="M118 53l15 15" stroke="#e3a008" strokeWidth="7" strokeLinecap="round" />
    <path d="M98 36q4-6 11-4" stroke="#fff" strokeWidth="3" strokeLinecap="round" fill="none" />
  </>, 'No records to show');

export const ErrorArt = () => wrap(
  <>
    <ellipse cx="80" cy="98" rx="46" ry="6" fill="#9d3d34" opacity=".08" />
    <ellipse cx="80" cy="76" rx="50" ry="22" fill="#fff" stroke="#f0d3ce" strokeWidth="2" />
    <path d="M66 68l8 6-6 6 9 5" stroke="#e5b3aa" strokeWidth="2" fill="none" strokeLinecap="round" strokeLinejoin="round" />
    <path d="M80 12c4 0 6 2 8 6l30 50c3 6 0 12-7 12H49c-7 0-10-6-7-12l30-50c2-4 4-6 8-6Z" fill="#fff4f2" stroke="#d9604b" strokeWidth="5" strokeLinejoin="round" transform="translate(0 -4) scale(.9) translate(9 4)" />
    <path d="M80 34v22" stroke="#d9604b" strokeWidth="6" strokeLinecap="round" />
    <circle cx="80" cy="66" r="3.6" fill="#d9604b" />
  </>, 'Something went wrong');

export const NotFoundArt = () => wrap(
  <>
    <ellipse cx="80" cy="98" rx="52" ry="6" fill="#143229" opacity=".07" />
    <ellipse cx="80" cy="84" rx="56" ry="10" fill="#fff" stroke="#dbe7e1" strokeWidth="2" />
    <g transform="rotate(-14 110 60)">
      <path d="M46 70a34 34 0 0 1 68 0Z" fill="#f2b74d" />
      <path d="M52 62a28 28 0 0 1 18-18" stroke="#fff" strokeWidth="4" strokeLinecap="round" fill="none" opacity=".6" />
      <circle cx="80" cy="34" r="5" fill="#d98510" />
    </g>
    <path d="M60 60q-6-8 0-14t0-14" stroke="#9fc5b3" strokeWidth="3" strokeDasharray="1 7" strokeLinecap="round" fill="none" />
    <path d="M78 66q-6-8 0-14t0-14" stroke="#9fc5b3" strokeWidth="3" strokeDasharray="1 7" strokeLinecap="round" fill="none" />
  </>, 'Page not found');
