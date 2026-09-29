/* Small, text-free hero illustrations, one per dashboard.
   Same palette as the restaurant illustration. viewBox 300x190.
   Add or change a page in PAGE_ART at the bottom. */

const GOLD = '#e0bb6a';
const G1 = '#87ddb1';
const G2 = '#2f9470';
const BLUE = '#8ec1ff';
const RED = '#ff8a75';
const DARK = '#17322a';
const glass = { fill: 'rgba(255,255,255,.09)', stroke: 'rgba(255,255,255,.2)', strokeWidth: 1.5 };

function Art({ children }) {
  return (
    <svg className="page-art" viewBox="0 0 300 190" aria-hidden="true">
      <defs>
        <linearGradient id="paGold" x1="0" x2="1" y1="0" y2="1"><stop stopColor="#ecc878" /><stop offset="1" stopColor="#c99a3e" /></linearGradient>
        <linearGradient id="paGreen" x1="0" x2="0" y1="0" y2="1"><stop stopColor="#87ddb1" /><stop offset="1" stopColor="#2f9470" /></linearGradient>
        <linearGradient id="paBlue" x1="0" x2="0" y1="0" y2="1"><stop stopColor="#a8d0ff" /><stop offset="1" stopColor="#5887db" /></linearGradient>
        <linearGradient id="paRed" x1="0" x2="0" y1="0" y2="1"><stop stopColor="#ffab98" /><stop offset="1" stopColor="#d9604b" /></linearGradient>
      </defs>
      <circle cx="222" cy="52" r="62" fill={GOLD} opacity=".05" />
      {children}
    </svg>
  );
}

const starPts = (cx, cy, r) =>
  Array.from({ length: 10 }, (_, i) => {
    const a = -Math.PI / 2 + (i * Math.PI) / 5;
    const rad = i % 2 ? r * 0.45 : r;
    return `${(cx + rad * Math.cos(a)).toFixed(1)},${(cy + rad * Math.sin(a)).toFixed(1)}`;
  }).join(' ');

const burstPts = (cx, cy, r1, r2, n) =>
  Array.from({ length: n * 2 }, (_, i) => {
    const a = (i * Math.PI) / n;
    const rad = i % 2 ? r2 : r1;
    return `${(cx + rad * Math.cos(a)).toFixed(1)},${(cy + rad * Math.sin(a)).toFixed(1)}`;
  }).join(' ');

const sparkle = (x, y, s) =>
  `M${x} ${y - s}Q${x + s * 0.15} ${y - s * 0.15} ${x + s} ${y}Q${x + s * 0.15} ${y + s * 0.15} ${x} ${y + s}Q${x - s * 0.15} ${y + s * 0.15} ${x - s} ${y}Q${x - s * 0.15} ${y - s * 0.15} ${x} ${y - s}Z`;

const line = { fill: 'none', strokeLinecap: 'round', strokeLinejoin: 'round' };

/* Executive: rising bars + growth arrow */
export const ExecutiveArt = () => (
  <Art>
    <rect x="34" y="26" width="232" height="140" rx="26" {...glass} />
    {[[62, 30], [102, 50], [142, 70], [182, 92]].map(([x, h], i) => (
      <rect key={x} x={x} y={146 - h} width="28" height={h} rx="9" fill={i === 3 ? 'url(#paGold)' : 'url(#paGreen)'} />
    ))}
    <path d="M62 96 L112 74 L150 84 L220 42" stroke={G1} strokeWidth="5" {...line} />
    <path d="M202 40 L221 41 L218 60" stroke={G1} strokeWidth="5" {...line} />
    <circle cx="112" cy="74" r="5" fill="#fff" />
  </Art>
);

/* Recommendations: checklist + sparkle */
export const RecommendationsArt = () => (
  <Art>
    <rect x="44" y="30" width="160" height="130" rx="24" {...glass} />
    {[58, 88, 118].map((y, i) => (
      <g key={y}>
        <circle cx="76" cy={y + 6} r="11" fill={i === 2 ? 'rgba(255,255,255,.2)' : 'url(#paGold)'} />
        {i < 2 && <path d={`M70 ${y + 6} l4 4 l8 -9`} stroke={DARK} strokeWidth="3.5" {...line} />}
        <rect x="98" y={y + 2} width={i === 1 ? 70 : 86} height="9" rx="4.5" fill="rgba(255,255,255,.3)" />
      </g>
    ))}
    <path d={sparkle(230, 66, 30)} fill="url(#paGold)" />
    <path d={sparkle(256, 118, 12)} fill={G1} />
    <path d={sparkle(204, 126, 9)} fill="#fff" opacity=".7" />
  </Art>
);

/* Menu: plate with cutlery */
export const MenuArt = () => (
  <Art>
    <ellipse cx="150" cy="112" rx="88" ry="46" fill="#fff" opacity=".95" />
    <ellipse cx="150" cy="112" rx="64" ry="31" fill="#f4f8f5" stroke="#dfeae4" strokeWidth="3" />
    <path d="M108 112c10-18 28-25 48-23 18 2 30 12 40 26-16 14-32 20-52 19-20-1-31-8-36-22Z" fill="url(#paGold)" />
    <path d="M118 108c12 10 26 14 42 11 9-2 16-6 24-14" stroke="#5f8e5e" strokeWidth="6" {...line} />
    <circle cx="132" cy="104" r="5" fill="#cc4d3d" /><circle cx="170" cy="122" r="4.5" fill="#cc4d3d" /><circle cx="152" cy="98" r="3.5" fill="#f6ecd3" />
    <g stroke="#fff" strokeWidth="5" opacity=".8" {...line}>
      <path d="M32 54v30M42 54v30M52 54v30M32 84q10 12 20 0M42 90v66" />
      <path d="M262 54q14 16 4 52h-4zM262 106v50" />
    </g>
    <path d={sparkle(232, 44, 14)} fill={GOLD} />
  </Art>
);

/* Pricing: tag with up / down arrows */
export const PricingArt = () => (
  <Art>
    <path d="M70 52h84l56 58-56 58H70a18 18 0 0 1-18-18V70a18 18 0 0 1 18-18Z" fill="url(#paGold)" />
    <circle cx="82" cy="78" r="8" fill="#a86a12" />
    <path d="M100 112h62M100 132h40" stroke="#fff" strokeWidth="7" opacity=".7" {...line} />
    <path d="M236 92V44M222 58l14-14 14 14" stroke={G1} strokeWidth="8" {...line} />
    <path d="M266 100v48M252 134l14 14 14-14" stroke={RED} strokeWidth="8" {...line} />
  </Art>
);

/* Promotions: discount burst */
export const PromotionsArt = () => (
  <Art>
    <polygon points={burstPts(150, 96, 74, 62, 14)} fill="url(#paGold)" />
    <circle cx="128" cy="76" r="11" fill="none" stroke={DARK} strokeWidth="7" />
    <circle cx="172" cy="116" r="11" fill="none" stroke={DARK} strokeWidth="7" />
    <path d="M176 70L124 122" stroke={DARK} strokeWidth="8" {...line} />
    <circle cx="48" cy="52" r="6" fill={G1} /><circle cx="252" cy="44" r="5" fill="#fff" opacity=".7" />
    <circle cx="254" cy="146" r="7" fill={BLUE} /><circle cx="44" cy="140" r="4" fill={RED} />
    <path d="M40 92l10 6M262 96l-10 6" stroke="#fff" strokeWidth="4" opacity=".5" {...line} />
  </Art>
);

/* Basket: basket with items and a cross-sell plus */
export const BasketArt = () => (
  <Art>
    <circle cx="120" cy="88" r="17" fill="url(#paGold)" />
    <circle cx="154" cy="78" r="19" fill="url(#paGreen)" />
    <circle cx="188" cy="90" r="15" fill="url(#paBlue)" />
    <path d="M100 96q50-76 100 0" stroke="rgba(255,255,255,.55)" strokeWidth="6" {...line} />
    <path d="M62 98h176l-18 66H80Z" fill="rgba(255,255,255,.16)" stroke="rgba(255,255,255,.4)" strokeWidth="2" />
    <path d="M100 112l6 40M150 112v40M200 112l-6 40" stroke="rgba(255,255,255,.25)" strokeWidth="4" {...line} />
    <circle cx="246" cy="48" r="18" fill="url(#paGold)" />
    <path d="M246 40v16M238 48h16" stroke={DARK} strokeWidth="4.5" {...line} />
  </Art>
);

/* Ratings: stars + anomaly spike */
export const RatingsArt = () => (
  <Art>
    {[58, 104, 150, 196, 242].map((x, i) => (
      <polygon key={x} points={starPts(x, 70, 21)} fill={i < 4 ? 'url(#paGold)' : 'rgba(255,255,255,.22)'} />
    ))}
    <rect x="34" y="112" width="232" height="58" rx="18" {...glass} />
    <path d="M52 150L96 146L134 150L162 126L188 152L246 146" stroke={G1} strokeWidth="4" {...line} />
    <circle cx="162" cy="126" r="12" fill="none" stroke={RED} strokeWidth="2.5" opacity=".7" />
    <circle cx="162" cy="126" r="6" fill={RED} />
  </Art>
);

/* Customers: group of guests + heart */
export const CustomersArt = () => (
  <Art>
    <circle cx="82" cy="94" r="15" fill="url(#paGreen)" />
    <path d="M52 158q0-36 30-36t30 36Z" fill="url(#paGreen)" opacity=".85" />
    <circle cx="218" cy="94" r="15" fill="url(#paBlue)" />
    <path d="M188 158q0-36 30-36t30 36Z" fill="url(#paBlue)" opacity=".85" />
    <circle cx="150" cy="84" r="22" fill="url(#paGold)" />
    <path d="M108 164q0-50 42-50t42 50Z" fill="url(#paGold)" />
    <path d="M150 40c-15-9-22-19-11-26 7-3 11 1 11 6 0-5 4-9 11-6 11 7 4 17-11 26Z" fill={RED} transform="translate(0 6)" />
  </Art>
);

/* Peak: clock + demand bars */
export const PeakArt = () => (
  <Art>
    <circle cx="100" cy="98" r="58" {...glass} />
    <path d="M100 52v8M100 136v8M54 98h8M138 98h8" stroke="rgba(255,255,255,.5)" strokeWidth="4" {...line} />
    <path d="M100 98V66M100 98l24 14" stroke={GOLD} strokeWidth="6" {...line} />
    <circle cx="100" cy="98" r="6" fill="#fff" />
    {[[184, 34], [204, 62], [224, 92], [244, 58], [264, 30]].map(([x, h], i) => (
      <rect key={x} x={x - 8} y={150 - h} width="16" height={h} rx="6" fill={i === 2 ? 'url(#paGold)' : 'url(#paGreen)'} opacity={i === 2 ? 1 : 0.8} />
    ))}
  </Art>
);

/* Forecast: history line continuing into a forecast band */
export const ForecastArt = () => (
  <Art>
    <rect x="30" y="28" width="240" height="136" rx="26" {...glass} />
    <path d="M180 92C204 76 224 62 250 54L250 100C226 108 204 112 180 104Z" fill={GOLD} opacity=".2" />
    <path d="M180 30V162" stroke="rgba(255,255,255,.35)" strokeWidth="2" strokeDasharray="4 6" />
    <path d="M52 134C78 126 88 100 114 104S156 122 180 98" stroke={G1} strokeWidth="6" {...line} />
    <path d="M180 98C204 84 226 68 250 60" stroke={GOLD} strokeWidth="6" strokeDasharray="1 11" {...line} />
    <path d="M234 52l18 6-10 16" stroke={GOLD} strokeWidth="5" {...line} />
    <circle cx="180" cy="98" r="7" fill="#fff" />
  </Art>
);

/* Wastage: shrinking bars + red arrow down */
export const WastageArt = () => (
  <Art>
    <rect x="30" y="28" width="240" height="136" rx="26" {...glass} />
    {[100, 78, 56, 36].map((h, i) => (
      <rect key={i} x={54 + i * 38} y={148 - h} width="26" height={h} rx="9" fill="url(#paRed)" opacity={1 - i * 0.16} />
    ))}
    <path d="M232 46v72" stroke={RED} strokeWidth="11" {...line} />
    <path d="M212 104l20 22 20-22" stroke={RED} strokeWidth="11" {...line} />
  </Art>
);

/* Locations: map with pins and route */
export const LocationsArt = () => (
  <Art>
    <rect x="26" y="26" width="248" height="140" rx="26" {...glass} />
    <rect x="44" y="42" width="60" height="38" rx="10" fill="rgba(255,255,255,.1)" />
    <rect x="200" y="112" width="58" height="40" rx="10" fill="rgba(255,255,255,.1)" />
    <rect x="212" y="42" width="46" height="52" rx="10" fill="rgba(255,255,255,.07)" />
    <path d="M62 146Q104 170 150 142T240 134" stroke="rgba(255,255,255,.5)" strokeWidth="3" strokeDasharray="2 9" {...line} />
    <g transform="translate(150 142)"><path d="M0 0c-26-26-34-42-34-54a34 34 0 0 1 68 0c0 12-8 28-34 54Z" fill="url(#paGold)" /><circle cx="0" cy="-54" r="12" fill={DARK} /></g>
    <g transform="translate(62 148) scale(.5)"><path d="M0 0c-26-26-34-42-34-54a34 34 0 0 1 68 0c0 12-8 28-34 54Z" fill="url(#paGreen)" /><circle cx="0" cy="-54" r="12" fill={DARK} /></g>
    <g transform="translate(240 140) scale(.5)"><path d="M0 0c-26-26-34-42-34-54a34 34 0 0 1 68 0c0 12-8 28-34 54Z" fill="url(#paBlue)" /><circle cx="0" cy="-54" r="12" fill={DARK} /></g>
  </Art>
);

/* What-if: one choice forks into two outcomes */
export const WhatIfArt = () => (
  <Art>
    <path d="M78 96C124 96 132 52 196 52" stroke={G1} strokeWidth="6" {...line} />
    <path d="M78 96C124 96 132 140 196 140" stroke={BLUE} strokeWidth="6" strokeDasharray="2 11" {...line} />
    <circle cx="62" cy="96" r="18" fill="url(#paGold)" />
    <circle cx="214" cy="52" r="18" fill="url(#paGreen)" />
    <circle cx="214" cy="140" r="18" fill="url(#paBlue)" />
    <path d="M206 54l6 6 11-13" stroke={DARK} strokeWidth="4" {...line} />
    <g stroke="rgba(255,255,255,.4)" strokeWidth="4" {...line}><path d="M250 74v58M272 74v58" /></g>
    <circle cx="250" cy="96" r="7" fill="#fff" /><circle cx="272" cy="116" r="7" fill={GOLD} />
  </Art>
);

/* Models / dual pipeline: two streams merge into one output */
export const PipelineArt = () => (
  <Art>
    <path d="M78 58C122 58 118 96 150 96M78 134C122 134 118 96 150 96" stroke="rgba(255,255,255,.45)" strokeWidth="5" {...line} />
    <path d="M176 96H214" stroke={GOLD} strokeWidth="6" {...line} />
    <circle cx="60" cy="58" r="18" fill="url(#paGreen)" />
    <circle cx="60" cy="134" r="18" fill="url(#paBlue)" />
    <circle cx="162" cy="96" r="20" fill="url(#paGold)" />
    <rect x="214" y="70" width="58" height="52" rx="16" fill="url(#paGreen)" />
    <path d="M230 98l9 9 17-20" stroke="#fff" strokeWidth="5" {...line} />
  </Art>
);

/* Reports: stacked sheets with a chart */
export const ReportsArt = () => (
  <Art>
    <rect x="80" y="30" width="120" height="142" rx="16" fill="rgba(255,255,255,.14)" transform="rotate(-8 140 100)" />
    <rect x="96" y="22" width="128" height="148" rx="16" fill="#fff" opacity=".96" />
    <rect x="112" y="40" width="54" height="10" rx="5" fill="url(#paGold)" />
    <rect x="112" y="60" width="96" height="6" rx="3" fill="#d8e7df" />
    <rect x="112" y="72" width="72" height="6" rx="3" fill="#d8e7df" />
    {[[116, 26], [138, 44], [160, 34], [182, 58]].map(([x, h]) => (
      <rect key={x} x={x} y={150 - h} width="16" height={h} rx="5" fill="url(#paGreen)" />
    ))}
    <circle cx="240" cy="56" r="20" fill="url(#paGold)" />
    <path d="M240 46v13M240 59l9 6" stroke={DARK} strokeWidth="4" {...line} />
  </Art>
);

/* Jobs: gear + schedule clock */
export const JobsArt = () => (
  <Art>
    <g transform="translate(112 98)">
      {[0, 45, 90, 135].map((a) => <rect key={a} x="-9" y="-52" width="18" height="104" rx="6" fill="url(#paGold)" transform={`rotate(${a})`} />)}
      <circle r="40" fill="url(#paGold)" />
      <circle r="16" fill={DARK} />
    </g>
    <circle cx="226" cy="120" r="34" {...glass} />
    <path d="M226 120V98M226 120l16 8" stroke={G1} strokeWidth="5" {...line} />
    <circle cx="226" cy="120" r="4.5" fill="#fff" />
    <path d="M216 62a34 34 0 0 1 50 24" stroke={G1} strokeWidth="5" strokeDasharray="1 9" {...line} />
    <path d="M258 74l8 12-14 3" stroke={G1} strokeWidth="4.5" {...line} />
  </Art>
);

/* Admin: shield with check */
export const AdminArt = () => (
  <Art>
    <path d="M150 24L218 50v46c0 42-30 64-68 78-38-14-68-36-68-78V50Z" fill="url(#paGreen)" />
    <path d="M150 40L204 60v36c0 32-22 50-54 62" fill="rgba(255,255,255,.12)" />
    <path d="M118 98l24 24 42-48" stroke="#fff" strokeWidth="11" {...line} />
    <circle cx="246" cy="52" r="12" fill="url(#paGold)" /><path d="M234 92q0-16 12-16t12 16Z" fill="url(#paGold)" />
  </Art>
);

export const PAGE_ART = {
  '/': ExecutiveArt,
  '/recommendations': RecommendationsArt,
  '/menu': MenuArt,
  '/pricing': PricingArt,
  '/promotions': PromotionsArt,
  '/basket': BasketArt,
  '/ratings': RatingsArt,
  '/customers': CustomersArt,
  '/peak': PeakArt,
  '/forecast': ForecastArt,
  '/wastage': WastageArt,
  '/locations': LocationsArt,
  '/whatif': WhatIfArt,
  '/models': PipelineArt,
  '/reports': ReportsArt,
  '/jobs': JobsArt,
  '/admin': AdminArt,
};
