export function DishIllustration({ compact = false }) {
  return (
    <svg className={`dish-illustration ${compact ? 'compact' : ''}`} viewBox="0 0 360 230" role="img" aria-label="Stylized restaurant dish with analytics accents">
      <defs>
        <linearGradient id="plateG" x1="0" x2="1"><stop stopColor="#fff"/><stop offset="1" stopColor="#eef7f2"/></linearGradient>
        <linearGradient id="sauceG" x1="0" x2="1"><stop stopColor="#efad2f"/><stop offset="1" stopColor="#d26d2a"/></linearGradient>
        <filter id="shadow"><feDropShadow dx="0" dy="12" stdDeviation="12" floodOpacity=".16"/></filter>
      </defs>
      <g opacity=".65"><circle cx="286" cy="42" r="7" fill="#f1b43a"/><circle cx="318" cy="74" r="4" fill="#5aa581"/><path d="M260 24h54" stroke="#d9e7df" strokeWidth="3" strokeLinecap="round"/></g>
      <g filter="url(#shadow)"><ellipse cx="174" cy="131" rx="126" ry="74" fill="url(#plateG)"/><ellipse cx="174" cy="131" rx="103" ry="56" fill="#f8fbf9" stroke="#dfeae4" strokeWidth="3"/></g>
      <path d="M96 132c18-30 45-43 81-39 31 3 55 18 75 44-22 24-48 34-79 33-34-1-59-13-77-38Z" fill="url(#sauceG)"/>
      <path d="M111 124c24-17 48-21 73-11 15 6 28 17 39 33-28 11-53 12-75 2-16-7-28-15-37-24Z" fill="#e7c75d"/>
      <path d="M125 116c9 18 24 27 45 29 21 2 38-6 51-24" fill="none" stroke="#618e58" strokeWidth="9" strokeLinecap="round"/>
      <circle cx="148" cy="111" r="11" fill="#c44638"/><circle cx="199" cy="135" r="9" fill="#c44638"/><circle cx="176" cy="101" r="7" fill="#f4efe8"/>
      <path d="M118 151c16 8 31 11 46 11M205 105c11 3 23 10 34 20" stroke="#fff6d9" strokeWidth="5" strokeLinecap="round" opacity=".8"/>
      <g className="dish-bars"><rect x="270" y="116" width="15" height="38" rx="6"/><rect x="291" y="94" width="15" height="60" rx="6"/><rect x="312" y="68" width="15" height="86" rx="6"/></g>
      <path d="M268 87c19-12 35-21 53-45" fill="none" stroke="#1f7a5b" strokeWidth="4" strokeLinecap="round"/><path d="m312 43 11-3-2 11" fill="none" stroke="#1f7a5b" strokeWidth="4" strokeLinecap="round" strokeLinejoin="round"/>
      <g opacity=".7"><path d="M55 75c17-16 31-24 46-23" fill="none" stroke="#9fc5b3" strokeWidth="3" strokeDasharray="5 7"/><circle cx="51" cy="79" r="7" fill="#fff" stroke="#9fc5b3" strokeWidth="3"/></g>
    </svg>
  );
}

export function RestaurantAnalyticsIllustration() {
  const nav = [
    ['Executive', true],
    ['Menu', false],
    ['Customers', false],
    ['Forecasts', false],
    ['Actions', false],
  ];

  const kpis = [
    { x: 242, title: 'TOTAL REVENUE', value: '$145.2K', delta: '▲ 8.6% vs last mo.', color: '#7dd79f' },
    { x: 392, title: 'AVG RATING', value: '4.7', delta: '+1.2k new reviews', color: '#7bb7ff' },
    { x: 542, title: 'WASTAGE COST', value: '6.8%', delta: '↓ 1.1% this month', color: '#f2c866' },
  ];

  return (
    <svg
      className="hero-illustration"
      viewBox="0 0 860 620"
      role="img"
      aria-label="Premium restaurant intelligence illustration with analytics dashboard and dish cards"
    >
      <defs>
        <linearGradient id="glassBoard" x1="0" y1="0" x2="1" y2="1">
          <stop offset="0" stopColor="#14392f" />
          <stop offset="1" stopColor="#0d2a22" />
        </linearGradient>
        <linearGradient id="tileFill" x1="0" y1="0" x2="1" y2="1">
          <stop offset="0" stopColor="#174434" />
          <stop offset="1" stopColor="#12372c" />
        </linearGradient>
        <linearGradient id="brandGold" x1="0" x2="1">
          <stop stopColor="#f2b74d" />
          <stop offset="1" stopColor="#da8e20" />
        </linearGradient>
        <linearGradient id="accentGreen" x1="0" x2="1">
          <stop stopColor="#87ddb1" />
          <stop offset="1" stopColor="#2f9470" />
        </linearGradient>
        <linearGradient id="dishWarm" x1="0" x2="1">
          <stop stopColor="#f0b33d" />
          <stop offset="1" stopColor="#d97d2e" />
        </linearGradient>
        <filter id="shadowSoft" x="-20%" y="-20%" width="140%" height="160%">
          <feDropShadow dx="0" dy="18" stdDeviation="20" floodColor="#081b15" floodOpacity=".24" />
        </filter>
        <filter id="shadowTiny" x="-20%" y="-20%" width="140%" height="160%">
          <feDropShadow dx="0" dy="10" stdDeviation="10" floodColor="#071913" floodOpacity=".18" />
        </filter>
      </defs>

      {/* background glow */}
      <circle cx="120" cy="100" r="74" fill="#29634f" opacity=".28" />
      <circle cx="700" cy="110" r="90" fill="#f0b33d" opacity=".08" />
      <circle cx="730" cy="500" r="92" fill="#1f513f" opacity=".22" />
      <path d="M40 170c44-48 93-68 151-61" fill="none" stroke="#86b3a0" strokeOpacity=".38" strokeWidth="4" strokeLinecap="round" strokeDasharray="4 13" />

      {/* ===== Dashboard board ===== */}
      <g filter="url(#shadowSoft)">
        <rect x="100" y="118" width="600" height="354" rx="34" fill="url(#glassBoard)" stroke="#224d3f" />
        <rect x="100" y="118" width="118" height="354" rx="34" fill="#10281f" />

        {/* brand */}
        <rect x="124" y="142" width="50" height="50" rx="15" fill="url(#brandGold)" />
        <text x="149" y="176" textAnchor="middle" fill="#17322a" fontSize="24" fontWeight="800">D</text>
        <text x="124" y="216" fill="#ffffff" fontSize="19" fontWeight="800">DineIQ</text>
        <text x="124" y="232" fill="#7e9e91" fontSize="10" fontWeight="600">menu intelligence</text>

        {/* nav */}
        {nav.map(([label, active], i) => (
          <g key={label} opacity={active ? 1 : 0.8}>
            <rect x="112" y={258 + i * 30} width="94" height="22" rx="11" fill={active ? '#1f7657' : '#18372e'} />
            <circle cx="126" cy={269 + i * 30} r="3.4" fill={active ? '#f4c45f' : '#89a395'} />
            <text x="136" y={273 + i * 30} fill={active ? '#ffffff' : '#9bb1a7'} fontSize="10" fontWeight="700">{label}</text>
          </g>
        ))}

        {/* KPI cards */}
        {kpis.map((card) => (
          <g key={card.title}>
            <rect x={card.x} y="144" width="138" height="82" rx="18" fill="url(#tileFill)" stroke="#244f41" />
            <text x={card.x + 16} y="167" fill="#8ea89d" fontSize="10" fontWeight="700">{card.title}</text>
            <text x={card.x + 16} y="199" fill="#ffffff" fontSize="26" fontWeight="800">{card.value}</text>
            <text x={card.x + 16} y="215" fill={card.color} fontSize="10" fontWeight="700">{card.delta}</text>
          </g>
        ))}

        {/* Sales trend */}
        <g>
          <rect x="242" y="248" width="290" height="206" rx="24" fill="#102b22" stroke="#244f41" />
          <text x="262" y="278" fill="#ffffff" fontSize="15" fontWeight="700">Sales Trend</text>
          <text x="262" y="296" fill="#7f9b90" fontSize="11">Orders and revenue by day</text>
          {[0, 1, 2, 3].map((i) => (
            <line key={i} x1="262" x2="512" y1={326 + i * 28} y2={326 + i * 28} stroke="#21453a" strokeDasharray="4 9" />
          ))}
          <g transform="translate(-48 0)">
            <path d="M310 390 C336 380 344 350 370 333 S420 322 446 340 S494 386 522 365 S547 333 564 338 L564 412 L310 412 Z" fill="#265745" opacity=".26" />
            <path d="M310 390 C336 380 344 350 370 333 S420 322 446 340 S494 386 522 365 S547 333 564 338" fill="none" stroke="url(#accentGreen)" strokeWidth="6" strokeLinecap="round" />
          </g>
          {['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'].map((d, i) => (
            <text key={d} x={264 + i * 46} y="428" fill="#7d978d" fontSize="10">{d}</text>
          ))}
          {/* legend (was an overlay covering the line) */}
          <circle cx="266" cy="442" r="3.5" fill="#2f9470" />
          <text x="274" y="445.5" fill="#95ab9e" fontSize="10" fontWeight="600">Pasta</text>
          <circle cx="322" cy="442" r="3.5" fill="#5887db" />
          <text x="330" y="445.5" fill="#95ab9e" fontSize="10" fontWeight="600">Burgers</text>
          <circle cx="392" cy="442" r="3.5" fill="#efb444" />
          <text x="400" y="445.5" fill="#95ab9e" fontSize="10" fontWeight="600">Salads</text>
        </g>

        {/* Peak hours */}
        <g>
          <rect x="548" y="248" width="132" height="100" rx="22" fill="#102b22" stroke="#244f41" />
          <text x="566" y="272" fill="#ffffff" fontSize="14" fontWeight="700">Peak Hours</text>
          {[0, 1, 2, 3].map((r) =>
            [0, 1, 2, 3, 4].map((c) => (
              <rect
                key={`${r}-${c}`}
                x={566 + c * 16}
                y={282 + r * 12}
                width="12"
                height="9"
                rx="3"
                fill={['#18392f', '#255243', '#32715c', '#efb444', '#2f9470'][(r + c) % 5]}
                opacity={0.55 + ((r + c) % 4) * 0.08}
              />
            ))
          )}
          <text x="566" y="338" fill="#7d978d" fontSize="9.5">Lunch &amp; dinner</text>
        </g>

        {/* Segments */}
        <g>
          <rect x="548" y="356" width="132" height="98" rx="22" fill="#102b22" stroke="#244f41" />
          <text x="566" y="380" fill="#ffffff" fontSize="14" fontWeight="700">Segments</text>
          <g transform="rotate(-90 585 418)">
            <circle cx="585" cy="418" r="16" fill="none" stroke="#21493d" strokeWidth="8" />
            <circle cx="585" cy="418" r="16" fill="none" stroke="#efb444" strokeWidth="8" pathLength="100" strokeDasharray="42 58" strokeDashoffset="0" />
            <circle cx="585" cy="418" r="16" fill="none" stroke="#67b987" strokeWidth="8" pathLength="100" strokeDasharray="36 64" strokeDashoffset="-42" />
            <circle cx="585" cy="418" r="16" fill="none" stroke="#6796e8" strokeWidth="8" pathLength="100" strokeDasharray="22 78" strokeDashoffset="-78" />
          </g>
          {[
            ['Loyal', '#efb444'],
            ['Promo-driven', '#67b987'],
            ['At-risk', '#6796e8'],
          ].map(([label, color], i) => (
            <g key={label}>
              <circle cx="614" cy={406 + i * 13} r="3" fill={color} />
              <text x="622" y={409.5 + i * 13} fill="#95ab9e" fontSize="9.5">{label}</text>
            </g>
          ))}
        </g>
      </g>

      {/* ===== Flagship dish card ===== */}
      <g filter="url(#shadowTiny)">
        <rect x="36" y="432" width="196" height="146" rx="24" fill="#fff8ee" />
        <text x="58" y="458" fill="#9a6812" fontSize="10.5" fontWeight="800">FLAGSHIP DISH</text>
        <g transform="translate(-26 18)">
          <ellipse cx="160" cy="474" rx="46" ry="28" fill="#ffffff" stroke="#e6d9bc" strokeWidth="3" />
          <ellipse cx="160" cy="474" rx="35" ry="19" fill="#f7faf8" stroke="#edf1ed" strokeWidth="2" />
          <path d="M129 474c10-15 25-21 43-20 14 1 25 9 35 22-13 12-27 18-44 18-17-1-27-6-34-20Z" fill="url(#dishWarm)" />
          <path d="M138 468c12 9 24 12 37 9 7-2 13-6 19-12" fill="none" stroke="#5f8e5e" strokeWidth="5" strokeLinecap="round" />
          <circle cx="150" cy="466" r="4.3" fill="#cc4d3d" />
          <circle cx="177" cy="480" r="4" fill="#cc4d3d" />
          <circle cx="164" cy="462" r="3" fill="#f6ecd3" />
        </g>
        <text x="58" y="548" fill="#17322a" fontSize="16" fontWeight="800">Truffle Pasta</text>
        <text x="58" y="564" fill="#6b7b74" fontSize="10.5">4.8 rating • 31% margin • top repeat</text>
      </g>

      {/* ===== Recommended action card ===== */}
      <g filter="url(#shadowTiny)" transform="translate(-40 0)">
        <rect x="608" y="474" width="260" height="124" rx="26" fill="#f8fbf9" />
        <text x="630" y="500" fill="#64796f" fontSize="10.5" fontWeight="800">RECOMMENDED ACTION</text>
        <text x="630" y="526" fill="#17322a" fontSize="20" fontWeight="800">Promote Hidden Gems</text>
        <text x="630" y="546" fill="#6b7d76" fontSize="11.5">Spotlight high-margin dishes with</text>
        <text x="630" y="561" fill="#6b7d76" fontSize="11.5">strong ratings but low visibility.</text>
        <rect x="630" y="574" width="84" height="9" rx="4.5" fill="#d8e7df" />
        <rect x="630" y="574" width="55" height="9" rx="4.5" fill="#2f9470" />
        <text x="846" y="583" textAnchor="end" fill="#1f7657" fontSize="11.5" fontWeight="800">impact: high</text>
      </g>
      {/* ===== Live alert card ===== */}
      <g filter="url(#shadowTiny)">
        <rect x="690" y="88" width="160" height="138" rx="22" fill="#ffffff" />
        <text x="708" y="112" fill="#64796f" fontSize="10.5" fontWeight="800">LIVE ALERT</text>
        <text x="708" y="134" fill="#17322a" fontSize="14" fontWeight="800">Weekend demand</text>
        <text x="708" y="151" fill="#17322a" fontSize="14" fontWeight="800">is rising</text>
        <text x="708" y="171" fill="#6b7d76" fontSize="10.5">Prep more buns and salad</text>
        <text x="708" y="185" fill="#6b7d76" fontSize="10.5">kits before the dinner</text>
        <text x="708" y="199" fill="#6b7d76" fontSize="10.5">rush.</text>
        <rect x="708" y="208" width="100" height="7" rx="3.5" fill="#d8e7df" />
        <rect x="708" y="208" width="70" height="7" rx="3.5" fill="#2f9470" />
      </g>
    </svg>
  );
}

export function MiniSparkline() {
  return <svg className="mini-spark" viewBox="0 0 180 54" aria-hidden="true"><path d="M3 45 C22 42 31 23 47 29 S72 41 88 25 S111 10 126 18 S151 26 177 5" fill="none" stroke="currentColor" strokeWidth="4" strokeLinecap="round"/><path d="M3 45 C22 42 31 23 47 29 S72 41 88 25 S111 10 126 18 S151 26 177 5 L177 54 L3 54Z" fill="currentColor" opacity=".08"/></svg>;
}

export function DishThumb({ name = 'Dish', size = 38 }) {
  let h = 0; for (const c of name) h = (h * 31 + c.charCodeAt(0)) >>> 0;
  const garnish = ['#4f8d5c','#d98435','#b94c42','#d6aa3f'][h % 4];
  const filling = ['#e2bc64','#dc8a3e','#c95a43','#7fa45c'][Math.floor(h / 4) % 4];
  return <span className="dish-thumb" style={{ width:size, height:size }} aria-hidden="true"><svg viewBox="0 0 48 48"><circle cx="24" cy="24" r="22" fill="#f1f6f3"/><ellipse cx="24" cy="27" rx="15" ry="10" fill="#fff" stroke="#dce8e1"/><path d="M13 27c4-8 18-10 23 0-5 7-18 8-23 0Z" fill={filling}/><path d="M17 23c4 5 10 6 15 1" fill="none" stroke={garnish} strokeWidth="3" strokeLinecap="round"/><circle cx="29" cy="27" r="2.4" fill="#f7ead2"/></svg></span>;
}
