/* More illustrations in the same style as RestaurantAnalyticsIllustration.
   Exports: MenuMatrixIllustration, CustomerSegmentsIllustration,
            DemandForecastIllustration, WastageControlIllustration
   All use viewBox 860x620 and className "hero-illustration". */

const GOLD = '#efb444';
const GREEN = '#2f9470';
const BLUE = '#5887db';
const RED = '#cc4d3d';

function ArtDefs() {
  return (
    <defs>
      <linearGradient id="artBoard" x1="0" y1="0" x2="1" y2="1"><stop offset="0" stopColor="#14392f" /><stop offset="1" stopColor="#0d2a22" /></linearGradient>
      <linearGradient id="artTile" x1="0" y1="0" x2="1" y2="1"><stop offset="0" stopColor="#174434" /><stop offset="1" stopColor="#12372c" /></linearGradient>
      <linearGradient id="artGreen" x1="0" x2="1"><stop stopColor="#87ddb1" /><stop offset="1" stopColor="#2f9470" /></linearGradient>
      <linearGradient id="artWarm" x1="0" x2="1"><stop stopColor="#f0b33d" /><stop offset="1" stopColor="#d97d2e" /></linearGradient>
      <filter id="artSoft" x="-20%" y="-20%" width="140%" height="160%"><feDropShadow dx="0" dy="18" stdDeviation="20" floodColor="#081b15" floodOpacity=".24" /></filter>
      <filter id="artTiny" x="-20%" y="-20%" width="140%" height="160%"><feDropShadow dx="0" dy="10" stdDeviation="10" floodColor="#071913" floodOpacity=".18" /></filter>
    </defs>
  );
}

function Frame({ label, children }) {
  return (
    <svg className="hero-illustration" viewBox="0 0 860 620" role="img" aria-label={label}>
      <ArtDefs />
      <circle cx="110" cy="90" r="74" fill="#29634f" opacity=".28" />
      <circle cx="720" cy="110" r="90" fill="#f0b33d" opacity=".08" />
      <circle cx="740" cy="500" r="92" fill="#1f513f" opacity=".22" />
      <path d="M40 170c44-48 93-68 151-61" fill="none" stroke="#86b3a0" strokeOpacity=".38" strokeWidth="4" strokeLinecap="round" strokeDasharray="4 13" />
      {children}
    </svg>
  );
}

function Board({ title, sub, children }) {
  return (
    <g filter="url(#artSoft)">
      <rect x="120" y="100" width="560" height="340" rx="34" fill="url(#artBoard)" stroke="#224d3f" />
      <text x="146" y="134" fill="#ffffff" fontSize="16" fontWeight="800">{title}</text>
      <text x="146" y="152" fill="#7f9b90" fontSize="11">{sub}</text>
      {children}
    </g>
  );
}

const Panel = ({ x, y, w, h, fill = '#102b22', stroke = '#244f41' }) => (
  <rect x={x} y={y} width={w} height={h} rx="22" fill={fill} stroke={stroke} />
);

function Tile({ x, y, w, h = 78, label, value, sub, color }) {
  return (
    <g>
      <rect x={x} y={y} width={w} height={h} rx="18" fill="url(#artTile)" stroke="#244f41" />
      <text x={x + 16} y={y + 21} fill="#8ea89d" fontSize="10" fontWeight="700">{label}</text>
      <text x={x + 16} y={y + 49} fill="#ffffff" fontSize="24" fontWeight="800">{value}</text>
      <text x={x + 16} y={y + 65} fill={color} fontSize="10" fontWeight="700">{sub}</text>
    </g>
  );
}

function LightCard({ x, y, w, h, label, bg = '#f8fbf9', labelColor = '#64796f', children }) {
  return (
    <g filter="url(#artTiny)">
      <rect x={x} y={y} width={w} height={h} rx="24" fill={bg} />
      <text x={x + 22} y={y + 24} fill={labelColor} fontSize="10.5" fontWeight="800">{label}</text>
      {children}
    </g>
  );
}

const Track = ({ x, y, w, fill, color = GREEN }) => (
  <>
    <rect x={x} y={y} width={w} height="8" rx="4" fill="#d8e7df" />
    <rect x={x} y={y} width={fill} height="8" rx="4" fill={color} />
  </>
);

/* ---------------------------------------------------------------- */
export function MenuMatrixIllustration() {
  const cells = [
    { x: 146, y: 172, name: 'Stars', sub: 'Top sales, top margin', hot: true, color: GOLD, dots: [[36, 76, 7], [72, 92, 5], [100, 68, 9], [58, 100, 4]] },
    { x: 300, y: 172, name: 'Hidden gems', sub: 'High margin, low sales', color: GREEN, dots: [[30, 70, 6], [60, 96, 8], [98, 78, 5]] },
    { x: 146, y: 296, name: 'Workhorses', sub: 'High sales, low margin', color: BLUE, dots: [[40, 74, 8], [78, 90, 6], [104, 68, 5]] },
    { x: 300, y: 296, name: 'Underperformers', sub: 'Low sales, low margin', color: RED, dots: [[34, 92, 5], [70, 74, 6], [102, 96, 4]] },
  ];
  return (
    <Frame label="Menu matrix illustration with dish quadrants and pricing signal">
      <Board title="Menu Matrix" sub="Popularity vs margin, by dish">
        {cells.map((c) => (
          <g key={c.name}>
            <rect x={c.x} y={c.y} width="146" height="116" rx="20" fill={c.hot ? '#1a3f31' : '#102b22'} stroke={c.hot ? GOLD : '#244f41'} strokeOpacity={c.hot ? 0.7 : 1} />
            <text x={c.x + 14} y={c.y + 24} fill="#ffffff" fontSize="11.5" fontWeight="700">{c.name}</text>
            <text x={c.x + 14} y={c.y + 38} fill="#7f9b90" fontSize="9.5">{c.sub}</text>
            {c.dots.map(([dx, dy, r], i) => <circle key={i} cx={c.x + dx} cy={c.y + dy} r={r} fill={c.color} opacity={0.85} />)}
          </g>
        ))}
        <Tile x={470} y={172} w={164} label="DISHES TAGGED" value="42" sub="▲ 6 new this month" color="#7dd79f" />
        <Tile x={470} y={256} w={164} label="STAR DISHES" value="8" sub="19% of the menu" color="#f2c866" />
        <Tile x={470} y={340} w={164} label="TO REPRICE" value="5" sub="Low price, high demand" color="#7bb7ff" />
      </Board>

      <LightCard x={60} y={400} w={220} h={120} label="STAR DISH" bg="#fff8ee" labelColor="#9a6812">
        <text x={82} y={452} fill="#17322a" fontSize="16" fontWeight="800">Butter Chicken</text>
        <text x={82} y={470} fill="#6b7b74" fontSize="10.5">4.7 rating • 38% margin</text>
        <Track x={82} y={488} w={100} fill={78} color={GOLD} />
        <ellipse cx={244} cy={456} rx="24" ry="15" fill="#ffffff" stroke="#e6d9bc" strokeWidth="2" />
        <path d="M228 456c6-9 15-12 25-11 8 1 13 5 19 12-8 7-16 10-26 10-10 0-14-3-18-11Z" fill="url(#artWarm)" />
        <circle cx={240} cy={452} r="2.6" fill={RED} />
      </LightCard>

      <LightCard x={640} y={96} w={190} h={108} label="PRICING SIGNAL">
        <text x={662} y={142} fill="#17322a" fontSize="13" fontWeight="800">Naan is under-priced</text>
        <text x={662} y={160} fill="#6b7d76" fontSize="10.5">+$0.50 keeps demand</text>
        <text x={662} y={174} fill="#6b7d76" fontSize="10.5">steady, adds margin.</text>
        <Track x={662} y={184} w={100} fill={66} />
      </LightCard>
    </Frame>
  );
}

/* ---------------------------------------------------------------- */
export function CustomerSegmentsIllustration() {
  const segs = [['Loyal', GOLD, 38], ['Promo-driven', '#67b987', 27], ['Occasional', '#6796e8', 20], ['At-risk', RED, 15]];
  let offset = 0;
  const rings = segs.map(([name, color, pct]) => {
    const el = <circle key={name} cx="256" cy="298" r="50" fill="none" stroke={color} strokeWidth="18" pathLength="100" strokeDasharray={`${pct - 1} ${101 - pct}`} strokeDashoffset={-offset} />;
    offset += pct;
    return el;
  });
  const rfm = [['Loyal', 138], ['Promo-driven', 96], ['Occasional', 70], ['At-risk', 34]];
  return (
    <Frame label="Customer segmentation illustration with donut, RFM bars and loyal guest card">
      <Board title="Customer Segments" sub="RFM view of active guests">
        <Panel x={146} y={170} w={220} h={246} />
        <text x={168} y={196} fill="#ffffff" fontSize="14" fontWeight="700">Segment mix</text>
        <g transform="rotate(-90 256 298)">
          <circle cx="256" cy="298" r="50" fill="none" stroke="#21493d" strokeWidth="18" />
          {rings}
        </g>
        <text x="256" y="298" textAnchor="middle" fill="#ffffff" fontSize="20" fontWeight="800">12.4K</text>
        <text x="256" y="314" textAnchor="middle" fill="#7f9b90" fontSize="10">guests</text>
        {segs.map(([name, color], i) => (
          <g key={name}>
            <circle cx={170 + (i % 2) * 88} cy={372 + Math.floor(i / 2) * 20} r="3.5" fill={color} />
            <text x={178 + (i % 2) * 88} y={375.5 + Math.floor(i / 2) * 20} fill="#95ab9e" fontSize="10">{name}</text>
          </g>
        ))}

        <Panel x={386} y={170} w={268} h={150} />
        <text x={406} y={196} fill="#ffffff" fontSize="14" fontWeight="700">RFM score</text>
        {rfm.map(([name, w], i) => (
          <g key={name}>
            <text x={406} y={222 + i * 26} fill="#95ab9e" fontSize="10">{name}</text>
            <rect x={486} y={214 + i * 26} width="120" height="9" rx="4.5" fill="#21493d" />
            <rect x={486} y={214 + i * 26} width={w * 0.86} height="9" rx="4.5" fill={segs[i][1]} />
            <text x={634} y={222 + i * 26} textAnchor="end" fill="#ffffff" fontSize="10" fontWeight="700">{w}</text>
          </g>
        ))}
        <Tile x={386} y={334} w={128} h={82} label="REPEAT RATE" value="64%" sub="▲ 3.2% this month" color="#7dd79f" />
        <Tile x={526} y={334} w={128} h={82} label="CHURN RISK" value="9.1%" sub="↓ 0.8% this month" color="#f2c866" />
      </Board>

      <LightCard x={40} y={420} w={210} h={112} label="LOYAL GUEST">
        <circle cx={78} cy={472} r="18" fill="url(#artWarm)" />
        <text x={78} y={478} textAnchor="middle" fill="#17322a" fontSize="16" fontWeight="800">A</text>
        <text x={106} y={468} fill="#17322a" fontSize="15" fontWeight="800">Aisha K.</text>
        <text x={106} y={484} fill="#6b7d76" fontSize="10.5">18 visits • $412 spent</text>
        <Track x={62} y={498} w={160} fill={125} />
        <text x={62} y={523} fill="#1f7657" fontSize="10.5" fontWeight="800">78% to Platinum</text>
      </LightCard>

      <LightCard x={664} y={400} w={190} h={120} label="WIN-BACK IDEA">
        <text x={686} y={450} fill="#17322a" fontSize="14" fontWeight="800">Free dessert offer</text>
        <text x={686} y={468} fill="#6b7d76" fontSize="10.5">for guests inactive</text>
        <text x={686} y={482} fill="#6b7d76" fontSize="10.5">over 30 days.</text>
        <Track x={686} y={496} w={90} fill={60} />
        <text x={834} y={504} textAnchor="end" fill="#1f7657" fontSize="10.5" fontWeight="800">high</text>
      </LightCard>
    </Frame>
  );
}

/* ---------------------------------------------------------------- */
export function DemandForecastIllustration() {
  return (
    <Frame label="Demand forecast illustration with confidence band and prep alert">
      <Board title="Demand Forecast" sub="Next 14 days • MAPE 6.2%">
        <Panel x={146} y={170} w={350} h={246} />
        <text x={168} y={196} fill="#ffffff" fontSize="14" fontWeight="700">Orders per day</text>
        {[0, 1, 2, 3].map((i) => <line key={i} x1="170" x2="472" y1={220 + i * 40} y2={220 + i * 40} stroke="#21453a" strokeDasharray="4 9" />)}
        <line x1="340" x2="340" y1="208" y2="372" stroke="#3b6b5a" strokeDasharray="3 6" />
        <text x="346" y="216" fill={GOLD} fontSize="10" fontWeight="700">Forecast</text>
        <path d="M340 236 C370 218 390 188 410 194 S450 234 472 204 L472 252 C450 280 430 240 410 230 S370 246 340 260 Z" fill={GOLD} opacity=".16" />
        <path d="M170 330 C200 320 210 280 240 270 S290 300 310 262 S330 242 340 248" fill="none" stroke="url(#artGreen)" strokeWidth="6" strokeLinecap="round" />
        <path d="M340 248 C370 232 390 200 410 210 S450 250 472 228" fill="none" stroke={GOLD} strokeWidth="5" strokeLinecap="round" strokeDasharray="2 10" />
        {['Mon', 'Wed', 'Fri', 'Sun', 'Tue', 'Thu'].map((d, i) => <text key={d} x={170 + i * 56} y="392" fill="#7d978d" fontSize="10">{d}</text>)}
        <circle cx="172" cy="408" r="3.5" fill={GREEN} /><text x="180" y="411.5" fill="#95ab9e" fontSize="10">Actual</text>
        <circle cx="232" cy="408" r="3.5" fill={GOLD} /><text x="240" y="411.5" fill="#95ab9e" fontSize="10">Forecast</text>

        <Tile x={516} y={170} w={138} label="PEAK DAY" value="Fri" sub="+18% vs average" color="#f2c866" />
        <Tile x={516} y={254} w={138} label="NEXT WEEK" value="$38.6K" sub="▲ 5.1% forecast" color="#7dd79f" />
        <Tile x={516} y={338} w={138} label="COVERS" value="1,240" sub="expected guests" color="#7bb7ff" />
      </Board>

      <LightCard x={650} y={84} w={190} h={118} label="PREP ALERT">
        <text x={672} y={132} fill="#17322a" fontSize="14" fontWeight="800">Friday dinner surge</text>
        <text x={672} y={150} fill="#6b7d76" fontSize="10.5">Stock 20% more chicken</text>
        <text x={672} y={164} fill="#6b7d76" fontSize="10.5">and bread before 6 PM.</text>
        <Track x={672} y={176} w={100} fill={72} />
      </LightCard>

      <LightCard x={50} y={400} w={200} h={110} label="MODEL CHECK">
        <text x={72} y={456} fill="#17322a" fontSize="26" fontWeight="800">93.8%</text>
        <text x={72} y={474} fill="#6b7d76" fontSize="10.5">accuracy, last 8 weeks</text>
        <Track x={72} y={486} w={152} fill={143} />
      </LightCard>
    </Frame>
  );
}

/* ---------------------------------------------------------------- */
export function WastageControlIllustration() {
  const rows = [['Produce', '$2.4K', 180, GOLD], ['Dairy', '$1.7K', 130, GREEN], ['Bakery', '$1.2K', 92, GREEN], ['Proteins', '$0.9K', 70, GREEN], ['Beverages', '$0.4K', 36, GREEN]];
  return (
    <Frame label="Wastage control illustration with cost by category and recommended action">
      <Board title="Wastage Control" sub="Cost of waste by category">
        <Panel x={146} y={170} w={340} h={246} />
        {rows.map(([name, val, w, color], i) => {
          const cy = 222 + i * 38;
          return (
            <g key={name}>
              <text x={168} y={cy + 4} fill="#95ab9e" fontSize="11">{name}</text>
              <rect x={244} y={cy - 6} width="180" height="12" rx="6" fill="#21493d" />
              <rect x={244} y={cy - 6} width={w} height="12" rx="6" fill={color} />
              <text x={468} y={cy + 4} textAnchor="end" fill="#ffffff" fontSize="11" fontWeight="700">{val}</text>
            </g>
          );
        })}
        <text x={168} y={404} fill="#7d978d" fontSize="10">Waste peaks on Sunday after close</text>

        <Tile x={506} y={170} w={148} h={82} label="WASTAGE COST" value="6.8%" sub="↓ 1.1% after actions" color="#f2c866" />
        <Tile x={506} y={264} w={148} h={82} label="SAVED THIS MONTH" value="$4.2K" sub="▲ 12% vs target" color="#7dd79f" />
        <rect x={506} y={358} width="148" height="58" rx="18" fill="url(#artTile)" stroke="#244f41" />
        <text x={522} y={378} fill="#8ea89d" fontSize="10" fontWeight="700">WASTE TARGET</text>
        <rect x={522} y={388} width="116" height="8" rx="4" fill="#21493d" />
        <rect x={522} y={388} width="84" height="8" rx="4" fill={GOLD} />
        <text x={522} y={409} fill="#f2c866" fontSize="10" fontWeight="700">72% to target</text>
      </Board>

      <LightCard x={50} y={400} w={210} h={112} label="HIGH-RISK ITEM">
        <text x={72} y={448} fill="#17322a" fontSize="15" fontWeight="800">Spinach &amp; herbs</text>
        <text x={72} y={466} fill="#6b7d76" fontSize="10.5">34% likely to spoil by Sun</text>
        <Track x={72} y={480} w={162} fill={55} color={RED} />
      </LightCard>

      <LightCard x={632} y={424} w={220} h={120} label="RECOMMENDED ACTION">
        <text x={654} y={474} fill="#17322a" fontSize="18" fontWeight="800">Cut prep by 15%</text>
        <text x={654} y={492} fill="#6b7d76" fontSize="11">Lower Sunday prep for produce</text>
        <text x={654} y={506} fill="#6b7d76" fontSize="11">and dairy, then re-check.</text>
        <Track x={654} y={519} w={84} fill={46} />
        <text x={830} y={527} textAnchor="end" fill="#1f7657" fontSize="11" fontWeight="800">impact: medium</text>
      </LightCard>
    </Frame>
  );
}
