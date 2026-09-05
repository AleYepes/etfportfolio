# Bronze Snapshots Schema Catalog & Metric Inventory

This catalog documents the object-level schemas, data contracts, edge-case tolerances, and metric inventories across all 7 snapshot endpoints in `bronze.snapshots`.

---

## Catalog Architectural Rule: Schema Structure vs. Domain Variety

When ingesting, modeling, and validating snapshot payloads:
1. **Structural Schema (Invariant)**: The formal contract defining key names, nesting, data types, nullability, and optionality. This structure remains consistent across all funds regardless of asset class or domicile.
2. **Categorical Domain Variety (Variant)**: The unbounded set of categorical values populated within structural fields (e.g., 56 currency labels, 105 countries, 110 debt types, 491 thematic taxonomy labels, 19,500+ security names).

> **Contract Rule**: Exhaustive categorical enumerations belong in **Domain Reference Taxonomies**, not in structural schemas. The schema defines the *shape* that houses them.

---

## Endpoint 1: `ratios`
- **URL Prefix**: `/tws.proxy/fundamentals/mf_ratios_fundamentals/`
- **Total Blobs Profiled**: 10,223
- **Primary Sample**: [`ratios.json`](./ratios.json)

### Top-Level Fields
| Field | Presence | Type | Description / Sample |
| :--- | :--- | :--- | :--- |
| `as_of_date` | 99.6% | `int` | Assessment date as epoch milliseconds (`1785470400000`). |
| `title_vs` | 43.0% | `str` (nullable) | Category benchmark comparison group (e.g., `"Real Estate Funds"`). |
| `dividend` | 100.0% | `list[MetricBenchmarkItem]` | Dividend yield, growth, and payout metrics vs. category. |
| `financials` | 100.0% | `list[MetricBenchmarkItem]` | Revenue, cash flow, and sales growth metrics vs. category. |
| `fixed_income` | 100.0% | `list[MetricBenchmarkItem]` | Duration, coupon, quality, and yield metrics vs. category. |
| `ratios` | 100.0% | `list[MetricBenchmarkItem]` | Profitability, valuation multiples, and leverage ratios vs. category. |
| `zscore` | 100.0% | `list[ZScoreItem]` | Normalized valuation and factor Z-scores. |

### Component Data Contracts

#### `MetricBenchmarkItem`
Standard model for metrics evaluated against a peer group distribution.
```typescript
interface MetricBenchmarkItem {
  id: string;             // Numeric metric identifier (e.g., "1004926")
  name: string;           // Human-readable metric name (e.g., "Price/Earnings")
  name_tag: string;       // Normalized tag (e.g., "price_earnings")
  value: number;          // Full-precision float (e.g., 38.1496613349)
  value_fmt: string;      // Formatted display string; letter grade for quality (e.g., "38.15", "BBB")
  avg: number;            // Category benchmark mean (full precision float)
  avg_fmt: string;        // Formatted benchmark mean (e.g., "36.48", "BBB")
  min: number;            // Category benchmark minimum
  min_fmt: string;        // Display minimum; may decouple from raw min due to quantile bounds (e.g., "11.46", "B")
  max: number;            // Category benchmark maximum
  max_fmt: string;        // Display maximum; may decouple from raw max due to quantile bounds (e.g., "43.47", "AA")
  percentile: number;     // Fund percentile rank within category (0.0 to 100.0)
  vs: number;             // Standardized variance vs. category mean (-1.0 to 1.0, clamped at boundaries)
}
```

#### `ZScoreItem`
```typescript
interface ZScoreItem {
  id: string;             // Numeric identifier (e.g., "1006632")
  name: string;           // Display name (e.g., "Latest Composite Z-Score")
  name_tag: string;       // Normalized tag (e.g., "Latest_Composite_Z_Score")
  value: number;          // Standard deviation relative to benchmark distribution (full precision)
  value_fmt: string;      // Pre-formatted decimal string (e.g., "-0.18")
}
```

### Observed Metric Tags Inventory
* **`dividend`**: `Dividend_Yield_Weighted_Average`, `Price_to_Dividend`, `DividendPayoutRatio5yr`, `Dividend_Per_Share_3Yr`, `Dividend_Per_Share_1Yr`
* **`financials`**: `Sales_Growth_5_Yr`, `Sales_Per_Share_Growth_3_Year`, `Sales_Per_Share_Growth_1_Year`, `Operating_Cash_Flow_Growth_Rate_3Yr`, `Sales_Growth_3_Year`, `Sales_Growth_1_Year`
* **`fixed_income`**: `effective_maturity`, `nominal_maturity`, `Average_Coupon`, `Yield_to_Maturity`, `average_quality`
* **`ratios`**: `relative_strength`, `Sales_to_Total_Assets`, `Total_Debt_Total_Capital`, `LT_Debt_Shareholders_Equity`, `Total_Debt_Total_Equity`, `Return_on_Investment_1Yr`, `Return_on_Equity_3Yr`, `Return_on_Equity_1Yr`, `EPS_growth_1yr`, `EBIT_to_interest`, `Return_on_Assets_3Yr`, `Total_Assets_Total_Equity`, `Return_on_Assets_1Yr`, `price_cash`, `price_earnings`, `price_book`, `Return_on_Capital_3Yr`, `Return_on_Capital`, `EPS_growth_5yr`, `EPS_growth_3yr`, `price_sales`, `Return_on_Investment_3Yr`
* **`zscore`**: `Latest_Composite_Z_Score`, `Latest_Price_Sales_ZScore`, `Latest_Price_to_Earnings_ZScore`, `Latest_Price_to_Book_ZScore`, `Latest_SPS_Growth_ZScore`, `Weighted_Final_Composite_ZScore`, `Average_Final_Composite_Zscore`, `Latest_Dividend_Yield_ZScore`, `Latest_Return_on_Equity_ZScore`

---

## Endpoint 2: `holdings`
- **URL Prefix**: `/tws.proxy/fundamentals/mf_holdings/`
- **Total Blobs Profiled**: 11,913
- **Primary Sample**: [`holdings.json`](./holdings.json)

### Top-Level Fields
| Field | Presence | Type | Description / Sample |
| :--- | :--- | :--- | :--- |
| `as_of_date` | 100.0% | `int` | Snapshot date as epoch milliseconds (`1785470400000`). |
| `allocation_self` | 100.0% | `list[BreakdownItem]` | Broad asset class exposure (`Equity`, `Cash`, `Fixed Income`, `Other`). |
| `currency` | 100.0% | `list[BreakdownItem]` | Currency exposure. Contains optional `code`. May include `<No Currency>`. |
| `debt_type` | 31.8% | `list[BreakdownItem]` (sparse) | Fixed income instrument breakdown (e.g., `CORP`, `Senior Note`, `Bond`). |
| `debtor` | 31.8% | `list[BreakdownItem]` (sparse) | Credit rating breakdown (e.g., `% Quality/A`, `% Quality/BBB`). |
| `geographic` | 100.0% | `dict[str, str]` | Flat map of regional keys to formatted percentage strings. |
| `industry` | 69.4% | `list[BreakdownItem]` (sparse) | Sector / industry allocation (e.g., `Real Estate`, `Consumer Cyclicals`). |
| `investor_country` | 100.0% | `list[BreakdownItem]` | Country breakdown. Contains optional `country_code`. |
| `maturity` | 31.8% | `list[BreakdownItem]` (sparse) | Maturity duration distribution buckets (e.g., `% Maturity 1 to 3 Years`). |
| `top_10` | 100.0% | `list[TopHoldingItem]` | Largest portfolio holdings (length may be $\le 10$). |
| `top_10_weight` | 100.0% | `str` | Summed percentage of top positions (e.g., `"59.04%"`). |

### Component Data Contracts

#### `BreakdownItem`
Unified model implemented by allocation arrays (`allocation_self`, `currency`, `debt_type`, `debtor`, `industry`, `investor_country`, `maturity`).
```typescript
interface BreakdownItem {
  name: string;               // Category name; may contain placeholders ("<No Currency>", "Unidentified")
  weight: number;             // Floating-point weight; can be negative for hedges/shorts (e.g., -0.0022)
  formatted_weight: string;   // Display formatted percentage; can be negative (e.g., "-0%", "99.76%")
  rank: number;               // 1-based order within breakdown; duplicates may occur across merged groups
  vs: number;                 // Unbounded variance vs. category benchmark (can exceed ±100.0; NOT clamped to [-1, 1])
  code?: string;              // ISO Currency code (present only in `currency`, omitted for placeholders)
  country_code?: string;      // ISO Country code (present in `investor_country`, omitted for "Unidentified")
}
```

#### `TopHoldingItem`
```typescript
interface TopHoldingItem {
  name: string;               // Legal issuer or entity name (e.g., "WELLTOWER INC.")
  ticker?: string;            // Exchange ticker; OMITTED for unlisted/foreign/multi-class entities
  conids: number[];           // Array of 1 to 4 Interactive Brokers internal contract IDs
  rank: number;               // Portfolio position rank (1-based)
  assets_pct: string;         // Pre-formatted portfolio weight (e.g., "8.45%")
}
```

#### `GeographicExposureMap`
Flat map of region codes to formatted percentage strings:
```typescript
type GeographicExposureMap = Record<string, string>;
// Observed keys: "asia", "em_asia", "em_eu", "eu", "jpn", "latam", "mena", "na", "nafr", "others", "uk", "us"
// Note: Values reflect regional group definitions and do not necessarily sum to 100%.
```

---

## Endpoint 3: `profile`
- **URL Prefix**: `/tws.proxy/fundamentals/mf_profile_and_fees/`
- **Total Blobs Profiled**: 20,200
- **Primary Sample**: [`profile.json`](./profile.json)

### Top-Level Fields
| Field | Presence | Type | Description / Sample |
| :--- | :--- | :--- | :--- |
| `symbol` | 100.0% | `str` | Ticker symbol (`"ICF"`). |
| `objective` | 88.3% | `str` (nullable) | Fund investment objective narrative. |
| `jap_fund_warning` | 100.0% | `bool` | Japanese regulatory compliance disclosure flag. |
| `expenses_allocation`| 100.0% | `list[ExpenseAllocationItem]` | High-level split between management and non-management fees. |
| `fund_and_profile` | 100.0% | `list[FundProfileField]` | Key-value profile and regulatory attributes. |
| `mstar` | 39.8% | `MorningstarStyleBox` (sparse) | Style box classification and coordinate/historical grids. |
| `reports` | 100.0% | `list[FinancialReport]` | Prospectus, Annual Report schedules, and stub records. |
| `themes` | 100.0% | `list[str]` | Classification tags (e.g., `["Index Tracking", "Ethical"]`). |

### Component Data Contracts

#### `ExpenseAllocationItem`
```typescript
interface ExpenseAllocationItem {
  name: string;               // "Management Expenses" | "Non-Management Expenses"
  ratio: number;              // Float ratio (-0.0 to 1.0; can be signed zero)
  value: string;              // Pre-formatted percentage string (e.g., "100%", "0%")
}
```

#### `FundProfileField`
```typescript
interface FundProfileField {
  name: string;               // Display field name (e.g., "Inception Date", "Total Net Assets (Month End)")
  name_tag: string;           // Normalized identifier; WARNING: may contain unescaped spaces (e.g., "Japanese ITA Broad Category")
  value: string;              // String value; holds heterogeneous formats ("YYYY/MM/DD", "YYYY-MM-DD", "M/D", "$2.1B (2026/07/31)")
  value_tag?: string;         // Standardized enumeration tag for value (e.g., "paid_tag")
}
```

#### `MorningstarStyleBox`
```typescript
interface MorningstarStyleBox {
  name: string;               // Style box classification (e.g., "International Large-Cap Core")
  x_axis: string[];           // ["Core", "Growth", "Value"]
  x_axis_tag: string[];       // ["core", "growth", "value"]
  y_axis: string[];           // ["Large", "Mid", "Multi", "Small"]
  y_axis_tag: string[];       // ["large", "mid", "multi", "small"]
  selected: number[][] | [];  // Active coordinates [[y_index, x_index]]; EMPTY [] if unrated
  hist: number[][] | [];      // Historical frequency matrix; EMPTY [] if historical style unavailable
}
```

#### `FinancialReport`
```typescript
interface FinancialReport {
  name?: string;              // "Prospectus Report" | "Annual Report"; OMITTED on stub records
  as_of_date: number;         // Epoch ms; set to 0 on unpopulated stub records
  fields?: Array<{            // OMITTED on stub records
    name: string;             // Fee line item (e.g., "Management Fees", "Total Expense")
    value: string;            // Pre-formatted fee percentage (e.g., "0.3167%")
    is_summary?: boolean;     // Present and true ONLY for rollup summary rows; omitted when false
  }>;
}
```

---

## Endpoint 4: `lipper`
- **URL Prefix**: `/tws.proxy/fundamentals/mf_lip_ratings/`
- **Total Blobs Profiled**: 7,424
- **Primary Sample**: [`lipper.json`](./lipper.json)

### Top-Level Fields
| Field | Presence | Type | Description / Sample |
| :--- | :--- | :--- | :--- |
| `universes` | 100.0% | `list[LipperUniverse]` | Peer evaluation groups partitioned by jurisdiction / registration. |

*Note: Ingestions may return an empty object `{}` when ratings are entirely unavailable.*

### Component Data Contracts

#### `LipperUniverse`
Represents an evaluation group. **Names are non-unique**: multiple entries with the same `name` (e.g., multiple `"United States"` or `"Peru"` records) can coexist to evaluate distinct share classes, benchmarks, or currency-hedged sub-universes.
```typescript
interface LipperUniverse {
  name: string;               // Geographic universe name (e.g., "United States", "Chile", "Peru", "UK", "Spain")
  as_of_date: number;         // Rating effective date as epoch ms; CAN BE STALE/HISTORICAL (e.g., 2010 or 2016 timestamps)
  overall: LipperMetric[];    // Full-cycle ratings; may contain only a single metric subset (e.g., only "preservation")
  "3_year"?: LipperMetric[];  // 3-year trailing ratings (sparse; omitted in incomplete feeds)
  "5_year"?: LipperMetric[];  // 5-year trailing ratings (sparse; omitted in newer or international feeds)
  "10_year"?: LipperMetric[]; // 10-year trailing ratings (sparse; omitted in newer or international feeds)
}
```

#### `LipperMetric`
```typescript
interface LipperMetric {
  name: string;               // Display metric label (e.g., "Consistent Return", "Preservation")
  name_tag: string;           // "consistent_return" | "expense" | "preservation" | "tax_efficiency" | "total_return"
  rating: {
    name: string;             // Peer context count string (e.g., "199 funds", "11931 funds")
    value: number;            // Rating score: 1 (Lowest) to 5 (Highest)
  };
}
```

---

## Endpoint 5: `mstar`
- **URL Prefix**: `/tws.proxy/mstar/fund/detail?conid=`
- **Total Blobs Profiled**: 10,361
- **Primary Sample**: [`mstar.json`](./mstar.json)

### Top-Level Fields
| Field | Presence | Type | Description / Sample |
| :--- | :--- | :--- | :--- |
| `as_of_date` | 61.9% | `str` (nullable) | Rating date formatted as `"YYYYMMDD"` (`"20260831"`). |
| `q_full_report_id` | 74.8% | `str` (nullable) | Hexadecimal document report ID (`"636d1b68e26a17cb57af48f4"`). |
| `summary` | 100.0% | `list[MstarSummaryPillar]` | Pillar scores, Medalist ratings, and qualitative/quantitative evaluations. |
| `commentary` | 100.0% | `list[MstarCommentaryArticle]` | Analyst narratives and automated research write-ups. |

### Component Data Contracts

#### `MstarSummaryPillar`
Contains both qualitative scores and automated algorithmic counterparts prefixed with `q_`.
```typescript
interface MstarSummaryPillar {
  id: string;                 // Observed: "category" | "category_index" | "medalist_rating" | "morningstar_rating" |
                              //           "parent" | "people" | "process" | "q_parent" | "q_people" | "q_process" |
                              //           "quantitative_rating" | "sustainability_rating"
  title: string;              // Pillar display title (e.g., "Medalist Rating", "Process", "Sustainability Rating")
  value: string;              // Assigned tier (e.g., "High", "Above_Average", "Average", "Below_Average", "Neutral", "Silver", "3")
  q: boolean;                 // Flag: true if quantitatively derived via model, false if analyst-assigned
  publish_date?: string;      // Effective date as "YYYYMMDD"; present across most rating/pillar records
}
```

#### `MstarCommentaryArticle`
Multiple commentary sections can share identical `id` tags (e.g., multiple `"process"` sections from different dates or scopes), distinguished by `subsection_id` and `subtitle`.
```typescript
interface MstarCommentaryArticle {
  id: string;                 // Section ID: "people", "parent", "performance", "price", "process", "summary", "sustainability"
  title: string;              // Heading title (e.g., "Process", "Summary")
  text: string;               // Rich editorial text; contains raw HTML markup (<p>, <b>)
  q: boolean;                 // Flag: true for quantitative automation, false for analyst-written
  publish_date: string;       // Publication date as "YYYYMMDD"
  author?: {                  // Author metadata block; present for named analysts AND "Morningstar Automated Analysis";
    name: string;             // Omitted primarily on quantitative feeds (e.g., sustainability with q: true)
  };
  subsection_id?: string;     // Granular subsection tag (e.g., "process_approach", "process_portfolio", "summary_body")
  subtitle?: string;          // Granular subsection subtitle (e.g., "Approach", "Portfolio", "Body", "Title")
}
```

---

## Endpoint 6: `esg`
- **URL Prefix**: `/tws.proxy/impact/esg/`
- **Total Blobs Profiled**: 2,588
- **Primary Sample**: [`esg.json`](./esg.json)

### Top-Level Fields
| Field | Presence | Type | Description / Sample |
| :--- | :--- | :--- | :--- |
| `asOfDate` | 100.0% | `str` | Assessment date as `"YYYYMMDD"` (**note camelCase naming**). |
| `symbol` | 100.0% | `str` | Ticker symbol (`"ICF"`). |
| `title` | 100.0% | `str` | Payload descriptor (`"ESG"`). |
| `source` | 99.8% | `str` (nullable) | Calculation origin (`"CALCULATED"`). |
| `coverage` | 99.8% | `float` (nullable) | Portfolio coverage ratio (0.0 to 1.0, e.g., `0.99762`). |
| `no_settings` | 100.0% | `bool` | Impact profile configuration override flag. |
| `content` | 100.0% | `list[ESGNode]` | Recursive Refinitiv ESG scores tree. Root contains pillar trees and composite scores. |

### Component Data Contracts

#### `ESGNode`
Hierarchical tree node. Composite scores are leaf nodes at the root; pillar parent nodes contain nested `children`.
```typescript
interface ESGNode {
  name: string;               // Refinitiv metric code (e.g., "TRESGS", "TRESGENS")
  value: number;              // Standardized score integer (1 to 10)
  children?: Array<{         // Pillar sub-category scores; present only on pillar nodes
    name: string;             // Sub-metric code (e.g., "TRESGENERS")
    value: number;            // Standardized sub-score integer (1 to 10)
  }>;
}
```

### Refinitiv Metric Hierarchy Reference
```
├── TRESGS (ESG Score)
├── TRESGCS (ESG Combined Score)
├── TRESGCCS (ESG Controversies Score)
├── TRESGENS (Environmental Pillar)
│   ├── TRESGENRRS (Resource Use Score)
│   ├── TRESGENERS (Emissions Score)
│   └── TRESGENPIS (Environmental Innovation Score)
├── TRESGSOS (Social Pillar)
│   ├── TRESGSOWOS (Workforce Score)
│   ├── TRESGSOHRS (Human Rights Score)
│   ├── TRESGSOCOS (Community Score)
│   └── TRESGSOPRS (Product Responsibility Score)
└── TRESGCGS (Governance Pillar)
    ├── TRESGCGBDS (Management Score)
    ├── TRESGCGSRS (Shareholders Score)
    └── TRESGCGVSS (CSR Strategy Score)
```

---

## Endpoint 7: `theme_weights`
- **URL Prefix**: `/tws.proxy/knowledge-graph/ui/fund?conid=`
- **Total Blobs Profiled**: 10,845
- **Primary Sample**: [`theme_weights.json`](./theme_weights.json)

### Top-Level Fields
| Field | Presence | Type | Description / Sample |
| :--- | :--- | :--- | :--- |
| `conid` | 100.0% | `int` | Interactive Brokers contract identifier (`8335`). |
| `symbol` | 100.0% | `str` | Ticker symbol (`"ICF"`). |
| `name` | 100.0% | `str` | Formal fund name (`"ISHARES SELECT U.S. REIT ETF"`). |
| `assetType` | 100.0% | `str` | Security asset classification (`"STK"`). |
| `exchange` | 100.0% | `str` | Exchange routing code (`"SMART"`). |
| `coverage` | 100.0% | `float` | Thematic entity-resolution coverage ratio (`0.99762`). |
| `themes` | 100.0% | `list[ThematicExposureItem]` | Thematic exposure model scores and rank weights. |

### Component Data Contracts

#### `ThematicExposureItem`
> **Relational Link**: `key` directly references `theme_id` in `bronze.themes`. Contrast with `profile.themes`, which is a flat array of qualitative strings (`list[str]`).
```typescript
interface ThematicExposureItem {
  key: string;                  // Canonical UUID matching bronze.themes.theme_id
  name: string;                 // Display label (e.g., "Discount Retail", "AI Infrastructure")
  weight: number;               // Unadjusted raw thematic weight (full-precision float)
  rank_adjusted_weight: number; // Factor-adjusted portfolio thematic exposure weight (full-precision float)
}
```

---

## Cross-Endpoint Ingestion Traps & Implementation Notes

| Issue | Affected Endpoints | Manifestation & Guidance |
| :--- | :--- | :--- |
| **Date Serialization Divergence** | All | • **Epoch milliseconds (`int`)**: `holdings.as_of_date`, `profile.reports[].as_of_date`, `lipper.universes[].as_of_date`, `ratios.as_of_date`.<br>• **`"YYYYMMDD"` (`str`)**: `mstar.as_of_date`, `mstar.commentary[].publish_date`, `mstar.summary[].publish_date`, `esg.asOfDate`.<br>• **Heterogeneous Strings**: `profile.fund_and_profile` serializes dates as `"YYYY/MM/DD"`, `"YYYY-MM-DD"`, `"M/D"`, or embedded inside strings (`"$2.1B (2026/07/31)"`). |
| **Key Casing Inconsistency** | `esg` vs. All | `esg.json` uses camelCase `asOfDate`. All other endpoints use snake_case `as_of_date`. Ingestion pipelines must normalize this property. |
| **`themes` Property Collision** | `profile` vs. `theme_weights` | • In `profile.json`: `themes` is `list[str]` (qualitative tags).<br>• In `theme_weights.json`: `themes` is `list[ThematicExposureItem]` (weighted UUID structures). |
| **Stub Financial Reports** | `profile` | Some entries in `profile.reports` contain only `{"as_of_date": 0}` with no `name` and no `fields`. Parsers must treat `name` and `fields` as optional on report models. |
| **`TopHoldingItem` Ticker Sparsity** | `holdings` | Foreign, unlisted, or multi-class securities omit the `ticker` key. Schemas must mark `ticker` as optional/nullable. |
| **Negative Weights & Unbounded `vs`** | `holdings` | • `weight` can be negative (e.g., `-0.0022`) and formatted as `"-0%"`.<br>• `vs` in `holdings` represents raw basis point/percentage spread and is **not bounded** to $[-1, 1]$ (can exceed $\pm 100.0$). Contrast with `ratios`, where `vs` is clamped to $[-1.0, 1.0]$. |
| **Lipper Universe Collisions & Stale Snapshots** | `lipper` | • The `universes` array may contain multiple records with identical `name` strings (e.g., two `"United States"` blocks) representing distinct share classes.<br>• Non-US universes may contain legacy snapshots (e.g., UK/Spain snapshots dated 2010 or 2016) alongside current US snapshots.<br>• Trailing windows (`3_year`, `5_year`, `10_year`) and specific metrics within them are frequently omitted. |
| **MStar Author Attribution** | `mstar` | `commentary[].author` is frequently populated with `{"name": "Morningstar Automated Analysis"}` on quantitative articles (`q: false`), but omitted on quantitative feeds like sustainability (`q: true`). Do not use `author == null` to infer automated generation. |
| **MStar Summary Quantitative Duals** | `mstar` | Morningstar emits both qualitative (`people`, `process`, `parent`) and model-derived (`q_people`, `q_process`, `q_parent`) pillars within the same payload. |
| **Style Box Empty Matrix State** | `profile` | When Morningstar style ratings are unassigned, `mstar.selected` and `mstar.hist` are emitted as empty arrays `[]` rather than coordinate grids. |
| **Unescaped Spaces in Tags** | `profile` | `fund_and_profile[].name_tag` occasionally contains raw spaces (e.g., `"Japanese ITA Broad Category"`). Parsers must not enforce strict snake_case regex validation. |
| **Empty Blob Failures** | `lipper`, `mstar`, `esg`, `theme_weights` | Approximately 1 blob per 10,000 ingestions returns `{}` when data is unavailable for a given contract. Deserializers must validate for root-level empty objects prior to reading required fields. |
| **Embedded HTML Content** | `mstar` | `mstar.commentary[].text` embeds formatted HTML tags (`<p>`, `<b>`). Downstream text renderers must either sanitize or parse this content as HTML. |