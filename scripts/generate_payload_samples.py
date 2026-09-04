"""Inspect bronze snapshots and generate representative Frankenstein JSON payloads and schema catalog.

This script scans unique payload blobs in data/etf.duckdb across all 7 snapshot
endpoints (excluding landing), generates property-complete sample JSON payloads,
and writes the authoritative structural schema catalog to docs/samples/.
"""

from __future__ import annotations

import copy
import shutil
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import duckdb
import orjson
import zstandard as zstd

DB_PATH = "data/etf.duckdb"
OUTPUT_DIR = Path("docs/samples")
OLD_OUTPUT_DIR = Path("data/samples")

# The 7 detail endpoints in bronze.snapshots
ENDPOINTS: dict[str, str] = {
    "ratios": "/tws.proxy/fundamentals/mf_ratios_fundamentals/",
    "holdings": "/tws.proxy/fundamentals/mf_holdings/",
    "profile": "/tws.proxy/fundamentals/mf_profile_and_fees/",
    "lipper": "/tws.proxy/fundamentals/mf_lip_ratings/",
    "mstar": "/tws.proxy/mstar/fund/detail?conid=",
    "esg": "/tws.proxy/impact/esg/",
    "theme_weights": "/tws.proxy/knowledge-graph/ui/fund?conid=",
}

# Candidate identifier fields for semantic metric/factor items
# 'subsection_id' is placed before 'id' to prevent collisions in Morningstar commentaries
ITEM_ID_CANDIDATES = ("name_tag", "subsection_id", "id", "name", "key", "country_code", "code")

# Semantic metric/factor lists where we maintain the full union of distinct metric items
METRIC_FACTOR_LISTS = frozenset(
    {
        "ratios",
        "financials",
        "fixed_income",
        "dividend",
        "zscore",
        "fund_and_profile",
        "summary",
        "commentary",
        "content",
        "children",
        "reports",
        "fields",
        "expenses_allocation",
        "10_year",
        "3_year",
        "5_year",
        "overall",
    }
)


def _get_item_key(item: dict[str, Any], list_name: str) -> str | None:
    """Return a unique semantic identifier for a list item."""
    # Composite key for commentary items that share section IDs
    if "id" in item and "subsection_id" in item and item["subsection_id"]:
        return f"commentary:{item['id']}#{item['subsection_id']}"

    for id_field in ITEM_ID_CANDIDATES:
        if id_field in item and item[id_field] is not None:
            return f"{id_field}:{item[id_field]}"
    return None


def merge_dicts(
    target: dict[str, Any],
    source: dict[str, Any],
    path: str = "",
    seen_signatures_by_list: dict[str, set[frozenset[str]]] | None = None,
) -> dict[str, Any]:
    """Recursively deep-merge source dict into target dict."""
    if seen_signatures_by_list is None:
        seen_signatures_by_list = {}
    for k, v in source.items():
        sub_path = f"{path}.{k}" if path else k
        if k not in target or target[k] is None:
            if isinstance(v, list):
                target[k] = merge_lists([], v, sub_path, seen_signatures_by_list)
            elif isinstance(v, dict):
                target[k] = merge_dicts({}, v, sub_path, seen_signatures_by_list)
            else:
                target[k] = copy.deepcopy(v)
        elif isinstance(target[k], dict) and isinstance(v, dict):
            target[k] = merge_dicts(target[k], v, sub_path, seen_signatures_by_list)
        elif isinstance(target[k], list) and isinstance(v, list):
            target[k] = merge_lists(target[k], v, sub_path, seen_signatures_by_list)
        elif target[k] == "" and v:
            target[k] = v
    return target


def merge_lists(
    target: list[Any],
    source: list[Any],
    list_name: str,
    seen_signatures_by_list: dict[str, set[frozenset[str]]] | None = None,
) -> list[Any]:
    """Merge two lists respecting the boundary between invariant schema and domain variety.

    - For METRIC_FACTOR_LISTS: Preserves the full union of distinct metrics/fields,
      merging attributes on match so every observed property variant is present.
    - For high-cardinality entity lists (top_10 holdings, currency, countries, debt types, theme weights):
      Guarantees every distinct property signature (e.g. presence vs absence of optional
      discriminators like 'code' or 'country_code') is retained, while capping samples
      at a compact 3-5 items to avoid payload bloating.
    - For primitives (e.g. themes list of strings): keeps up to 10 distinct values.
    """
    if not source:
        return target

    if seen_signatures_by_list is None:
        seen_signatures_by_list = {}

    list_basename = list_name.split(".")[-1]
    is_metric_list = list_basename in METRIC_FACTOR_LISTS

    if (target and isinstance(target[0], dict)) or (source and isinstance(source[0], dict)):
        target_by_id: dict[str, dict[str, Any]] = {}
        for idx, item in enumerate(target):
            if isinstance(item, dict):
                item_id = _get_item_key(item, list_name) or f"idx_{idx}"
                target_by_id[item_id] = item

        seen_sigs = seen_signatures_by_list.setdefault(list_name, set())
        for item in target:
            if isinstance(item, dict):
                seen_sigs.add(frozenset(item.keys()))

        max_sample_len = 5

        for item in source:
            if not isinstance(item, dict):
                continue

            item_id = _get_item_key(item, list_name)
            item_signature = frozenset(item.keys())

            if is_metric_list:
                if item_id and item_id in target_by_id:
                    # Merge properties into existing item
                    target_by_id[item_id] = merge_dicts(target_by_id[item_id], item, list_name, seen_signatures_by_list)
                    seen_sigs.add(frozenset(target_by_id[item_id].keys()))
                elif item_id:
                    # Metric item not yet seen: preserve full union
                    new_item = copy.deepcopy(item)
                    target_by_id[item_id] = new_item
                    target.append(new_item)
                    seen_sigs.add(item_signature)
                else:
                    # Nameless metric item (e.g. {'as_of_date': 0} placeholder in reports)
                    if item in target:
                        continue
                    if item_signature not in seen_sigs or len(target) < max_sample_len:
                        target.append(copy.deepcopy(item))
                        seen_sigs.add(item_signature)
            else:
                # Entity list (e.g. universes, top_10, currency, themes):
                # Keep up to max_sample_len standard items, PLUS any new structural shape
                is_new_structural_shape = item_signature not in seen_sigs
                if is_new_structural_shape or len(target) < max_sample_len:
                    if item in target:
                        continue
                    new_item = copy.deepcopy(item)
                    if item_id:
                        target_by_id[item_id] = new_item
                    target.append(new_item)
                    seen_sigs.add(item_signature)

        return target

    if (target and isinstance(target[0], (str, int, float, bool))) or (
        source and isinstance(source[0], (str, int, float, bool))
    ):
        seen = set(target)
        for val in source:
            if val not in seen and len(target) < 10:
                seen.add(val)
                target.append(val)
        return target

    return target


@dataclass
class FieldStats:
    seen_types: Counter[str] = field(default_factory=Counter)
    present_count: int = 0
    null_count: int = 0
    sample_values: list[Any] = field(default_factory=list)


def profile_endpoint(
    conn: duckdb.DuckDBPyConnection,
    dctx: zstd.ZstdDecompressor,
    endpoint_name: str,
    url_prefix: str,
) -> tuple[dict[str, Any], dict[str, FieldStats], int]:
    """Scan all blobs for an endpoint using cursor streaming and construct Frankenstein payload."""
    cursor = conn.execute(
        """
        SELECT b.payload
        FROM (SELECT DISTINCT hash FROM bronze.snapshots WHERE url_prefix = $1) AS s
        JOIN bronze.payload_blobs b ON s.hash = b.hash
        """,
        [url_prefix],
    )

    frankenstein: dict[str, Any] = {}
    field_stats: dict[str, FieldStats] = defaultdict(FieldStats)
    total_blobs = 0
    sigs_by_list: dict[str, set[frozenset[str]]] = {}

    while True:
        batch = cursor.fetchmany(1000)
        if not batch:
            break

        for (blob,) in batch:
            total_blobs += 1
            raw = dctx.decompress(blob)
            payload = orjson.loads(raw)

            if not isinstance(payload, dict) or not payload:
                field_stats["__ROOT__"].seen_types["empty_dict"] += 1
                continue

            frankenstein = merge_dicts(frankenstein, payload, "", sigs_by_list)

            for k, v in payload.items():
                stats = field_stats[k]
                stats.present_count += 1
                if v is None:
                    stats.null_count += 1
                    stats.seen_types["null"] += 1
                else:
                    stats.seen_types[type(v).__name__] += 1
                    if (
                        len(stats.sample_values) < 2
                        and not isinstance(v, (dict, list))
                        and v not in stats.sample_values
                    ):
                        stats.sample_values.append(v)

    return frankenstein, field_stats, total_blobs


SCHEMA_CATALOG_CONTENT = """# Bronze Snapshots Schema Catalog & Metric Inventory

This catalog documents the object-level schemas, data contracts, and metric inventories across all 7 snapshot endpoints in `bronze.snapshots`.

---

## Catalog Architectural Rule: Schema Structure vs. Domain Variety

When ingesting, modeling, and validating snapshot payloads:
1. **Structural Schema (Invariant)**: The formal contract defining key names, nesting, data types, and nullability. This structure remains consistent across all funds regardless of asset class or domicile.
2. **Categorical Domain Variety (Variant)**: The unbounded set of categorical values populated within structural fields (e.g., 56 distinct currency names, 105 countries, 110 debt types, 491 thematic taxonomy labels, 19,500+ security names).

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
  value: number;          // Fund absolute metric value (e.g., 38.14966)
  value_fmt: string;      // Formatted display value (e.g., "38.15", "BBB")
  avg: number;            // Category benchmark mean (e.g., 36.47636)
  avg_fmt: string;        // Formatted category benchmark mean (e.g., "36.48")
  min: number;            // Category benchmark minimum (e.g., 27.17376)
  min_fmt: string;        // Formatted category minimum (e.g., "11.46")
  max: number;            // Category benchmark maximum (e.g., 40.79230)
  max_fmt: string;        // Formatted category maximum (e.g., "43.47")
  percentile: number;     // Fund percentile rank within category (0.0 to 100.0)
  vs: number;             // Standardized variance score vs. category mean (-1.0 to 1.0)
}
```

#### `ZScoreItem`
```typescript
interface ZScoreItem {
  id: string;             // Numeric identifier (e.g., "1006632")
  name: string;           // Display name (e.g., "Latest Composite Z-Score")
  name_tag: string;       // Normalized tag (e.g., "Latest_Composite_Z_Score")
  value: number;          // Standard deviation relative to benchmark distribution
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
| `allocation_self` | 100.0% | `list[BreakdownItem]` | Broad asset class exposure (`Equity`, `Fixed Income`, `Cash`, `Other`). |
| `currency` | 100.0% | `list[BreakdownItem]` | Currency exposure breakdown. Contains optional `code`. |
| `debt_type` | 31.8% | `list[BreakdownItem]` (sparse) | Fixed income instrument breakdown (e.g., `CORP`, `Senior Note`). |
| `debtor` | 31.8% | `list[BreakdownItem]` (sparse) | Credit rating breakdown (e.g., `% Quality/BBB`, `% Quality/A`). |
| `geographic` | 100.0% | `dict[str, str]` | Key-value mapping of region codes to formatted percentages. |
| `industry` | 69.4% | `list[BreakdownItem]` (sparse) | Sector / industry allocation (e.g., `Real Estate`). |
| `investor_country` | 100.0% | `list[BreakdownItem]` | Country breakdown. Contains optional `country_code`. |
| `maturity` | 31.8% | `list[BreakdownItem]` (sparse) | Maturity duration distribution buckets. |
| `top_10` | 100.0% | `list[TopHoldingItem]` | The 10 largest individual fund holdings. |
| `top_10_weight` | 100.0% | `str` | Summed percentage of top 10 positions (e.g., `"59.04%"`). |

### Component Data Contracts

#### `BreakdownItem`
Unified structure implemented by all allocation arrays (`allocation_self`, `currency`, `debt_type`, `debtor`, `industry`, `investor_country`, `maturity`).
```typescript
interface BreakdownItem {
  name: string;               // Category name (e.g., "Equity", "US Dollar", "Real Estate")
  weight: number;             // Precise floating point percentage (e.g., 99.762)
  formatted_weight: string;   // Display formatted percentage (e.g., "99.76%")
  rank: number;               // 1-based order within breakdown category
  vs: number;                 // Variance vs. benchmark peer category
  code?: string;              // ISO Currency code (present only in `currency`)
  country_code?: string;      // ISO Country code (present only in `investor_country`)
}
```

#### `TopHoldingItem`
```typescript
interface TopHoldingItem {
  name: string;               // Entity legal name (e.g., "WELLTOWER INC.")
  ticker: string;             // Exchange ticker symbol (e.g., "WELL")
  conids: number[];           // Contract identifiers (Interactive Brokers internal IDs)
  rank: number;               // Ranking position (1 to 10)
  assets_pct: string;         // Pre-formatted portfolio weight (e.g., "8.45%")
}
```

#### `GeographicExposureMap`
Flat map of region codes to display weights:
```typescript
type GeographicExposureMap = Record<string, string>;
// Observed keys: "asia", "em_asia", "em_eu", "eu", "jpn", "latam", "mena", "na", "nafr", "others", "uk", "us"
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
| `fund_and_profile` | 100.0% | `list[FundProfileField]` | Dynamic key-value profile properties. |
| `mstar` | 39.8% | `MorningstarStyleBox` (sparse) | Morningstar Style Box coordinates and historical matrix. |
| `reports` | 100.0% | `list[FinancialReport]` | Prospectus and Annual Report fee and expense schedules. |
| `themes` | 100.0% | `list[str]` | Regulatory and classification labels (e.g., `["Index Tracking", "Ethical"]`). |

### Component Data Contracts

#### `ExpenseAllocationItem`
```typescript
interface ExpenseAllocationItem {
  name: string;               // "Management Expenses" | "Non-Management Expenses"
  ratio: number;              // Float ratio (0.0 to 1.0)
  value: string;              // Display percentage string (e.g., "100%", "0%")
}
```

#### `FundProfileField`
```typescript
interface FundProfileField {
  name: string;               // Display field name (e.g., "Inception Date")
  name_tag: string;           // Normalized identifier (e.g., "Inception_Date")
  value: string;              // Field value serialized as string (e.g., "2001/01/29", "$2.1B")
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
  selected: number[][];       // Active coordinates [[y_index, x_index]]
  hist: number[][];           // Historical frequency matrix across style coordinates
}
```

#### `FinancialReport`
```typescript
interface FinancialReport {
  name: string;               // "Prospectus Report" | "Annual Report"
  as_of_date: number;         // Filing effective date as epoch milliseconds
  fields: Array<{
    name: string;             // Fee line item (e.g., "Management Fees", "Sub-Advisor Expenses")
    value: string;            // Pre-formatted fee percentage (e.g., "0.3167%")
    is_summary?: boolean;     // Present and true if row represents an aggregate roll-up
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
| `universes` | 100.0% | `list[LipperUniverse]` | Geographic peer universes evaluating the fund. |

*Note: 1 profiled blob was an empty object `{}` when ratings were unavailable.*

### Component Data Contracts

#### `LipperUniverse`
Represents an evaluation group partitioned by geographic fund domicile / registration.
```typescript
interface LipperUniverse {
  name: string;               // Geographic universe name (e.g., "United States", "Chile", "Peru")
  as_of_date: number;         // Rating effective date as epoch milliseconds
  overall: LipperMetric[];    // Full-cycle Lipper Leader ratings
  "3_year": LipperMetric[];   // 3-year trailing ratings
  "5_year": LipperMetric[];   // 5-year trailing ratings
  "10_year": LipperMetric[];  // 10-year trailing ratings (may be omitted for newer funds)
}
```

#### `LipperMetric`
```typescript
interface LipperMetric {
  name: string;               // Metric label (e.g., "Consistent Return", "Preservation")
  name_tag: string;           // "consistent_return" | "expense" | "preservation" | "tax_efficiency" | "total_return"
  rating: {
    name: string;             // Peer count context string (e.g., "199 funds", "11931 funds")
    value: number;            // Lipper Leader rating score: 1 (Lowest) to 5 (Highest)
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
| `q_full_report_id` | 74.8% | `str` (nullable) | Hex identifier linking to Morningstar report document. |
| `summary` | 100.0% | `list[MstarSummaryPillar]` | Pillar scores and Medalist rating attributes. |
| `commentary` | 100.0% | `list[MstarCommentaryArticle]` | Analyst narratives and automated research write-ups. |

*Note: 1 profiled blob was an empty object `{}`.*

### Component Data Contracts

#### `MstarSummaryPillar`
```typescript
interface MstarSummaryPillar {
  id: string;                 // Pillar code: "medalist_rating", "people", "process", "parent", "sustainability_rating", "category"
  title: string;              // Pillar display title (e.g., "Medalist Rating", "People")
  value: string;              // Assigned tier: "Above_Average", "Average", "Below_Average", "Neutral", "Silver", "3"
  q: boolean;                 // Flag: true if quantitatively derived via model, false if analyst-assigned
  publish_date?: string;      // Effective date as "YYYYMMDD" (sparse, present on ratings)
}
```

#### `MstarCommentaryArticle`
```typescript
interface MstarCommentaryArticle {
  id: string;                 // Section ID: "people", "parent", "performance", "price", "process", "summary", "sustainability"
  title: string;              // Heading title
  text: string;               // Rich editorial text (contains HTML tags: <p>, <b>)
  q: boolean;                 // Flag: true if generated by Morningstar Automated Analysis
  publish_date: string;       // Publication date as "YYYYMMDD"
  author?: {                  // Author metadata (omitted for automated analyses)
    name: string;
  };
  subsection_id?: string;     // Granular subsection tag (e.g., "process_approach", "summary_body")
  subtitle?: string;          // Granular subsection subtitle (e.g., "Approach", "Body")
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
| `content` | 100.0% | `list[ESGNode]` | Recursive/hierarchical Refinitiv ESG scores tree. |

*Note: 1 profiled blob was an empty object `{}`.*

### Component Data Contracts

#### `ESGNode`
Hierarchical tree structure. Root contains top-level composite scores alongside pillar parents containing `children`.
```typescript
interface ESGNode {
  name: string;               // LSEG / Refinitiv metric code (e.g., "TRESGS", "TRESGENS")
  value: number;              // Standardized score integer (1 to 10)
  children?: Array<{         // Sub-category scores (present only on pillar nodes)
    name: string;             // Sub-metric code (e.g., "TRESGENERS" - Emissions Score)
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

*Note: 1 profiled blob was an empty object `{}`.*

### Component Data Contracts

#### `ThematicExposureItem`
> **Relational Link**: `key` directly references `theme_id` in `bronze.themes`. Contrast with `profile.themes`, which is a flat array of qualitative strings (`list[str]`).
```typescript
interface ThematicExposureItem {
  key: string;                  // Canonical UUID matching bronze.themes.theme_id
  name: string;                 // Display label (e.g., "Data Centers", "AI Infrastructure")
  weight: number;               // Unadjusted raw thematic weight (0.0 to 1.0)
  rank_adjusted_weight: number; // Factor-adjusted portfolio thematic exposure weight
}
```

---

## Cross-Endpoint Ingestion Traps & Implementation Notes

| Issue | Affected Endpoints | Manifestation & Guidance |
| :--- | :--- | :--- |
| **Date Serialization Divergence** | All | • **Epoch milliseconds (`int`)**: `holdings.as_of_date`, `profile.reports[].as_of_date`, `lipper.universes[].as_of_date`, `ratios.as_of_date`.<br>• **`"YYYYMMDD"` (`str`)**: `mstar.as_of_date`, `mstar.commentary[].publish_date`, `mstar.summary[].publish_date`, `esg.asOfDate`.<br>• **Slash dates (`"YYYY/MM/DD"`)**: `profile.fund_and_profile` (`Inception_Date`, `Manager_Tenure`). |
| **Key Casing Inconsistency** | `esg` vs. All | `esg.json` defines `asOfDate` (camelCase). All other endpoints use `as_of_date` (snake_case). Parsers must normalize or alias this key. |
| **`themes` Property Collision** | `profile` vs. `theme_weights` | • In `profile.json`: `themes` is `list[str]` (qualitative flags).<br>• In `theme_weights.json`: `themes` is `list[ThematicExposureItem]` (weighted UUID structures). |
| **Fixed Income Sparsity** | `holdings` | `debt_type`, `debtor`, and `maturity` appear only in funds with debt exposure (~31.8% of blobs). In pure equity funds, these keys are omitted. Schemas must designate them as optional/nullable. |
| **Empty Blob Failures** | `lipper`, `mstar`, `esg`, `theme_weights` | Approximately 1 blob per 10,000 ingestions returns `{}` when data is unavailable for a given contract. Deserializers must validate for root-level empty objects prior to reading required fields. |
| **Embedded HTML Content** | `mstar` | `mstar.commentary[].text` frequently embeds formatted HTML tags (`<p>`, `<b>`). Downstream text renderers must either sanitize or parse this content as HTML. |
"""


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(DB_PATH, read_only=True)
    dctx = zstd.ZstdDecompressor()

    for name, prefix in ENDPOINTS.items():
        print(f"Profiling {name} ({prefix})...")
        frankenstein, stats, total_blobs = profile_endpoint(conn, dctx, name, prefix)

        # Write Frankenstein JSON to docs/samples/
        json_path = OUTPUT_DIR / f"{name}.json"
        with open(json_path, "wb") as f:
            f.write(orjson.dumps(frankenstein, option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS))

        print(f"  Wrote {json_path} (from {total_blobs} blobs)")

    # Write authoritative schema catalog
    catalog_path = OUTPUT_DIR / "schema_catalog.md"
    catalog_path.write_text(SCHEMA_CATALOG_CONTENT, encoding="utf-8")
    print(f"Wrote schema catalog to {catalog_path}")

    # Clean up legacy data/samples if present
    if OLD_OUTPUT_DIR.exists():
        shutil.rmtree(OLD_OUTPUT_DIR)
        print(f"Removed legacy directory {OLD_OUTPUT_DIR}")


if __name__ == "__main__":
    main()
