import argparse
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path


def _normalize_endpoint(url: str) -> str:
    """Extracts clean endpoint name from full URL or prefix."""
    if "fundamentals/landing" in url:
        return "landing"
    if "fundamentals/mf_holdings" in url:
        return "holdings"
    if "fundamentals/mf_ratios_fundamentals" in url:
        return "ratios"
    if "fundamentals/mf_profile_and_fees" in url:
        return "profile"
    if "fundamentals/mf_lip_ratings" in url:
        return "lipper"
    if "mstar/fund/detail" in url:
        return "mstar"
    if "impact/esg" in url:
        return "esg"
    if "knowledge-graph/ui/fund" in url:
        return "theme_weights"
    if "knowledge-graph/meta/themes" in url:
        return "themes_meta"
    if "search/products-by-filters" in url:
        return "products_search"
    return url.split("?")[0].split("/")[-1] or url


def parse_log(log_path: Path) -> dict:
    counts = {
        "products": Counter(),
        "contracts": Counter(),
        "gateway": Counter(),
        "prices": Counter(),
        "themes": Counter(),
        "details": {
            "landing_gate": Counter(),
            "rate_limiter": Counter(),
            "backoff_delays": Counter(),
            "endpoints_attempted": Counter(),
            "endpoints_429": Counter(),
            "endpoints_failed": Counter(),
            "distinct_products": set(),
            "failed_products": set(),
            "start_time": None,
            "end_time": None,
        },
    }

    incremental_bars = 0
    refetched_bars = 0
    bar_distribution = Counter()

    re_inc = re.compile(r"Product (\d+): incremental price update complete \((\d+) bars\)")
    re_refetch = re.compile(r"Product (\d+): mismatch refetch archived and replaced \((\d+) bars\)")
    re_price_target = re.compile(r"Price ingestion:\s+(\d+)\s+products to process")
    re_qual_target = re.compile(r"Contract qualification:\s+(\d+)\s+products to process")
    re_landing_flag = re.compile(r"Landing for product (\d+): changed=(\w+)")
    re_landing_fail = re.compile(r"Landing fetch failed for product (\d+):")
    re_ep_fail = re.compile(r"Failed to fetch (\w+) for product (\d+): (.*)")
    re_gated_unsatisfied = re.compile(r"Product (\d+): gated endpoint\(s\) not satisfied")
    re_req_fail = re.compile(r"Request to (\S+) failed \(status (\d+)\), attempt (\d+)/(\d+)")
    re_pause = re.compile(r"Pausing outbound requests for ([\d\.]+)s \(next backoff: ([\d\.]+)s\)")

    with open(log_path, encoding="utf-8", errors="ignore") as f:
        for line in f:
            line_str = line.strip()
            if not line_str:
                continue

            # Parse line timestamp if available
            line_ts = None
            if len(line_str) >= 19 and line_str[4] == "-" and line_str[7] == "-":
                try:
                    line_ts = datetime.strptime(line_str[:19], "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    pass

            # Products phase
            if "etfportfolio.ingest.products" in line_str or "etfportfolio.ingestion.products" in line_str:
                if "Fetching products page" in line_str:
                    counts["products"]["page_fetches"] += 1
                elif "Received" in line_str and "products on page" in line_str:
                    counts["products"]["pages_received"] += 1
                elif "Pagination complete" in line_str:
                    counts["products"]["pagination_complete"] += 1

            # Contracts phase
            elif "etfportfolio.ingest.contracts" in line_str or "etfportfolio.ingestion.contracts" in line_str or "ib_async.wrapper" in line_str:
                if "Contract qualification:" in line_str:
                    counts["contracts"]["qualification_starts"] += 1
                    m = re_qual_target.search(line_str)
                    if m:
                        counts["contracts"]["target_products"] = int(m.group(1))
                elif "No contract details found" in line_str or "No security definition has been found" in line_str:
                    counts["contracts"]["missing_contracts"] += 1

            # Gateway
            elif "etfportfolio.ingest.gateway" in line_str or "etfportfolio.ingestion.gateway" in line_str:
                if "Connected to IB Gateway" in line_str:
                    counts["gateway"]["connected"] += 1
                elif "Disconnected from IB Gateway" in line_str:
                    counts["gateway"]["disconnected"] += 1

            # Prices phase
            elif "etfportfolio.ingest.prices" in line_str or "etfportfolio.ingestion.prices" in line_str:
                if "Price ingestion:" in line_str:
                    counts["prices"]["batch_starts"] += 1
                    m = re_price_target.search(line_str)
                    if m:
                        counts["prices"]["target_products"] = int(m.group(1))
                elif "value_mismatch detected" in line_str:
                    counts["prices"]["mismatch_detected"] += 1
                elif "incremental price update complete" in line_str:
                    counts["prices"]["incremental_updates"] += 1
                    m = re_inc.search(line_str)
                    if m:
                        bars = int(m.group(2))
                        incremental_bars += bars
                        bar_distribution[bars] += 1
                elif "mismatch refetch archived and replaced" in line_str or "corporate action confirmed; archived" in line_str:
                    counts["prices"]["mismatch_replaced"] += 1
                    m = re_refetch.search(line_str)
                    if m:
                        refetched_bars += int(m.group(2))

            # Themes phase
            elif "etfportfolio.ingest.themes" in line_str or "etfportfolio.ingestion.themes" in line_str:
                if "Syncing theme taxonomy" in line_str:
                    counts["themes"]["sync_started"] += 1
                elif "Theme taxonomy synced successfully" in line_str:
                    counts["themes"]["sync_succeeded"] += 1
                elif "Themes sync skipped" in line_str:
                    counts["themes"]["sync_skipped"] += 1

            # Details / Landing / Session
            is_details_line = any(
                k in line_str
                for k in (
                    "etfportfolio.ingest.details",
                    "etfportfolio.ingestion.details",
                    "etfportfolio.ingest.landing",
                    "etfportfolio.ingestion.landing",
                    "etfportfolio.ingest.snapshots",
                    "etfportfolio.ingestion.snapshots",
                    "etfportfolio.ingest.session",
                )
            )

            if is_details_line:
                det = counts["details"]
                if line_ts:
                    if det["start_time"] is None or line_ts < det["start_time"]:
                        det["start_time"] = line_ts
                    if det["end_time"] is None or line_ts > det["end_time"]:
                        det["end_time"] = line_ts

                # Landing checks (pre-refactor)
                m_land = re_landing_flag.search(line_str)
                if m_land:
                    pid, changed_str = int(m_land.group(1)), m_land.group(2)
                    det["distinct_products"].add(pid)
                    det["landing_gate"]["checks"] += 1
                    if changed_str == "True":
                        det["landing_gate"]["changed_true"] += 1
                    else:
                        det["landing_gate"]["changed_false"] += 1

                m_lfail = re_landing_fail.search(line_str)
                if m_lfail:
                    pid = int(m_lfail.group(1))
                    det["distinct_products"].add(pid)
                    det["failed_products"].add(pid)
                    det["landing_gate"]["landing_failures"] += 1

                m_gated_unsat = re_gated_unsatisfied.search(line_str)
                if m_gated_unsat:
                    det["landing_gate"]["gated_unsatisfied"] += 1

                # Endpoint failures
                m_epf = re_ep_fail.search(line_str)
                if m_epf:
                    ep_name, pid, err_msg = m_epf.group(1), int(m_epf.group(2)), m_epf.group(3)
                    det["distinct_products"].add(pid)
                    det["failed_products"].add(pid)
                    det["endpoints_failed"][ep_name] += 1

                # Outbound request retries / failures
                m_req = re_req_fail.search(line_str)
                if m_req:
                    url, status_code, attempt, max_retries = m_req.group(1), int(m_req.group(2)), int(m_req.group(3)), int(m_req.group(4))
                    ep_name = _normalize_endpoint(url)
                    if status_code == 429:
                        det["endpoints_429"][ep_name] += 1
                        det["rate_limiter"]["status_429_retries"] += 1

                # RateLimiter events (post-refactor)
                if "HTTP 429 hit. Pausing outbound requests for" in line_str:
                    det["rate_limiter"]["pause_waves"] += 1
                    m_p = re_pause.search(line_str)
                    if m_p:
                        delay = float(m_p.group(1))
                        det["rate_limiter"]["total_pause_seconds"] += delay
                        det["rate_limiter"]["max_pause_seconds"] = max(det["rate_limiter"]["max_pause_seconds"], delay)
                        det["backoff_delays"][f"{delay:.1f}s"] += 1
                elif "HTTP 429 received within active wave" in line_str:
                    det["rate_limiter"]["in_wave_suppressed"] += 1

    counts["prices"]["total_incremental_bars"] = incremental_bars
    counts["prices"]["total_refetched_bars"] = refetched_bars
    counts["prices"]["bar_distribution"] = bar_distribution
    return counts


def print_report(log_path: Path, data: dict) -> None:
    print("=" * 80)
    print(f"LOG REPORT: {log_path.name}")
    print("=" * 80)

    # PRODUCTS
    if data["products"]:
        print("\n[PRODUCTS]")
        for k, v in data["products"].items():
            print(f"  {k}: {v}")

    # CONTRACTS
    if data["contracts"]:
        print("\n[CONTRACTS]")
        for k, v in data["contracts"].items():
            print(f"  {k}: {v}")

    # GATEWAY
    if data["gateway"]:
        print("\n[GATEWAY]")
        for k, v in data["gateway"].items():
            print(f"  {k}: {v}")

    # PRICES
    if data["prices"]["batch_starts"] or data["prices"]["incremental_updates"]:
        print("\n[PRICES]")
        for k, v in data["prices"].items():
            if k in ("bar_distribution", "total_incremental_bars", "total_refetched_bars"):
                continue
            print(f"  {k}: {v}")
        print(f"  total_incremental_bars: {data['prices']['total_incremental_bars']}")
        print(f"  total_refetched_bars: {data['prices']['total_refetched_bars']}")
        if data["prices"]["bar_distribution"]:
            print("  bars_distribution:")
            for bars, freq in sorted(data["prices"]["bar_distribution"].items()):
                print(f"    {bars} bars: {freq} products")

    # THEMES
    if data["themes"]:
        print("\n[THEMES]")
        for k, v in data["themes"].items():
            print(f"  {k}: {v}")

    # DETAILS
    det = data["details"]
    start = det["start_time"]
    end = det["end_time"]
    duration_s = (end - start).total_seconds() if (start and end) else 0.0
    products_count = len(det["distinct_products"])
    failed_products_count = len(det["failed_products"])
    succeeded_products = products_count - failed_products_count

    print("\n[DETAILS INGESTION PHASE]")
    print(f"  Time span: {start} -> {end} ({duration_s:.1f}s = {duration_s/60:.1f}m = {duration_s/3600:.2f}h)")
    print(f"  Distinct products touched: {products_count}")
    print(f"  Products fully succeeded: {succeeded_products}")
    print(f"  Products with failures:    {failed_products_count}")

    # Legacy Landing Gate section
    if det["landing_gate"]["checks"] > 0:
        lg = det["landing_gate"]
        ch_true = lg["changed_true"]
        ch_false = lg["changed_false"]
        total_ch = ch_true + ch_false
        pct_true = (ch_true / total_ch * 100) if total_ch else 0.0
        pct_false = (ch_false / total_ch * 100) if total_ch else 0.0
        print("\n  [Legacy Landing Gate Analysis]")
        print(f"    Total landing checks:    {lg['checks']}")
        print(f"    Landing changed (True):  {ch_true} ({pct_true:.1f}%) -> triggered gated endpoints fetch")
        print(f"    Landing unchanged (False): {ch_false} ({pct_false:.1f}%) -> SKIPPED 5 gated endpoints")
        print(f"    Landing failures:        {lg['landing_failures']}")
        print(f"    Gated unsatisfied:      {lg['gated_unsatisfied']}")

    # Rate Limiting & Throttling
    rl = det["rate_limiter"]
    total_429 = rl["status_429_retries"] + rl["pause_waves"] + rl["in_wave_suppressed"]
    print("\n  [Rate Limiting (HTTP 429) & Throttling]")
    print(f"    RateLimiter pause waves: {rl['pause_waves']}")
    print(f"    RateLimiter wave suppressions: {rl['in_wave_suppressed']}")
    print(f"    Total pause duration:    {rl['total_pause_seconds']:.1f}s ({rl['total_pause_seconds']/60:.1f}m)")
    print(f"    Max pause wave duration: {rl['max_pause_seconds']:.1f}s")
    if det["backoff_delays"]:
        print("    Backoff delay distribution:")
        for delay, count in sorted(det["backoff_delays"].items(), key=lambda x: float(x[0].rstrip('s'))):
            print(f"      {delay}: {count} waves")
    if det["endpoints_429"]:
        print("    HTTP 429 retries by endpoint:")
        for ep, count in det["endpoints_429"].most_common():
            print(f"      {ep}: {count}")

    # Terminal Endpoint Failures
    if det["endpoints_failed"]:
        print("\n  [Terminal Endpoint Failures (retries exhausted)]")
        total_ep_fails = sum(det["endpoints_failed"].values())
        print(f"    Total failed endpoint requests: {total_ep_fails}")
        for ep, count in det["endpoints_failed"].most_common():
            print(f"      {ep}: {count}")
    else:
        print("\n  [Terminal Endpoint Failures]: 0")
    print()


def compare_all(logs_dir: Path) -> None:
    log_files = sorted(logs_dir.glob("*.log"))
    results = []
    for lf in log_files:
        if lf.stat().st_size == 0:
            continue
        data = parse_log(lf)
        det = data["details"]
        if det["distinct_products"] or det["landing_gate"]["checks"] or det["rate_limiter"]["pause_waves"] or det["endpoints_429"]:
            start = det["start_time"]
            end = det["end_time"]
            duration_m = ((end - start).total_seconds() / 60.0) if (start and end) else 0.0
            rl = det["rate_limiter"]
            results.append({
                "log": lf.name,
                "duration_m": duration_m,
                "products": len(det["distinct_products"]),
                "failed_products": len(det["failed_products"]),
                "landing_checks": det["landing_gate"]["checks"],
                "ch_true": det["landing_gate"]["changed_true"],
                "ch_false": det["landing_gate"]["changed_false"],
                "pause_waves": rl["pause_waves"],
                "pause_sec": rl["total_pause_seconds"],
                "ep_429s": sum(det["endpoints_429"].values()),
                "ep_fails": sum(det["endpoints_failed"].values()),
            })

    print("=" * 125)
    print("DETAILS INGESTION COMPARATIVE AUDIT ACROSS ALL LOGS")
    print("=" * 125)
    header = f"{'Log File':<22} | {'Time':>7} | {'Products':>8} | {'Failed':>6} | {'Landing':>8} | {'Ch=T':>6} | {'Ch=F':>6} | {'429 Waves':>9} | {'Paused':>7} | {'429 Retries':>11} | {'Ep Fails':>8}"
    print(header)
    print("-" * 125)
    for r in results:
        print(
            f"{r['log']:<22} | "
            f"{r['duration_m']:>6.1f}m | "
            f"{r['products']:>8} | "
            f"{r['failed_products']:>6} | "
            f"{r['landing_checks']:>8} | "
            f"{r['ch_true']:>6} | "
            f"{r['ch_false']:>6} | "
            f"{r['pause_waves']:>9} | "
            f"{r['pause_sec']:>6.0f}s | "
            f"{r['ep_429s']:>11} | "
            f"{r['ep_fails']:>8}"
        )
    print("=" * 125)


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze ingestion log files.")
    parser.add_argument("path", nargs="?", default=None, help="Path to log file or directory (default: latest log)")
    parser.add_argument("--all", action="store_true", help="Compare all logs in data/logs directory")
    args = parser.parse_args()

    default_dir = Path(__file__).resolve().parent.parent / "data" / "logs"

    if args.all or (args.path and Path(args.path).is_dir()):
        target_dir = Path(args.path) if (args.path and Path(args.path).is_dir()) else default_dir
        compare_all(target_dir)
        return

    if args.path:
        target_path = Path(args.path)
    else:
        # Pick the most recent non-empty log file
        log_files = sorted(default_dir.glob("*.log"), key=lambda p: p.stat().st_mtime, reverse=True)
        target_path = log_files[0] if log_files else default_dir / "20261003_160940.log"

    data = parse_log(target_path)
    print_report(target_path, data)


if __name__ == "__main__":
    main()
