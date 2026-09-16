import re
import sys
from collections import Counter
from pathlib import Path

default_log = (
    Path(__file__).resolve().parent.parent
    / "data"
    / "logs"
    / "20260913_131727.log"
)
log_path = Path(sys.argv[1]) if len(sys.argv) > 1 else default_log

counts = {
    "products": Counter(),
    "contracts": Counter(),
    "gateway": Counter(),
    "prices": Counter(),
}

incremental_bars = 0
refetched_bars = 0
bar_distribution = Counter()

re_inc = re.compile(r"Product (\d+): incremental price update complete \((\d+) bars\)")
re_refetch = re.compile(
    r"Product (\d+): mismatch refetch archived and replaced \((\d+) bars\)"
)
re_price_target = re.compile(r"Price ingestion:\s+(\d+)\s+products to process")
re_qual_target = re.compile(r"Contract qualification:\s+(\d+)\s+products to process")

with open(log_path, "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue

        if "etfportfolio.ingestion.products" in line:
            if "Fetching products page" in line:
                counts["products"]["page_fetches"] += 1
            elif "Received" in line and "products on page" in line:
                counts["products"]["pages_received"] += 1
            elif "Pagination complete" in line:
                counts["products"]["pagination_complete"] += 1

        elif "etfportfolio.ingestion.contracts" in line or "ib_async.wrapper" in line:
            if "Contract qualification:" in line:
                counts["contracts"]["qualification_starts"] += 1
                m = re_qual_target.search(line)
                if m:
                    counts["contracts"]["target_products"] = int(m.group(1))
            elif (
                "No contract details found" in line
                or "No security definition has been found" in line
            ):
                counts["contracts"]["missing_contracts"] += 1

        elif "etfportfolio.ingestion.gateway" in line:
            if "Connected to IB Gateway" in line:
                counts["gateway"]["connected"] += 1
            elif "Disconnected from IB Gateway" in line:
                counts["gateway"]["disconnected"] += 1

        elif "etfportfolio.ingestion.prices" in line:
            if "Price ingestion:" in line:
                counts["prices"]["batch_starts"] += 1
                m = re_price_target.search(line)
                if m:
                    counts["prices"]["target_products"] = int(m.group(1))
            elif "value_mismatch detected" in line:
                counts["prices"]["mismatch_detected"] += 1
            elif "incremental price update complete" in line:
                counts["prices"]["incremental_updates"] += 1
                m = re_inc.search(line)
                if m:
                    bars = int(m.group(2))
                    incremental_bars += bars
                    bar_distribution[bars] += 1
            elif "mismatch refetch archived and replaced" in line:
                counts["prices"]["mismatch_replaced"] += 1
                m = re_refetch.search(line)
                if m:
                    refetched_bars += int(m.group(2))

print(f"Log file: {log_path}\n")

for phase, phase_counts in counts.items():
    if phase == "prices":
        continue
    print(f"[{phase.upper()}]")
    for k, v in phase_counts.items():
        print(f"  {k}: {v}")
    print()

print("[PRICES]")
for k, v in counts["prices"].items():
    print(f"  {k}: {v}")
print(f"  total_incremental_bars: {incremental_bars}")
print(f"  total_refetched_bars: {refetched_bars}")
print("  bars_distribution:")
for bars, freq in sorted(bar_distribution.items()):
    print(f"    {bars} bars: {freq} products")