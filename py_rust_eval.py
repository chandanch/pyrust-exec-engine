import argparse
import csv
import json
import resource
import statistics
import subprocess
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path


def python_csv_to_json_file(csv_path: Path, json_path: Path) -> None:
    with csv_path.open("r", newline="", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)
        rows = list(reader)

    with json_path.open("w", encoding="utf-8") as json_file:
        json.dump(rows, json_file, indent=2)


def peak_rss_megabytes() -> float:
    max_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == "darwin":
        return max_rss / (1024 * 1024)
    return max_rss / 1024


def run_worker(mode: str, csv_path: Path, json_path: Path) -> dict:
    if mode == "python":
        converter = lambda: python_csv_to_json_file(csv_path, json_path)
    elif mode == "rust":
        try:
            import pyrust_exec_engine
        except ModuleNotFoundError as error:
            raise ModuleNotFoundError(
                "Rust extension module 'pyrust_exec_engine' is not available. "
                "Run 'maturin develop' in the current Python environment first."
            ) from error

        converter = lambda: pyrust_exec_engine.csv_to_json_file(
            str(csv_path), str(json_path)
        )
    else:
        raise ValueError(f"Unsupported mode: {mode}")

    start = time.perf_counter()
    converter()
    elapsed = time.perf_counter() - start
    return {
        "mode": mode,
        "duration_seconds": elapsed,
        "peak_rss_mb": peak_rss_megabytes(),
        "output_json": str(json_path),
        "output_size_bytes": json_path.stat().st_size,
    }


def run_single_benchmark(
    mode: str, csv_path: Path, json_path: Path, script_path: Path
) -> dict:
    command = [
        sys.executable,
        str(script_path),
        "--worker",
        mode,
        "--csv",
        str(csv_path),
        "--json",
        str(json_path),
    ]
    completed = subprocess.run(command, capture_output=True, text=True)
    if completed.returncode != 0:
        stderr = completed.stderr.strip()
        stdout = completed.stdout.strip()
        diagnostics = "\n".join(filter(None, [stdout, stderr]))
        raise RuntimeError(
            f"Benchmark worker failed for mode={mode} with exit code "
            f"{completed.returncode}.\n{diagnostics}"
        )
    output = completed.stdout.strip().splitlines()
    if not output:
        raise RuntimeError(f"No output received from benchmark worker: {mode}")
    return json.loads(output[-1])


def summarize(mode: str, samples: list[dict]) -> dict:
    durations = [sample["duration_seconds"] for sample in samples]
    peaks = [sample["peak_rss_mb"] for sample in samples]
    return {
        "mode": mode,
        "runs": len(samples),
        "duration_mean_s": statistics.mean(durations),
        "duration_median_s": statistics.median(durations),
        "duration_min_s": min(durations),
        "duration_max_s": max(durations),
        "peak_rss_mean_mb": statistics.mean(peaks),
        "peak_rss_median_mb": statistics.median(peaks),
        "peak_rss_max_mb": max(peaks),
        "output_size_bytes": samples[-1]["output_size_bytes"],
    }


def detect_winners(summaries: dict[str, dict]) -> dict[str, str]:
    rust_time = summaries["rust"]["duration_mean_s"]
    python_time = summaries["python"]["duration_mean_s"]
    rust_mem = summaries["rust"]["peak_rss_mean_mb"]
    python_mem = summaries["python"]["peak_rss_mean_mb"]

    if rust_time < python_time:
        time_winner = "Rust"
    elif python_time < rust_time:
        time_winner = "Python"
    else:
        time_winner = "Tie"

    if rust_mem < python_mem:
        memory_winner = "Rust"
    elif python_mem < rust_mem:
        memory_winner = "Python"
    else:
        memory_winner = "Tie"

    max_time = max(rust_time, python_time) or 1.0
    max_mem = max(rust_mem, python_mem) or 1.0
    rust_score = 0.7 * (rust_time / max_time) + 0.3 * (rust_mem / max_mem)
    python_score = 0.7 * (python_time / max_time) + 0.3 * (python_mem / max_mem)

    if abs(rust_score - python_score) < 1e-12:
        overall_winner = "Tie"
    elif rust_score < python_score:
        overall_winner = "Rust"
    else:
        overall_winner = "Python"

    return {
        "time_winner": time_winner,
        "memory_winner": memory_winner,
        "overall_winner": overall_winner,
    }


def count_csv_rows(csv_path: Path) -> int:
    with csv_path.open("r", newline="", encoding="utf-8") as csv_file:
        reader = csv.reader(csv_file)
        next(reader, None)
        return sum(1 for _ in reader)


def generate_sales_csv(csv_path: Path, row_count: int) -> None:
    headers = [
        "order_id",
        "order_date",
        "region",
        "salesperson",
        "product",
        "units_sold",
        "unit_price",
        "total_amount",
        "payment_method",
    ]
    regions = ["North", "South", "East", "West"]
    salespeople = [
        "Alice Johnson",
        "Bob Smith",
        "Carla Reyes",
        "David Kim",
        "Emma Davis",
        "Frank Moore",
    ]
    products = [
        ("Laptop", 899.99),
        ("Monitor", 249.50),
        ("Keyboard", 45.00),
        ("Mouse", 19.99),
        ("Printer", 179.75),
        ("Desk Chair", 129.99),
    ]
    payments = ["Credit Card", "Debit Card", "UPI", "Cash", "Bank Transfer"]
    start_date = date(2026, 1, 1)

    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(headers)

        for idx in range(row_count):
            order_id = 1001 + idx
            order_date = (start_date + timedelta(days=idx % 365)).isoformat()
            region = regions[idx % len(regions)]
            salesperson = salespeople[idx % len(salespeople)]
            product_name, unit_price = products[idx % len(products)]
            units_sold = (idx % 20) + 1
            total_amount = units_sold * unit_price
            payment_method = payments[idx % len(payments)]

            writer.writerow(
                [
                    order_id,
                    order_date,
                    region,
                    salesperson,
                    product_name,
                    units_sold,
                    f"{unit_price:.2f}",
                    f"{total_amount:.2f}",
                    payment_method,
                ]
            )


def render_html(
    csv_path: Path,
    row_count: int,
    iterations: int,
    summaries: dict[str, dict],
    winners: dict[str, str],
    details: dict[str, list[dict]],
    generated_at: str,
) -> str:
    rust_mean = summaries["rust"]["duration_mean_s"]
    python_mean = summaries["python"]["duration_mean_s"]
    speedup = python_mean / rust_mean if rust_mean > 0 else float("inf")

    rust_mem = summaries["rust"]["peak_rss_mean_mb"]
    python_mem = summaries["python"]["peak_rss_mean_mb"]
    memory_ratio = python_mem / rust_mem if rust_mem > 0 else float("inf")

    max_time = max(rust_mean, python_mean)
    max_mem = max(rust_mem, python_mem)
    rust_time_bar = (rust_mean / max_time * 100) if max_time else 0
    python_time_bar = (python_mean / max_time * 100) if max_time else 0
    rust_mem_bar = (rust_mem / max_mem * 100) if max_mem else 0
    python_mem_bar = (python_mem / max_mem * 100) if max_mem else 0

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>CSV to JSON Benchmark Report</title>
  <style>
    :root {{
      --bg: #f6f7fb;
      --card: #ffffff;
      --text: #1f2937;
      --muted: #6b7280;
      --rust: #d97706;
      --python: #2563eb;
      --ok: #047857;
      --border: #e5e7eb;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
      background: radial-gradient(circle at top right, #e0ecff, var(--bg) 30%);
      color: var(--text);
      padding: 24px;
    }}
    .wrap {{ max-width: 980px; margin: 0 auto; }}
    .card {{
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 14px;
      padding: 18px;
      box-shadow: 0 12px 30px rgba(15, 23, 42, 0.06);
      margin-bottom: 16px;
    }}
    h1 {{ margin-top: 0; margin-bottom: 10px; font-size: 28px; }}
    h2 {{ margin-top: 0; font-size: 18px; }}
    p {{ margin: 6px 0; color: var(--muted); }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 14px;
    }}
    th, td {{
      padding: 10px;
      border-bottom: 1px solid var(--border);
      text-align: left;
    }}
    th {{ color: #111827; background: #f9fafb; }}
    .metric {{
      margin: 10px 0 14px;
      display: grid;
      gap: 8px;
    }}
    .bar {{
      height: 12px;
      background: #eef2ff;
      border-radius: 999px;
      overflow: hidden;
    }}
    .fill {{ height: 100%; }}
    .rust {{ background: var(--rust); width: {rust_time_bar:.2f}%; }}
    .python {{ background: var(--python); width: {python_time_bar:.2f}%; }}
    .rust-mem {{ background: var(--rust); width: {rust_mem_bar:.2f}%; }}
    .python-mem {{ background: var(--python); width: {python_mem_bar:.2f}%; }}
    .kpi {{
      display: inline-block;
      padding: 8px 10px;
      border-radius: 10px;
      background: #ecfdf5;
      color: var(--ok);
      font-weight: 600;
      margin-right: 8px;
      margin-bottom: 6px;
    }}
    code {{
      background: #f3f4f6;
      border-radius: 4px;
      padding: 2px 6px;
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="card">
      <h1>CSV to JSON Performance Report</h1>
      <p>Generated: {generated_at}</p>
      <p>Source CSV: <code>{csv_path}</code></p>
      <p>Rows in CSV: {row_count}</p>
      <p>Runs per implementation: {iterations}</p>
      <div class="kpi">Speedup (Python / Rust): {speedup:.2f}x</div>
      <div class="kpi">Memory ratio (Python / Rust): {memory_ratio:.2f}x</div>
      <div class="kpi">Time winner: {winners["time_winner"]}</div>
      <div class="kpi">Memory winner: {winners["memory_winner"]}</div>
      <div class="kpi">Overall winner: {winners["overall_winner"]}</div>
    </section>

    <section class="card">
      <h2>Summary</h2>
      <table>
        <thead>
          <tr>
            <th>Implementation</th>
            <th>Mean time (s)</th>
            <th>Median time (s)</th>
            <th>Min time (s)</th>
            <th>Max time (s)</th>
            <th>Mean peak RSS (MB)</th>
            <th>Max peak RSS (MB)</th>
            <th>Output size (bytes)</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td>Rust (via PyO3)</td>
            <td>{summaries["rust"]["duration_mean_s"]:.6f}</td>
            <td>{summaries["rust"]["duration_median_s"]:.6f}</td>
            <td>{summaries["rust"]["duration_min_s"]:.6f}</td>
            <td>{summaries["rust"]["duration_max_s"]:.6f}</td>
            <td>{summaries["rust"]["peak_rss_mean_mb"]:.3f}</td>
            <td>{summaries["rust"]["peak_rss_max_mb"]:.3f}</td>
            <td>{summaries["rust"]["output_size_bytes"]}</td>
          </tr>
          <tr>
            <td>Python</td>
            <td>{summaries["python"]["duration_mean_s"]:.6f}</td>
            <td>{summaries["python"]["duration_median_s"]:.6f}</td>
            <td>{summaries["python"]["duration_min_s"]:.6f}</td>
            <td>{summaries["python"]["duration_max_s"]:.6f}</td>
            <td>{summaries["python"]["peak_rss_mean_mb"]:.3f}</td>
            <td>{summaries["python"]["peak_rss_max_mb"]:.3f}</td>
            <td>{summaries["python"]["output_size_bytes"]}</td>
          </tr>
        </tbody>
      </table>
    </section>

    <section class="card">
      <h2>Mean Time Bars</h2>
      <div class="metric">
        <div>Rust: {rust_mean:.6f}s</div>
        <div class="bar"><div class="fill rust"></div></div>
        <div>Python: {python_mean:.6f}s</div>
        <div class="bar"><div class="fill python"></div></div>
      </div>
      <h2>Mean Peak Memory Bars</h2>
      <div class="metric">
        <div>Rust: {rust_mem:.3f}MB</div>
        <div class="bar"><div class="fill rust-mem"></div></div>
        <div>Python: {python_mem:.3f}MB</div>
        <div class="bar"><div class="fill python-mem"></div></div>
      </div>
    </section>

    <section class="card">
      <h2>Per-Run Detail</h2>
      <table>
        <thead>
          <tr>
            <th>Run</th>
            <th>Rust time (s)</th>
            <th>Rust peak RSS (MB)</th>
            <th>Python time (s)</th>
            <th>Python peak RSS (MB)</th>
          </tr>
        </thead>
        <tbody>
          {"".join(
              f"<tr><td>{index + 1}</td>"
              f"<td>{details['rust'][index]['duration_seconds']:.6f}</td>"
              f"<td>{details['rust'][index]['peak_rss_mb']:.3f}</td>"
              f"<td>{details['python'][index]['duration_seconds']:.6f}</td>"
              f"<td>{details['python'][index]['peak_rss_mb']:.3f}</td></tr>"
              for index in range(iterations)
          )}
        </tbody>
      </table>
    </section>
  </div>
</body>
</html>
"""


def run_report(csv_path: Path, report_path: Path, iterations: int) -> None:
    script_path = Path(__file__).resolve()
    base_dir = script_path.parent
    data_dir = base_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    details = {"rust": [], "python": []}
    for mode in ("rust", "python"):
        for index in range(iterations):
            output_path = data_dir / f"{csv_path.stem}.{mode}.run{index + 1}.json"
            details[mode].append(
                run_single_benchmark(mode, csv_path, output_path, script_path)
            )

    summaries = {
        "rust": summarize("rust", details["rust"]),
        "python": summarize("python", details["python"]),
    }
    winners = detect_winners(summaries)
    row_count = count_csv_rows(csv_path)

    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    report_path.parent.mkdir(parents=True, exist_ok=True)

    html = render_html(
        csv_path, row_count, iterations, summaries, winners, details, generated_at
    )
    report_path.write_text(html, encoding="utf-8")

    metrics_path = report_path.with_suffix(".json")
    metrics_path.write_text(
        json.dumps(
            {
                "generated_at": generated_at,
                "csv_path": str(csv_path),
                "csv_row_count": row_count,
                "iterations": iterations,
                "summaries": summaries,
                "winners": winners,
                "details": details,
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    print(f"HTML report written to: {report_path}")
    print(f"Raw metrics written to: {metrics_path}")


def parse_args() -> argparse.Namespace:
    base_dir = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(
        description="Benchmark CSV -> JSON conversion in Rust and Python."
    )
    parser.add_argument(
        "--csv",
        default=str(base_dir / "data" / "dummy_sales.csv"),
        help="Path to input CSV file.",
    )
    parser.add_argument(
        "--report",
        default=str(base_dir / "reports" / "csv_json_performance_report.html"),
        help="Path to output HTML report file.",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=5,
        help="Number of subprocess runs per implementation.",
    )
    parser.add_argument(
        "--generate-rows",
        type=int,
        default=0,
        help="Generate a synthetic CSV with this many rows before benchmarking.",
    )
    parser.add_argument(
        "--generated-csv",
        default=str(base_dir / "data" / "dummy_sales_500.csv"),
        help="Path for generated CSV when --generate-rows is used.",
    )
    parser.add_argument(
        "--worker",
        choices=["rust", "python"],
        help="Internal mode used for isolated benchmarking.",
    )
    parser.add_argument(
        "--json",
        help="Output JSON file for worker mode.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    csv_path = Path(args.csv).resolve()

    if args.generate_rows < 0:
        raise ValueError("--generate-rows must be >= 0")

    if args.worker:
        if not args.json:
            raise ValueError("--json is required in worker mode")
        result = run_worker(args.worker, csv_path, Path(args.json).resolve())
        print(json.dumps(result))
        return

    if args.iterations < 1:
        raise ValueError("--iterations must be >= 1")

    if args.generate_rows > 0:
        csv_path = Path(args.generated_csv).resolve()
        generate_sales_csv(csv_path, args.generate_rows)
        print(f"Generated CSV with {args.generate_rows} rows at: {csv_path}")

    run_report(csv_path, Path(args.report).resolve(), args.iterations)


if __name__ == "__main__":
    main()
