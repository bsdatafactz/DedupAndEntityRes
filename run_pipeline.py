"""Run the whole pipeline in order, stopping at the first failure.

Usage (from anywhere, inside the activated venv):
    python run_pipeline.py            # all steps, including Postgres
    python run_pipeline.py --no-db    # skip the Postgres steps

Every step is a full refresh, so running this twice gives the same result.
"""
import shutil
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PY = sys.executable
DB = "meridian"

# (name, command, needs_postgres)
STEPS = [
    ("01 generate data", [PY, "src/01_generate_data.py"], False),
    ("02 bronze", [PY, "src/02_bronze.py"], False),
    ("03 silver: standardize", [PY, "src/03_silver.py"], False),
    ("04 silver: match", [PY, "src/04_silver_match.py"], False),
    ("05 gold", [PY, "src/05_gold.py"], False),
    ("06 evaluate", [PY, "src/06_evaluate.py"], False),
    ("db: reset schemas", ["psql", DB, "-v", "ON_ERROR_STOP=1", "-f", "sql/00_reset.sql"], True),
    ("07 publish to postgres + csv", [PY, "src/07_publish.py"], True),
    ("db: add keys", ["psql", DB, "-v", "ON_ERROR_STOP=1", "-f", "sql/01_keys.sql"], True),
]


def main():
    skip_db = "--no-db" in sys.argv
    steps = [s for s in STEPS if not (skip_db and s[2])]

    if any(s[2] for s in steps) and shutil.which("psql") is None:
        sys.exit("psql not found on PATH. Install Postgres, or run with --no-db.")

    timings = []
    for name, cmd, _ in steps:
        print(f"\n=== {name} ===", flush=True)
        start = time.time()
        result = subprocess.run(cmd, cwd=ROOT)
        seconds = time.time() - start
        timings.append((name, seconds))
        if result.returncode != 0:
            sys.exit(f"\nFAILED at step '{name}' (exit code {result.returncode}). Pipeline stopped.")

    print("\n=== pipeline finished ===")
    for name, seconds in timings:
        print(f"{seconds:6.1f}s  {name}")
    print(f"{sum(s for _, s in timings):6.1f}s  total")


if __name__ == "__main__":
    main()
