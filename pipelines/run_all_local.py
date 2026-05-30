import subprocess
import sys
from pathlib import Path


PIPELINE_DIR = Path(__file__).resolve().parent

STEPS = [
    ["ingest_nfz_to_rds_local.py"],
    ["transform_rds_to_dw_local.py", "11"],
    ["publish_dw_to_ads_local.py", "11"],
]


def run_step(step: list[str]):
    script_name = step[0]
    args = step[1:]

    script_path = PIPELINE_DIR / script_name

    print(f"\n=== Running: {script_name} {' '.join(args)} ===")

    result = subprocess.run(
        [sys.executable, str(script_path), *args],
        cwd=PIPELINE_DIR.parent,
    )

    if result.returncode != 0:
        raise RuntimeError(f"Pipeline failed on step: {script_name}")

    print(f"=== Finished: {script_name} ===")


def main():
    print("Starting local pipeline...")

    for step in STEPS:
        run_step(step)

    print("\nAll local pipeline steps finished successfully.")


if __name__ == "__main__":
    main()