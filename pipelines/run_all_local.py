import subprocess
import sys
from pathlib import Path
from sqlalchemy import text

sys.path.append(str(Path(__file__).resolve().parents[1]))

from db import get_engine


PIPELINE_DIR = Path(__file__).resolve().parent


def run_script(script_name: str, *args: str):
    script_path = PIPELINE_DIR / script_name

    print(f"\n=== Running: {script_name} {' '.join(args)} ===")

    result = subprocess.run(
        [sys.executable, str(script_path), *args],
        cwd=PIPELINE_DIR.parent,
    )

    if result.returncode != 0:
        raise RuntimeError(f"Pipeline failed on step: {script_name}")

    print(f"=== Finished: {script_name} ===")


def get_latest_success_batch_id() -> int:
    engine = get_engine()

    with engine.connect() as conn:
        batch_id = conn.execute(text("""
            select top 1 load_batch_id
            from rds.load_batch
            where source_system = 'NFZ_TERMINY_LECZENIA'
              and status = 'SUCCESS'
            order by load_batch_id desc
        """)).scalar_one_or_none()

    if batch_id is None:
        raise ValueError("Nie znaleziono żadnego poprawnego batcha w rds.load_batch")

    return int(batch_id)


def main():
    run_script("ingest_nfz_to_rds_local.py")

    load_batch_id = get_latest_success_batch_id()
    print(f"\nUsing load_batch_id = {load_batch_id}")

    run_script("transform_rds_to_dw_local.py", str(load_batch_id))
    run_script("publish_dw_to_ads_local.py", str(load_batch_id))

    print("\nAll local pipeline steps finished successfully.")


if __name__ == "__main__":
    main()