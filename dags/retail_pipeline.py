"""
Lab 12 - Data Pipeline Automation
CSV -> DuckDB -> dbt -> Tests -> Report -> CSV Export
"""

from __future__ import annotations

import os
import sys
import csv
from datetime import timedelta
from pathlib import Path

import pendulum
import duckdb

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task

LAB_HOME = Path(os.environ.get("LAB_HOME", Path(__file__).resolve().parents[1]))
RAW_DIR = LAB_HOME / "data" / "raw"
DBT_DIR = LAB_HOME / "dbt"
DB_PATH = LAB_HOME / "warehouse" / "retail.duckdb"
EXPORT_DIR = LAB_HOME / "data" / "export"

sys.path.insert(0, str(LAB_HOME / "scripts"))

default_args = {
    "owner": "student",
    "retries": 1,
    "retry_delay": timedelta(seconds=20),
}


@dag(
    dag_id="retail_pipeline",
    description="CSV -> DuckDB -> dbt -> tests -> report -> CSV export",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Jakarta"),
    catchup=False,
    default_args=default_args,
    tags=["lab12", "retail"],
)
def retail_pipeline():

    @task
    def check_source_files() -> list[str]:
        files = sorted(p.name for p in RAW_DIR.glob("*.csv"))
        print(f"Found in {RAW_DIR}: {files}")

        if "customers.csv" not in files or "products.csv" not in files:
            raise FileNotFoundError("customers.csv / products.csv not found")

        if not any(f.startswith("sales_") for f in files):
            raise FileNotFoundError("no sales_*.csv file found")

        return files

    @task
    def extract_load(files: list[str]) -> dict[str, int]:
        from extract_load import extract_load as run_extract_load

        print(f"Loading {len(files)} files")
        return run_extract_load(RAW_DIR, DB_PATH)

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="dbt run --profiles-dir . --no-use-colors",
        cwd=str(DBT_DIR),
        env={"DUCKDB_PATH": str(DB_PATH)},
        append_env=True,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="dbt test --profiles-dir . --no-use-colors",
        cwd=str(DBT_DIR),
        env={"DUCKDB_PATH": str(DB_PATH)},
        append_env=True,
        retries=0,
    )

    @task
    def report() -> None:
        con = duckdb.connect(str(DB_PATH), read_only=True)
        try:
            result = con.execute("""
                SELECT ym, category, SUM(revenue) AS revenue
                FROM dw.mart_monthly_revenue
                GROUP BY ym, category
                ORDER BY ym, revenue DESC
            """).fetchall()

            for row in result:
                print(row)
        finally:
            con.close()

    @task
    def export_monthly_revenue() -> None:
        EXPORT_DIR.mkdir(parents=True, exist_ok=True)

        con = duckdb.connect(str(DB_PATH), read_only=True)

        try:
            cursor = con.execute("""
                SELECT *
                FROM dw.mart_monthly_revenue
                ORDER BY ym, revenue DESC
            """)

            columns = [column[0] for column in cursor.description]
            rows = cursor.fetchall()

            output_file = EXPORT_DIR / "monthly_revenue.csv"

            with open(output_file, "w", newline="", encoding="utf-8") as file:
                writer = csv.writer(file)
                writer.writerow(columns)
                writer.writerows(rows)

            print(f"Exported {len(rows)} rows to {output_file}")

        finally:
            con.close()

    # Membuat task dan mengatur urutan pipeline
    loaded = extract_load(check_source_files())
    report_task = report()
    export_task = export_monthly_revenue()

    loaded >> dbt_run >> dbt_test >> report_task >> export_task


retail_pipeline()
