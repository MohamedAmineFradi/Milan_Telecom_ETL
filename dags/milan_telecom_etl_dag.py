from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.models.baseoperator import chain
from airflow.operators.python import PythonOperator

from src.database import create_database, create_schema
from src.etl import (
    get_top_cells,
    load_grid_geometries,
    load_mobility_data,
    load_provinces_geometries,
    load_traffic_data,
)
from sqlalchemy import text


def data_quality_checks_task() -> None:
    from src.database import get_sqlalchemy_engine

    engine = get_sqlalchemy_engine()
    failures = []

    critical_counts = {
        "dim_grid_milan": 1,
        "dim_provinces_it": 1,
        "fact_traffic_milan": 1,
        "fact_mobility_provinces": 1,
    }

    with engine.begin() as conn:
        for table_name, min_expected in critical_counts.items():
            count = conn.execute(
                text(f"SELECT COUNT(*) FROM {table_name}")
            ).scalar_one()
            if int(count) < min_expected:
                failures.append(
                    f"Table {table_name} has {count} rows, expected >= {min_expected}"
                )

        negative_checks = {
            "fact_traffic_milan": ["smsin", "smsout", "callin", "callout", "internet"],
            "fact_mobility_provinces": ["cell2province", "province2cell"],
        }

        for table_name, cols in negative_checks.items():
            for col in cols:
                violations = conn.execute(
                    text(f"SELECT COUNT(*) FROM {table_name} WHERE {col} < 0")
                ).scalar_one()
                if int(violations) > 0:
                    failures.append(
                        f"{table_name}.{col} has {violations} negative values"
                    )

    if failures:
        message = "Data quality checks failed: " + " | ".join(failures)
        raise AirflowFailException(message)

    print("All critical data quality checks passed")


def setup_database_task() -> None:
    create_database()
    create_schema()


def validate_top_cells_task() -> None:
    df = get_top_cells(limit=10)
    if df.empty:
        print("No traffic rows available yet")
        return
    print("Top 10 cells by average activity:")
    print(df.to_string(index=False))


default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="milan_telecom_etl",
    description="Orchestrates Milan Telecom ETL pipeline",
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["etl", "milan", "telecom"],
) as dag:
    setup_db = PythonOperator(
        task_id="setup_database",
        python_callable=setup_database_task,
    )

    load_grid = PythonOperator(
        task_id="load_grid_geometries",
        python_callable=load_grid_geometries,
    )

    load_provinces = PythonOperator(
        task_id="load_provinces_geometries",
        python_callable=load_provinces_geometries,
    )

    load_traffic = PythonOperator(
        task_id="load_traffic_data",
        python_callable=load_traffic_data,
        op_kwargs={"limit_files": None},
    )

    load_mobility = PythonOperator(
        task_id="load_mobility_data",
        python_callable=load_mobility_data,
        op_kwargs={"limit_files": None},
    )

    validate_top_cells = PythonOperator(
        task_id="validate_top_cells",
        python_callable=validate_top_cells_task,
    )

    data_quality_checks = PythonOperator(
        task_id="data_quality_checks",
        python_callable=data_quality_checks_task,
    )

    setup_db >> [load_grid, load_provinces]
    chain(
        [load_grid, load_provinces],
        [load_traffic, load_mobility],
        data_quality_checks,
        validate_top_cells,
    )
