# dags/milan_telecom_etl_production.py
"""
Production ETL Pipeline: Fetch Milan Telecom data from LOCAL SERVER via SSH
→ Transform with existing src/ modules → Load to Azure PostgreSQL

Tags: etl, milan, telecom, ssh, production, azure
Schedule: 0 2 * * * (daily at 2 AM UTC)
"""

from datetime import datetime, timedelta
import os
import logging
from pathlib import Path

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.models.baseoperator import chain
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from sqlalchemy import text

# Imports de votre code métier existant
from src.database import create_database, create_schema, get_sqlalchemy_engine
from src.etl import (
    get_top_cells,
    load_grid_geometries,
    load_mobility_data,
    load_provinces_geometries,
    load_traffic_data,
)

logger = logging.getLogger(__name__)

# ─────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────
# Chemin temporaire pour stocker les données fetchées depuis le serveur local
# Ce chemin doit être accessible par les fonctions load_* de src/etl.py
TEMP_DATA_DIR = Path(os.getenv("TEMP_DATA_DIR", "/tmp/milan-telecom-data"))

# Paramètres SSH pour la connexion au serveur local
SSH_CONN_ID = os.getenv("SSH_CONN_ID", "local_data_server")
REMOTE_DATA_PATH = os.getenv(
    "REMOTE_DATA_PATH", "/srv/telecom-data"
)  # Chemin sur le serveur local

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": lambda context: logger.error(
        f"Pipeline failed: {context['task_instance']}"
    ),
}


# ─────────────────────────────────────
# TASKS
# ─────────────────────────────────────


def fetch_remote_data_task(**context) -> None:
    """
    Fetch CSV files from LOCAL SERVER via SSH/SFTP → Save to TEMP_DATA_DIR
    This task runs BEFORE the existing ETL functions.
    """
    logger.info(f"🔄 Fetching data from {REMOTE_DATA_PATH} via SSH...")

    # Créer le dossier temporaire si nécessaire
    TEMP_DATA_DIR.mkdir(parents=True, exist_ok=True)

    ssh_hook = SSHHook(ssh_conn_id=SSH_CONN_ID)

    try:
        with ssh_hook.get_conn() as ssh_client:
            # Lister les fichiers CSV disponibles
            stdin, stdout, stderr = ssh_client.exec_command(
                f'find "{REMOTE_DATA_PATH}" -name "*.csv" -type f 2>/dev/null'
            )
            remote_files = [f.strip() for f in stdout.readlines() if f.strip()]

            if not remote_files:
                logger.warning(f"⚠️ Aucun fichier CSV trouvé dans {REMOTE_DATA_PATH}")
                return

            logger.info(f"📁 {len(remote_files)} fichier(s) trouvé(s) à distance")

            # Télécharger chaque fichier (avec limite pour les tests)
            downloaded_count = 0
            for remote_path in remote_files[
                :10
            ]:  # Limite à 10 fichiers pour éviter le timeout
                filename = Path(remote_path).name
                local_path = TEMP_DATA_DIR / filename

                # Utiliser SFTP pour le transfert (plus fiable pour les gros fichiers)
                sftp = ssh_client.open_sftp()
                sftp.get(remote_path, str(local_path))
                sftp.close()

                logger.info(
                    f"✅ Downloaded: {filename} ({local_path.stat().st_size / 1024 / 1024:.2f} MB)"
                )
                downloaded_count += 1

            # Stocker le chemin dans XCom pour les tâches suivantes
            context["ti"].xcom_push(key="data_dir", value=str(TEMP_DATA_DIR))
            logger.info(
                f"🎯 {downloaded_count} fichier(s) téléchargé(s) dans {TEMP_DATA_DIR}"
            )

    except Exception as e:
        logger.error(f"❌ Erreur lors du fetch SSH: {e}")
        raise AirflowFailException(f"SSH fetch failed: {e}")


def setup_database_task() -> None:
    """Initialise la base de données Azure PostgreSQL (idempotent)."""
    logger.info("🗄️ Setting up database schema...")
    create_database()
    create_schema()
    logger.info("✅ Database setup complete")


def load_traffic_data_with_path(**context) -> None:
    """
    Wrapper autour de load_traffic_data() pour injecter le chemin des données fetchées.
    """
    ti = context["ti"]
    data_dir = ti.xcom_pull(key="data_dir", task_ids="fetch_remote_data")

    if data_dir:
        logger.info(f"📊 Loading traffic data from {data_dir}")
        # Injecter le chemin dans l'environnement ou passer en paramètre
        # Selon l'implémentation de votre fonction load_traffic_data
        load_traffic_data(data_dir=str(data_dir), limit_files=None)
    else:
        # Fallback: utiliser le chemin par défaut
        logger.warning("⚠️ No data_dir from XCom, using default path")
        load_traffic_data(limit_files=None)


def load_mobility_data_with_path(**context) -> None:
    """Wrapper autour de load_mobility_data() avec injection du chemin."""
    ti = context["ti"]
    data_dir = ti.xcom_pull(key="data_dir", task_ids="fetch_remote_data")

    if data_dir:
        logger.info(f"🚗 Loading mobility data from {data_dir}")
        load_mobility_data(data_dir=str(data_dir), limit_files=None)
    else:
        logger.warning("⚠️ No data_dir from XCom, using default path")
        load_mobility_data(limit_files=None)


def data_quality_checks_task() -> None:
    """Vérifications de qualité des données après chargement."""
    logger.info("🔍 Running data quality checks...")

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
        logger.error(message)
        raise AirflowFailException(message)

    logger.info("✅ All critical data quality checks passed")


def validate_top_cells_task() -> None:
    """Affiche les top cells pour validation visuelle."""
    logger.info("📈 Validating top cells...")
    df = get_top_cells(limit=10)
    if df.empty:
        logger.warning("⚠️ No traffic rows available yet")
        return
    logger.info("Top 10 cells by average activity:\n" + df.to_string(index=False))


# ─────────────────────────────────────
# DAG DEFINITION
# ─────────────────────────────────────

with DAG(
    dag_id="milan_telecom_etl_production",
    description="Production ETL: Fetch from local server via SSH → Transform → Load to Azure PostgreSQL",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),  # Ajustez selon vos besoins
    schedule="0 2 * * *",  # Daily at 2 AM UTC
    catchup=False,
    max_active_runs=1,
    tags=["etl", "milan", "telecom", "ssh", "production", "azure"],
) as dag:

    # 1. Fetch des données depuis le serveur local (NOUVELLE TÂCHE)
    fetch_data = PythonOperator(
        task_id="fetch_remote_data",
        python_callable=fetch_remote_data_task,
    )

    # 2. Setup de la base de données (existant)
    setup_db = PythonOperator(
        task_id="setup_database",
        python_callable=setup_database_task,
    )

    # 3. Chargement des géométries (existant)
    load_grid = PythonOperator(
        task_id="load_grid_geometries",
        python_callable=load_grid_geometries,
    )

    load_provinces = PythonOperator(
        task_id="load_provinces_geometries",
        python_callable=load_provinces_geometries,
    )

    # 4. Chargement des données métier (adapté avec chemin dynamique)
    load_traffic = PythonOperator(
        task_id="load_traffic_data",
        python_callable=load_traffic_data_with_path,
        op_kwargs={"limit_files": None},
    )

    load_mobility = PythonOperator(
        task_id="load_mobility_data",
        python_callable=load_mobility_data_with_path,
        op_kwargs={"limit_files": None},
    )

    # 5. Validation et qualité des données (existant)
    data_quality_checks = PythonOperator(
        task_id="data_quality_checks",
        python_callable=data_quality_checks_task,
    )

    validate_top_cells = PythonOperator(
        task_id="validate_top_cells",
        python_callable=validate_top_cells_task,
    )

    # ─────────────────────────────────────
    # DÉPENDANCES DES TÂCHES
    # ─────────────────────────────────────
    # Fetch d'abord, puis setup DB en parallèle des géométries,
    # puis chargement des données, puis validation
    fetch_data >> setup_db
    fetch_data >> [load_grid, load_provinces]

    setup_db >> [load_traffic, load_mobility]
    [load_grid, load_provinces] >> [load_traffic, load_mobility]

    chain([load_traffic, load_mobility], data_quality_checks, validate_top_cells)
