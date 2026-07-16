"""
mantrac_pipeline_dag.py

Daily predictive-maintenance pipeline:

    consume_telemetry_stream        (Spark Structured Streaming, trigger=availableNow -
                                      processes whatever's queued in Kafka since last run,
                                      then exits; see telemetry_stream_consumer.py docstring
                                      for why this isn't a perpetually-running job)
                |
    score_predictive_maintenance    (Spark, submitted to spark-master:7077)
                |
    merge_signals_output            (flatten Spark's part-files -> ml_maintenance_signals.csv)
                |
    apply_triage_rules              (auto-suppress duplicates, SLA escalation)
                |
    resolve_parts_demand            (component -> model-specific parts, log catalog gaps)
                |
    convert_signals_to_workorders   (human-in-the-loop simulation -> fact_maintenance_events)

The Kafka-to-warehouse gap noted in earlier versions of this DAG is now
closed: consume_telemetry_stream lands new fault events and daily telemetry
into fact_fault_events / fact_telemetry_daily before scoring runs, so this
DAG genuinely processes new data each day rather than re-scoring the same
historical window.

NOTE: telemetry_stream_consumer.py's Kafka READ path has not been validated
against a real running Kafka broker (this was built/tested in a sandbox with
no network access to install one) - only its Postgres upsert logic has been
verified directly. Confirm the Kafka connectivity works end-to-end the first
time this runs against the real docker-compose stack.

All tasks currently run as scripts against local CSV files mounted into the
containers (/opt/sql, /opt/kafka_data) - the same files the seed/transaction
generators produced. Swapping these for direct Postgres reads/writes (via the
AIRFLOW_CONN_MANTRAC_DW connection already registered in docker-compose.yml)
is a reasonable follow-up once the warehouse is the single source of truth
instead of CSVs.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "kay",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="mantrac_predictive_maintenance_pipeline",
    description="Daily telemetry ingestion -> predictive scoring -> triage -> parts demand -> work orders",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=["mantrac", "predictive-maintenance"],
) as dag:

    consume_telemetry_stream = SparkSubmitOperator(
        task_id="consume_telemetry_stream",
        application="/opt/spark_jobs/telemetry_stream_consumer.py",
        conn_id="spark_default",
        packages="org.postgresql:postgresql:42.7.3",
        application_args=[
            "--kafka-bootstrap", "kafka:29092",
            "--jdbc-url", "jdbc:postgresql://postgres-dw:5432/{{ var.value.get('dw_db_name', 'mantrac_dw') }}",
            "--db-user", "{{ conn.mantrac_dw.login }}",
            "--db-password", "{{ conn.mantrac_dw.password }}",
            "--checkpoint-dir", "/opt/spark_checkpoints",
        ],
        conf={"spark.master": "spark://spark-master:7077"},
        verbose=False,
    )

    score_predictive_maintenance = SparkSubmitOperator(
        task_id="score_predictive_maintenance",
        application="/opt/spark_jobs/predictive_maintenance_scoring.py",
        conn_id="spark_default",
        application_args=[
            "--sql-dir", "/opt/sql",
            "--kafka-data-dir", "/opt/kafka_data",
            "--out-dir", "/opt/sql/transactions",
        ],
        conf={"spark.master": "spark://spark-master:7077"},
        verbose=False,
    )

    merge_signals_output = BashOperator(
        task_id="merge_signals_output",
        bash_command=(
            "python /opt/spark_jobs/merge_signals_output.py "
            "--raw-dir /opt/sql/transactions/ml_maintenance_signals_raw "
            "--out-file /opt/sql/transactions/ml_maintenance_signals.csv"
        ),
    )

    apply_triage_rules = BashOperator(
        task_id="apply_triage_rules",
        bash_command="python /opt/pipeline/apply_triage_rules.py --sql-dir /opt/sql",
    )

    resolve_parts_demand = BashOperator(
        task_id="resolve_parts_demand",
        bash_command="python /opt/pipeline/resolve_parts_demand.py --sql-dir /opt/sql",
    )

    convert_signals_to_workorders = BashOperator(
        task_id="convert_signals_to_workorders",
        bash_command="python /opt/pipeline/convert_signals_to_workorders.py --sql-dir /opt/sql",
    )

    (
        consume_telemetry_stream
        >> score_predictive_maintenance
        >> merge_signals_output
        >> apply_triage_rules
        >> resolve_parts_demand
        >> convert_signals_to_workorders
    )
