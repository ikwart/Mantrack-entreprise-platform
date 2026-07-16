"""
telemetry_stream_consumer.py

Closes the gap flagged in the Airflow DAG: consumes the two Kafka topics
telemetry_kafka_producer.py publishes to, and lands them into the warehouse:

    equipment.fault_events    -> fact_fault_events  (append)
    equipment.telemetry_daily -> fact_telemetry_daily (upsert)

DESIGN CHOICE - batch-triggered streaming, not a perpetual service:
    This uses Spark Structured Streaming (not a plain batch job) because
    Kafka is the source and Structured Streaming is the correct tool for
    consuming a Kafka topic with proper offset tracking (checkpointing) so
    re-runs don't reprocess or drop messages. BUT it runs with
    `.trigger(availableNow=True)` - process everything currently queued,
    then STOP - rather than running forever. That's deliberate: this
    platform's orchestration model is a daily Airflow DAG, not a fleet of
    always-on streaming services. A perpetually-running Spark job doesn't
    fit a SparkSubmitOperator task that Airflow expects to complete. If a
    genuinely real-time use case emerges later (e.g. an alerting dashboard
    that needs sub-minute latency), THAT would justify a real always-on
    streaming deployment - this daily-batch pattern is the right choice for
    what this platform actually needs today.

UPSERT VIA foreachBatch:
    Spark has no native Postgres streaming sink. The standard, correct
    pattern is `foreachBatch`: for each micro-batch, write to a JDBC staging
    table, then run a single INSERT ... ON CONFLICT upsert from staging into
    the real fact table via a direct psycopg2 connection. fact_fault_events
    needs no upsert (every message is a distinct new event - plain append);
    fact_telemetry_daily DOES need upsert, since its primary key is
    (equipment_id, date_id) and a retried/re-run micro-batch publishing the
    same day again should update, not fail on a PK violation.

KNOWN LIMITATION (see docs/architecture.md and the DAG docstring): this has
been validated end-to-end against the foreachBatch/upsert write path using a
manually constructed DataFrame standing in for a Kafka micro-batch (proving
the Postgres side is correct), but NOT against a real running Kafka broker -
this sandbox has no network access to install one. The Kafka READ side
should be verified against the real docker-compose stack before relying on
it in production.

Usage:
  spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
      telemetry_stream_consumer.py \
      --kafka-bootstrap kafka:29092 \
      --jdbc-url jdbc:postgresql://postgres-dw:5432/mantrac_dw \
      --db-user mantrac_admin --db-password <pw> \
      --checkpoint-dir /opt/spark_checkpoints
"""

import argparse

import psycopg2
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

FAULT_EVENT_SCHEMA = StructType([
    StructField("equipment_id", IntegerType()),
    StructField("event_date", StringType()),
    StructField("fault_code", StringType()),
    StructField("severity", StringType()),
])

DAILY_AGG_SCHEMA = StructType([
    StructField("equipment_id", IntegerType()),
    StructField("date_id", StringType()),
    StructField("utilization_hours", DoubleType()),
    StructField("fault_count_info", IntegerType()),
    StructField("fault_count_warning", IntegerType()),
    StructField("fault_count_critical", IntegerType()),
    StructField("avg_sensor_reading", DoubleType()),
    StructField("max_sensor_reading", DoubleType()),
])


def get_spark():
    return (
        SparkSession.builder
        .appName("MantracTelemetryStreamConsumer")
        .getOrCreate()
    )


def write_fault_events_batch(batch_df, batch_id, jdbc_url, db_props):
    """fact_fault_events: plain append, every message is a genuinely new event."""
    if batch_df.rdd.isEmpty():
        return
    (
        batch_df.select("equipment_id", "event_date", "fault_code", "severity")
        .write.jdbc(url=jdbc_url, table="fact_fault_events", mode="append", properties=db_props)
    )
    print(f"[batch {batch_id}] appended {batch_df.count()} fault events")


def write_daily_agg_batch(batch_df, batch_id, jdbc_url, db_props, pg_conn_str):
    """
    fact_telemetry_daily: UPSERT on (equipment_id, date_id). We don't track
    engine_hours cumulatively here yet (same known gap noted in
    telemetry_kafka_producer.py) - it's left NULL on insert and simply not
    updated on conflict, rather than overwriting a real historical value with
    a wrong one. A follow-up should read the prior day's engine_hours from
    the warehouse before publishing, so this can be computed properly.
    """
    if batch_df.rdd.isEmpty():
        return

    staging_table = f"staging_telemetry_daily_batch_{batch_id}"
    (
        batch_df.select(
            "equipment_id", "date_id", "utilization_hours",
            "fault_count_info", "fault_count_warning", "fault_count_critical",
            "avg_sensor_reading", "max_sensor_reading",
        )
        .write.jdbc(url=jdbc_url, table=staging_table, mode="overwrite", properties=db_props)
    )

    upsert_sql = f"""
        INSERT INTO fact_telemetry_daily
            (equipment_id, date_id, utilization_hours, fault_count_info,
             fault_count_warning, fault_count_critical, avg_sensor_reading, max_sensor_reading)
        SELECT equipment_id, date_id::date, utilization_hours, fault_count_info,
               fault_count_warning, fault_count_critical, avg_sensor_reading, max_sensor_reading
        FROM {staging_table}
        ON CONFLICT (equipment_id, date_id) DO UPDATE SET
            utilization_hours = EXCLUDED.utilization_hours,
            fault_count_info = EXCLUDED.fault_count_info,
            fault_count_warning = EXCLUDED.fault_count_warning,
            fault_count_critical = EXCLUDED.fault_count_critical,
            avg_sensor_reading = EXCLUDED.avg_sensor_reading,
            max_sensor_reading = EXCLUDED.max_sensor_reading;
        DROP TABLE {staging_table};
    """
    conn = psycopg2.connect(pg_conn_str)
    try:
        with conn.cursor() as cur:
            cur.execute(upsert_sql)
        conn.commit()
    finally:
        conn.close()

    print(f"[batch {batch_id}] upserted {batch_df.count()} daily telemetry rows")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--kafka-bootstrap", default="kafka:29092")
    parser.add_argument("--jdbc-url", default="jdbc:postgresql://postgres-dw:5432/mantrac_dw")
    parser.add_argument("--db-user", default="mantrac_admin")
    parser.add_argument("--db-password", required=True)
    parser.add_argument("--checkpoint-dir", default="/opt/spark_checkpoints")
    args = parser.parse_args()

    db_props = {"user": args.db_user, "password": args.db_password, "driver": "org.postgresql.Driver"}
    pg_conn_str = (
        f"dbname={args.jdbc_url.split('/')[-1]} user={args.db_user} "
        f"password={args.db_password} host={args.jdbc_url.split('//')[1].split(':')[0]} "
        f"port={args.jdbc_url.split(':')[-1].split('/')[0]}"
    )

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    fault_events_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.kafka_bootstrap)
        .option("subscribe", "equipment.fault_events")
        .option("startingOffsets", "earliest")
        .load()
    )
    fault_events = (
        fault_events_raw
        .select(F.from_json(F.col("value").cast("string"), FAULT_EVENT_SCHEMA).alias("data"))
        .select("data.*")
    )

    daily_agg_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.kafka_bootstrap)
        .option("subscribe", "equipment.telemetry_daily")
        .option("startingOffsets", "earliest")
        .load()
    )
    daily_agg = (
        daily_agg_raw
        .select(F.from_json(F.col("value").cast("string"), DAILY_AGG_SCHEMA).alias("data"))
        .select("data.*")
    )

    fault_query = (
        fault_events.writeStream
        .foreachBatch(lambda df, bid: write_fault_events_batch(df, bid, args.jdbc_url, db_props))
        .option("checkpointLocation", f"{args.checkpoint_dir}/fault_events")
        .trigger(availableNow=True)
        .start()
    )
    fault_query.awaitTermination()

    daily_query = (
        daily_agg.writeStream
        .foreachBatch(lambda df, bid: write_daily_agg_batch(df, bid, args.jdbc_url, db_props, pg_conn_str))
        .option("checkpointLocation", f"{args.checkpoint_dir}/daily_agg")
        .trigger(availableNow=True)
        .start()
    )
    daily_query.awaitTermination()

    spark.stop()


if __name__ == "__main__":
    main()
