"""
predictive_maintenance_scoring.py

Spark batch job that scores every (equipment, component) pair for failure
probability and writes ml_maintenance_signals.

IMPORTANT - avoiding data leakage:
  The simulator's internal ground truth (which component a fault code was
  "really" generated for, and the planned failure calendar) is NEVER used as
  a model input. The model only sees what real telemetry would actually
  report: (equipment_id, event_date, fault_code, severity). Component
  attribution is inferred the same way a real system would - via
  bridge_faultcode_component correlation weights - not read off a hidden
  label. The failure calendar is used ONLY to construct training labels
  (this mirrors how a real deployment would use historical failure records
  from fact_maintenance_events, which is exactly what the calendar stands in
  for) and to evaluate the model afterward. It is never a model feature.

Pipeline:
  1. Load fault events, telemetry daily, maintenance history, dimensions
  2. Fan fault events out to candidate components via bridge_faultcode_component
  3. Build a full (equipment, component, date) spine and engineer rolling features
     (7/14/30-day weighted fault scores, critical counts, days since last
     maintenance, machine age)
  4. Label rows using the failure calendar: does a real failure of this
     component happen in the next RECOMMENDED_WINDOW_DAYS days?
  5. Time-based train/test split (never random - avoids leaking the future
     into training)
  6. Train a GBTClassifier, evaluate honestly, print metrics
  7. Score the most recent day for the whole fleet
  8. Apply triage thresholds -> signal_status
  9. Write ml_maintenance_signals.csv (loadable via COPY, same pattern as
     every other fact table in this platform)

Usage:
  spark-submit predictive_maintenance_scoring.py \
      --sql-dir /opt/sql --kafka-data-dir /opt/kafka_data --out-dir /opt/sql/transactions
"""

import argparse

from pyspark.sql import SparkSession, functions as F, Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

MODEL_VERSION = "gbt-v1"
LABEL_WINDOW_DAYS = 21          # "will this component fail in the next N days?"
TEST_HOLDOUT_DAYS = 60          # most recent N days held out as a time-based test set
SEVERITY_WEIGHT = {"Critical": 3.0, "Warning": 1.5, "Info": 1.0}

TRIAGE_WATCH_THRESHOLD = 0.4
TRIAGE_ACTION_THRESHOLD = 0.7


def get_spark():
    return (
        SparkSession.builder
        .appName("MantracPredictiveMaintenanceScoring")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def load_data(spark, sql_dir, kafka_data_dir):
    equipment = spark.read.csv(f"{sql_dir}/seed_data/dim_equipment.csv", header=True, inferSchema=True)
    components = spark.read.csv(f"{sql_dir}/seed_data/dim_component.csv", header=True, inferSchema=True)
    bridge = spark.read.csv(f"{sql_dir}/seed_data/bridge_faultcode_component.csv", header=True, inferSchema=True)
    telemetry_daily = spark.read.csv(f"{sql_dir}/transactions/fact_telemetry_daily.csv", header=True, inferSchema=True)
    maintenance = spark.read.csv(f"{sql_dir}/transactions/fact_maintenance_events.csv", header=True, inferSchema=True)
    failure_calendar = spark.read.csv(f"{kafka_data_dir}/failure_calendar.csv", header=True, inferSchema=True)

    # Fault events: intentionally select ONLY the columns real telemetry would
    # report. Even though telemetry_fault_events.csv also carries a
    # simulation-internal component_id column (kept for our own debugging),
    # we deliberately never read it here.
    fault_events = (
        spark.read.csv(f"{kafka_data_dir}/telemetry_fault_events.csv", header=True, inferSchema=True)
        .select("equipment_id", "event_date", "fault_code", "severity")
    )

    return equipment, components, bridge, telemetry_daily, maintenance, failure_calendar, fault_events


def fan_out_faults_to_components(fault_events, bridge):
    """Each fault code maps to 1+ candidate components, weighted by correlation.
    A single fault event becomes multiple weighted (equipment, component, date) rows."""
    severity_map = F.create_map(*[F.lit(x) for pair in SEVERITY_WEIGHT.items() for x in pair])

    fanned = (
        fault_events.join(bridge, on="fault_code", how="inner")
        .withColumn("severity_weight", severity_map[F.col("severity")])
        .withColumn("weighted_score", F.col("correlation_weight") * F.col("severity_weight"))
        .withColumn("is_critical", (F.col("severity") == "Critical").cast("int"))
    )
    return (
        fanned.groupBy("equipment_id", "component_id", F.col("event_date").alias("date_id"))
        .agg(
            F.sum("weighted_score").alias("daily_weighted_score"),
            F.sum("is_critical").alias("daily_critical_count"),
            F.count("*").alias("daily_fault_count"),
        )
    )


def build_spine(spark, equipment, components, telemetry_daily):
    """Full (equipment, component, date) grid so rolling windows have zero-filled days, not gaps."""
    dates = telemetry_daily.select("date_id").distinct()
    active_equipment = equipment.filter(F.col("status").isin("Active", "Idle")).select(
        "equipment_id", "model_id", "customer_id", "install_date"
    )
    comp_ids = components.select("component_id")
    spine = active_equipment.crossJoin(comp_ids).crossJoin(dates)
    # only keep days on/after each machine's install date
    spine = spine.filter(F.col("date_id") >= F.col("install_date"))
    return spine


def build_maintenance_asof(spark, maintenance, telemetry_daily):
    """
    Efficient 'as-of' carry-forward of days-since-last-maintenance, without the
    fan-out join risk (equipment x maintenance-events x dates would explode).
    One row per (equipment_id, date_id); safe to join onto the spine at that key.
    """
    maint_marks = (
        maintenance.select("equipment_id", F.col("scheduled_date").alias("date_id"))
        .distinct()
        .withColumn("maint_date", F.col("date_id"))
    )
    base = (
        telemetry_daily.select("equipment_id", "date_id").distinct()
        .withColumn("maint_date", F.lit(None).cast("date"))
    )
    unioned = base.unionByName(maint_marks)
    dedup = unioned.groupBy("equipment_id", "date_id").agg(F.max("maint_date").alias("maint_date"))

    w = Window.partitionBy("equipment_id").orderBy("date_id").rowsBetween(Window.unboundedPreceding, 0)
    result = dedup.withColumn("last_maint_date", F.last("maint_date", ignorenulls=True).over(w))
    return result.select("equipment_id", "date_id", "last_maint_date")


def engineer_features(spine, daily_component_faults, telemetry_daily, maintenance_asof):
    df = spine.join(daily_component_faults, on=["equipment_id", "component_id", "date_id"], how="left")
    df = df.fillna({"daily_weighted_score": 0.0, "daily_critical_count": 0, "daily_fault_count": 0})

    w7 = Window.partitionBy("equipment_id", "component_id").orderBy("date_id").rowsBetween(-6, 0)
    w14 = Window.partitionBy("equipment_id", "component_id").orderBy("date_id").rowsBetween(-13, 0)
    w30 = Window.partitionBy("equipment_id", "component_id").orderBy("date_id").rowsBetween(-29, 0)

    df = (
        df.withColumn("weighted_score_7d", F.sum("daily_weighted_score").over(w7))
        .withColumn("weighted_score_14d", F.sum("daily_weighted_score").over(w14))
        .withColumn("weighted_score_30d", F.sum("daily_weighted_score").over(w30))
        .withColumn("critical_count_7d", F.sum("daily_critical_count").over(w7))
        .withColumn("critical_count_14d", F.sum("daily_critical_count").over(w14))
        .withColumn("machine_age_days", F.datediff("date_id", "install_date"))
    )

    # engine hours from the equipment-level telemetry (join on equipment+date, no fan-out)
    eng = telemetry_daily.select("equipment_id", "date_id", "engine_hours", "utilization_hours")
    df = df.join(eng, on=["equipment_id", "date_id"], how="left")

    # days since last maintenance - join the precomputed carry-forward table (equipment+date grain)
    df = df.join(maintenance_asof, on=["equipment_id", "date_id"], how="left")
    df = df.withColumn(
        "days_since_last_maintenance",
        F.when(F.col("last_maint_date").isNotNull(), F.datediff("date_id", "last_maint_date")).otherwise(9999),
    ).drop("last_maint_date")

    return df


def build_labels(df, failure_calendar):
    failures = failure_calendar.filter(F.col("event_type") == "Failure").select(
        "equipment_id", "component_id", F.col("failure_date")
    )
    df = df.join(failures, on=["equipment_id", "component_id"], how="left")
    df = df.withColumn(
        "label",
        F.when(
            (F.col("failure_date").isNotNull())
            & (F.datediff("failure_date", "date_id") > 0)
            & (F.datediff("failure_date", "date_id") <= LABEL_WINDOW_DAYS),
            1,
        ).otherwise(0),
    )
    # collapse potential fan-out from multiple failure rows per (equipment, component)
    agg_cols = [c for c in df.columns if c not in ("failure_date", "label")]
    df = df.groupBy(agg_cols).agg(F.max("label").alias("label"))
    return df


FEATURE_COLS = [
    "weighted_score_7d", "weighted_score_14d", "weighted_score_30d",
    "critical_count_7d", "critical_count_14d",
    "engine_hours", "machine_age_days", "days_since_last_maintenance",
]


def train_and_evaluate(df):
    max_date = df.agg(F.max("date_id")).first()[0]
    cutoff = F.date_sub(F.lit(max_date), TEST_HOLDOUT_DAYS)

    train_df = df.filter(F.col("date_id") < cutoff).fillna(0, subset=FEATURE_COLS)
    test_df = df.filter(F.col("date_id") >= cutoff).fillna(0, subset=FEATURE_COLS)

    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features")
    train_vec = assembler.transform(train_df)
    test_vec = assembler.transform(test_df)

    print(f"Train rows: {train_vec.count():,}  (positive rate: "
          f"{train_vec.agg(F.avg('label')).first()[0]:.4f})")
    print(f"Test rows:  {test_vec.count():,}  (positive rate: "
          f"{test_vec.agg(F.avg('label')).first()[0]:.4f})")

    gbt = GBTClassifier(featuresCol="features", labelCol="label", maxIter=50, maxDepth=5, seed=42)
    model = gbt.fit(train_vec)

    predictions = model.transform(test_vec)

    auc = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC").evaluate(predictions)
    precision = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="precisionByLabel", metricLabel=1.0
    ).evaluate(predictions)
    recall = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="recallByLabel", metricLabel=1.0
    ).evaluate(predictions)

    print(f"--- Holdout evaluation (last {TEST_HOLDOUT_DAYS} days, time-based split) ---")
    print(f"AUC:       {auc:.4f}")
    print(f"Precision (label=1): {precision:.4f}")
    print(f"Recall (label=1):    {recall:.4f}")

    return model, assembler


def score_latest(df, model, assembler):
    max_date = df.agg(F.max("date_id")).first()[0]
    latest = df.filter(F.col("date_id") == max_date).fillna(0, subset=FEATURE_COLS)
    latest_vec = assembler.transform(latest)

    scored = model.transform(latest_vec)

    get_p1 = F.udf(lambda v: float(v[1]), "double")
    scored = scored.withColumn("failure_probability", get_p1("probability"))

    scored = scored.withColumn(
        "signal_status",
        F.when(F.col("failure_probability") >= TRIAGE_ACTION_THRESHOLD, F.lit("Action Recommended"))
        .when(F.col("failure_probability") >= TRIAGE_WATCH_THRESHOLD, F.lit("Watch"))
        .otherwise(F.lit("New")),
    )

    return scored.select(
        "equipment_id", "component_id", "date_id", "failure_probability", "signal_status",
        "weighted_score_7d", "critical_count_7d",
    )


def write_signals(scored_df, components, out_dir):
    lead_window_by_component = {
        1: 14, 2: 11, 3: 19, 4: 10, 5: 7, 6: 15, 7: 32, 8: 27, 9: 24, 10: 20, 11: 11, 12: 8,
    }
    lead_window_map = F.create_map(*[F.lit(x) for pair in lead_window_by_component.items() for x in pair])

    out = (
        scored_df
        .withColumn("recommended_action_window_days", lead_window_map[F.col("component_id")])
        .withColumn("model_version", F.lit(MODEL_VERSION))
        .withColumnRenamed("date_id", "scoring_date")
        .withColumnRenamed("component_id", "predicted_component_id")
        .select(
            "equipment_id", "scoring_date", "predicted_component_id", "failure_probability",
            "model_version", "recommended_action_window_days", "signal_status",
        )
    )

    (
        out.coalesce(1).write.mode("overwrite")
        .option("header", True)
        .csv(f"{out_dir}/ml_maintenance_signals_raw")
    )
    print(f"Wrote scored signals to {out_dir}/ml_maintenance_signals_raw (Spark part-file output; "
          f"see accompanying merge step to flatten into a single ml_maintenance_signals.csv)")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sql-dir", default="/opt/sql")
    parser.add_argument("--kafka-data-dir", default="/opt/kafka_data")
    parser.add_argument("--out-dir", default="/opt/sql/transactions")
    parser.add_argument("--sample-days", type=int, default=None,
                         help="If set, only use the most recent N days of telemetry - "
                              "for fast local iteration/testing, not for real training runs.")
    args = parser.parse_args()

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    equipment, components, bridge, telemetry_daily, maintenance, failure_calendar, fault_events = \
        load_data(spark, args.sql_dir, args.kafka_data_dir)

    if args.sample_days:
        max_date = telemetry_daily.agg(F.max("date_id")).first()[0]
        cutoff = F.date_sub(F.lit(max_date), args.sample_days)
        telemetry_daily = telemetry_daily.filter(F.col("date_id") >= cutoff)
        fault_events = fault_events.filter(F.col("event_date") >= cutoff)
        print(f"[--sample-days {args.sample_days}] restricting to dates >= cutoff for fast local run")

    daily_component_faults = fan_out_faults_to_components(fault_events, bridge)
    spine = build_spine(spark, equipment, components, telemetry_daily)
    maintenance_asof = build_maintenance_asof(spark, maintenance, telemetry_daily)
    features = engineer_features(spine, daily_component_faults, telemetry_daily, maintenance_asof)
    labeled = build_labels(features, failure_calendar)

    labeled.cache()
    model, assembler = train_and_evaluate(labeled)
    scored = score_latest(labeled, model, assembler)
    write_signals(scored, components, args.out_dir)

    spark.stop()


if __name__ == "__main__":
    main()
