import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType


# ---------- Config helpers ----------
def must_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v


def get_env(name: str, default: str) -> str:
    return os.getenv(name, default)


def validate_jdbc_url(url: str) -> None:
    # Tell-it-like-it-is validation: this is the #1 reason you get "No suitable driver"
    if not url.startswith("jdbc:postgresql://"):
        raise RuntimeError(
            f"POSTGRES_JDBC_URL must start with 'jdbc:postgresql://'\n"
            f"Got: {url}\n"
            f"Example: jdbc:postgresql://wikimedia-postgres:5432/postgres"
        )


# ---------- Optional: ensure table exists (executed via JVM JDBC) ----------
def ensure_table_exists(spark: SparkSession, jdbc_url: str, user: str, password: str, table: str) -> None:
    jvm = spark._sc._jvm
    # Force driver registration so DriverManager can find it
    jvm.java.lang.Class.forName("org.postgresql.Driver")

    conn = None
    stmt = None
    try:
        props = jvm.java.util.Properties()
        props.setProperty("user", user)
        props.setProperty("password", password)

        conn = jvm.java.sql.DriverManager.getConnection(jdbc_url, props)
        stmt = conn.createStatement()

        # Keep schema pragmatic: store key fields + raw_json for flexibility
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {table} (
            doc_id TEXT,
            source TEXT,
            text TEXT,
            project TEXT,
            username TEXT,
            event_ts TEXT,
            raw_json TEXT,
            ingested_at TIMESTAMPTZ DEFAULT NOW()
        );
        """
        stmt.execute(ddl)
    finally:
        if stmt is not None:
            stmt.close()
        if conn is not None:
            conn.close()


# ---------- Main batch writer ----------
def write_batch_to_postgres(batch_df, batch_id: int) -> None:
    if batch_df.rdd.isEmpty():
        return

    spark = batch_df.sparkSession

    jdbc_url = must_env("POSTGRES_JDBC_URL")
    user = must_env("POSTGRES_USER")
    password = must_env("POSTGRES_PASSWORD")
    table = get_env("POSTGRES_TABLE", "wikimedia_events")

    validate_jdbc_url(jdbc_url)

    # This is the money line: removes classloader ambiguity, fixes DriverManager lookup.
    spark._sc._jvm.java.lang.Class.forName("org.postgresql.Driver")

    # Select/flatten columns (schema below matches your producer: doc_id, source, text, meta{project,user,ts})
    out_df = (
        batch_df.select(
            col("doc_id").cast("string").alias("doc_id"),
            col("source").cast("string").alias("source"),
            col("text").cast("string").alias("text"),
            col("meta.project").cast("string").alias("project"),
            col("meta.user").cast("string").alias("username"),
            col("meta.ts").cast("string").alias("event_ts"),
            col("raw_json").cast("string").alias("raw_json"),
        )
    )

    # Write to Postgres using JDBC
    (out_df.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("driver", "org.postgresql.Driver")
        .option("dbtable", table)
        .option("user", user)
        .option("password", password)
        .mode("append")
        .save()
    )


def main() -> None:
    # Required
    kafka_brokers = must_env("KAFKA_BROKERS")          # e.g. wikimedia-redpanda:29092
    kafka_topic = must_env("KAFKA_TOPIC")              # e.g. wikimedia.cleaned
    jdbc_url = must_env("POSTGRES_JDBC_URL")           # e.g. jdbc:postgresql://wikimedia-postgres:5432/postgres
    pg_user = must_env("POSTGRES_USER")
    pg_password = must_env("POSTGRES_PASSWORD")

    # Optional
    table = get_env("POSTGRES_TABLE", "wikimedia_events")
    checkpoint_dir = get_env("CHECKPOINT_DIR", "/tmp/checkpoints/wikimedia_to_postgres")
    starting_offsets = get_env("KAFKA_STARTING_OFFSETS", "latest")  # latest | earliest
    trigger_seconds = int(get_env("TRIGGER_SECONDS", "10"))

    validate_jdbc_url(jdbc_url)

    # Force packages from inside too (belt + suspenders).
    # If you already pass --packages at spark-submit, this is still fine.
    spark = (
        SparkSession.builder
        .appName("wikimedia-kafka-to-postgres")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(get_env("SPARK_LOG_LEVEL", "WARN"))

    # Ensure destination table exists (optional but recommended so first run doesn’t faceplant)
    ensure_table_exists(spark, jdbc_url, pg_user, pg_password, table)

    # Input schema matches your producer payload
    schema = StructType([
        StructField("doc_id", StringType(), True),
        StructField("source", StringType(), True),
        StructField("text", StringType(), True),
        StructField("meta", StructType([
            StructField("project", StringType(), True),
            StructField("user", StringType(), True),
            StructField("ts", StringType(), True),
        ]), True),
    ])

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_brokers)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw.selectExpr("CAST(value AS STRING) AS raw_json")
        .withColumn("data", from_json(col("raw_json"), schema))
        .select(
            col("data.doc_id").alias("doc_id"),
            col("data.source").alias("source"),
            col("data.text").alias("text"),
            col("data.meta").alias("meta"),
            col("raw_json").alias("raw_json"),
        )
    )

    query = (
        parsed.writeStream
        .foreachBatch(write_batch_to_postgres)
        .option("checkpointLocation", checkpoint_dir)
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"FATAL: {e}", file=sys.stderr)
        raise
