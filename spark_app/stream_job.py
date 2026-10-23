import json
import os
import time

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (BooleanType, DoubleType, IntegerType,
                               LongType, StringType, StructField, StructType)

# ---------------- Configuration ----------------
KAFKA_BOOTSTRAP = "kafka:9092"          # INTERNAL listener (container to container)
TOPIC = "flights_raw"
LAKE = "/opt/lake"                       # Docker named volume (fast, for Spark)
CLEAN_PATH = f"{LAKE}/flights_clean"     # Parquet store of cleaned records
CHECKPOINT = f"{LAKE}/checkpoints/flights"
LIVE_DIR = "/opt/data/live"              # Windows-visible folder (JSON for the UI)
LOOKBACK_SEC = 200                       # must be > 2 x producer POLL_INTERVAL
TRIGGER = "30 seconds"
MAX_OFFSETS = 30000                      # max Kafka messages per micro-batch

spark = (SparkSession.builder
         .appName("FlightStreamingPipeline")
         .config("spark.sql.shuffle.partitions", "6")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# ---------------- Schema of the JSON sent by the producer ----------------
schema = StructType([
    StructField("icao24", StringType()),
    StructField("callsign", StringType()),
    StructField("origin_country", StringType()),
    StructField("time_position", LongType()),
    StructField("last_contact", LongType()),
    StructField("longitude", DoubleType()),
    StructField("latitude", DoubleType()),
    StructField("baro_altitude", DoubleType()),
    StructField("on_ground", BooleanType()),
    StructField("velocity", DoubleType()),
    StructField("true_track", DoubleType()),
    StructField("vertical_rate", DoubleType()),
    StructField("geo_altitude", DoubleType()),
    StructField("squawk", StringType()),
    StructField("spi", BooleanType()),
    StructField("position_source", IntegerType()),
    StructField("api_time", LongType()),
    StructField("ingest_time", StringType()),
])


# ---------------- Small helpers (run on the driver) ----------------
def write_json(name, obj):
    """Write atomically so the UI never reads a half-written file."""
    os.makedirs(LIVE_DIR, exist_ok=True)
    tmp = os.path.join(LIVE_DIR, f".{name}.tmp")
    with open(tmp, "w") as f:
        json.dump(obj, f)
    os.replace(tmp, os.path.join(LIVE_DIR, name))


def read_json(name, default):
    try:
        with open(os.path.join(LIVE_DIR, name)) as f:
            return json.load(f)
    except Exception:
        return default


def rows(df):
    return [r.asDict() for r in df.collect()]


def make_regions(s):
    """Tiny reference table: rough, non-overlapping continent boxes."""
    return s.createDataFrame([
        ("North America",   7.0,  84.0, -170.0,  -50.0),
        ("South America", -56.0,   7.0,  -92.0,  -30.0),
        ("Europe",         35.0,  72.0,  -25.0,   45.0),
        ("Africa",        -35.0,  35.0,  -20.0,   45.0),
        ("Asia",          -11.0,  82.0,   45.0,  180.0),
        ("Oceania",       -50.0, -11.0,  110.0,  180.0),
    ], ["region", "lat_min", "lat_max", "lon_min", "lon_max"])


# ---------------- Work done for every micro-batch ----------------
def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    t0 = time.time()
    s = batch_df.sparkSession

    # 1. Cache the batch, count it, and append it to the Parquet store
    batch_df.persist()
    rows_in = batch_df.count()
    batch_df.write.mode("append").parquet(CLEAN_PATH)

    # 2. Read back the recent past and keep the latest record per aircraft
    max_t = batch_df.agg(F.max("api_time")).first()[0]
    # Handle an empty Parquet store on the first batch.
    has_parquet = any(
        name.startswith("part-") and name.endswith(".parquet")
        for name in os.listdir(CLEAN_PATH)
    ) if os.path.isdir(CLEAN_PATH) else False

    recent_source = s.read.parquet(CLEAN_PATH) if has_parquet else batch_df
    recent = recent_source.filter(
        F.col("api_time") >= max_t - LOOKBACK_SEC
    )
    w = Window.partitionBy("icao24").orderBy(F.col("api_time").desc())
    latest = (recent.withColumn("rn", F.row_number().over(w))
                    .filter("rn = 1").drop("rn"))
    latest.persist()
    latest.createOrReplaceTempView("latest_flights")

    # 3. Spark SQL aggregations
    kpis = rows(s.sql("""
        SELECT COUNT(*) AS total_aircraft,
               SUM(CASE WHEN on_ground = false THEN 1 ELSE 0 END) AS airborne,
               SUM(CASE WHEN on_ground = true  THEN 1 ELSE 0 END) AS grounded,
               ROUND(AVG(CASE WHEN on_ground = false THEN altitude_ft END), 0) AS avg_altitude_ft,
               ROUND(AVG(CASE WHEN on_ground = false THEN speed_kmh END), 0)   AS avg_speed_kmh,
               COUNT(DISTINCT origin_country) AS countries,
               MAX(api_time) AS snapshot_time
        FROM latest_flights
    """))[0]

    countries = rows(s.sql("""
        SELECT origin_country AS country,
               COUNT(*) AS aircraft,
               ROUND(AVG(CASE WHEN on_ground = false THEN altitude_ft END), 0) AS avg_altitude_ft,
               ROUND(AVG(CASE WHEN on_ground = false THEN speed_kmh END), 0)   AS avg_speed_kmh
        FROM latest_flights
        GROUP BY origin_country
        ORDER BY aircraft DESC
        LIMIT 15
    """))

    bands = rows(s.sql("""
        SELECT CASE WHEN altitude_ft < 10000 THEN '0-10k ft'
                    WHEN altitude_ft < 20000 THEN '10-20k ft'
                    WHEN altitude_ft < 30000 THEN '20-30k ft'
                    WHEN altitude_ft < 40000 THEN '30-40k ft'
                    ELSE '40k+ ft' END AS band,
               COUNT(*) AS aircraft
        FROM latest_flights
        WHERE on_ground = false AND altitude_ft IS NOT NULL
        GROUP BY band
        ORDER BY MIN(altitude_ft)
    """))

    # 4. DataFrame API: 5-degree traffic grid (for a heatmap)
    grid = rows(latest
                .withColumn("lat_cell", F.floor(F.col("latitude") / 5) * 5)
                .withColumn("lon_cell", F.floor(F.col("longitude") / 5) * 5)
                .groupBy("lat_cell", "lon_cell").count()
                .orderBy(F.desc("count")))

    # 5. Broadcast join with the regions reference table
    regions = make_regions(s)
    joined = latest.alias("f").join(
        F.broadcast(regions.alias("r")),
        (F.col("f.latitude") >= F.col("r.lat_min")) &
        (F.col("f.latitude") < F.col("r.lat_max")) &
        (F.col("f.longitude") >= F.col("r.lon_min")) &
        (F.col("f.longitude") < F.col("r.lon_max")),
        "left")
    by_region = rows(joined
                     .withColumn("region", F.coalesce(F.col("region"), F.lit("Oceanic / Other")))
                     .groupBy("region")
                     .agg(F.count("*").alias("aircraft"),
                          F.round(F.avg("speed_kmh"), 0).alias("avg_speed_kmh"))
                     .orderBy(F.desc("aircraft")))

    # 6. Traffic over time (one point per producer cycle)
    history_rows = rows(recent.groupBy("api_time").agg(
        F.count("*").alias("total"),
        F.sum(F.when(F.col("on_ground") == False, 1).otherwise(0)).alias("airborne"))
        .orderBy("api_time"))

    # 7. Positions for the map (compact arrays to keep the file small)
    positions = [list(r) for r in latest.select(
        "icao24", "callsign", "origin_country",
        F.round("latitude", 4), F.round("longitude", 4),
        "altitude_ft", "speed_kmh", "true_track", "on_ground").collect()]

    # 8. Save everything the UI needs
    hist = {h["api_time"]: h for h in read_json("history.json", [])}
    for h in history_rows:
        hist[h["api_time"]] = h            # later (complete) values replace earlier ones
    hist = [hist[k] for k in sorted(hist)][-300:]

    write_json("kpis.json", kpis)
    write_json("countries.json", countries)
    write_json("altitude_bands.json", bands)
    write_json("grid.json", grid)
    write_json("regions.json", by_region)
    write_json("history.json", hist)
    write_json("positions.json", positions)

    elapsed = round(time.time() - t0, 2)
    metrics = read_json("metrics.json", [])
    metrics.append({"batch_id": batch_id, "rows": rows_in,
                    "seconds": elapsed, "finished_at": int(time.time())})
    write_json("metrics.json", metrics[-200:])

    print(f"[batch {batch_id}] rows_in={rows_in} aircraft={kpis['total_aircraft']} "
          f"airborne={kpis['airborne']} countries={kpis['countries']} "
          f"time={elapsed}s", flush=True)

    latest.unpersist()
    batch_df.unpersist()


# ---------------- Build and start the streaming query ----------------
raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
       .option("subscribe", TOPIC)
       .option("startingOffsets", "earliest")
       .option("maxOffsetsPerTrigger", MAX_OFFSETS)
       .load())

clean = (raw.select(F.from_json(F.col("value").cast("string"), schema).alias("d"))
            .select("d.*")
            .filter(F.col("icao24").isNotNull())
            .filter(F.col("latitude").between(-90, 90) &
                    F.col("longitude").between(-180, 180))
            .withColumn("altitude_ft", F.round(F.col("baro_altitude") * 3.28084, 0))
            .withColumn("speed_kmh", F.round(F.col("velocity") * 3.6, 1)))

query = (clean.writeStream
         .foreachBatch(process_batch)
         .option("checkpointLocation", CHECKPOINT)
         .trigger(processingTime=TRIGGER)
         .start())

print("Streaming query started. Waiting for data ...", flush=True)
query.awaitTermination()