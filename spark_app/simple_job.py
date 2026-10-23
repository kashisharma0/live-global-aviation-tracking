import json
import os
import time

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (BooleanType, DoubleType, LongType,
                               StringType, StructField, StructType)
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

OUT = "/opt/data/live"
os.makedirs(OUT, exist_ok=True)
t0 = time.time()

spark = (SparkSession.builder.appName("FlightAnalysis")
         .config("spark.sql.shuffle.partitions", "6").getOrCreate())
spark.sparkContext.setLogLevel("WARN")


def save(name, df_or_obj):
    obj = ([r.asDict() for r in df_or_obj.collect()]
           if hasattr(df_or_obj, "collect") else df_or_obj)
    with open(f"{OUT}/{name}", "w") as f:
        json.dump(obj, f)


# ---------- 1. Read from Kafka ----------
schema = StructType([
    StructField("icao24", StringType()),
    StructField("callsign", StringType()),
    StructField("origin_country", StringType()),
    StructField("longitude", DoubleType()),
    StructField("latitude", DoubleType()),
    StructField("baro_altitude", DoubleType()),
    StructField("on_ground", BooleanType()),
    StructField("velocity", DoubleType()),
    StructField("true_track", DoubleType()),
    StructField("api_time", LongType()),
])

raw = (spark.read.format("kafka")
       .option("kafka.bootstrap.servers", "kafka:9092")
       .option("subscribe", "flights_raw")
       .option("startingOffsets", "earliest")
       .option("endingOffsets", "latest")
       .load())

# ---------- 2. Parse JSON and clean ----------
df = (raw.select(F.from_json(F.col("value").cast("string"), schema).alias("d"))
         .select("d.*")
         .filter(F.col("icao24").isNotNull())
         .filter(F.col("latitude").between(-90, 90) & F.col("longitude").between(-180, 180))
         .withColumn("altitude_ft", F.round(F.col("baro_altitude") * 3.28084, 0))
         .withColumn("speed_kmh", F.round(F.col("velocity") * 3.6, 1)))
df.persist()
total_messages = df.count()

# ---------- 3. Latest record per aircraft (current sky) ----------
max_t = df.agg(F.max("api_time")).first()[0]
recent = df.filter(F.col("api_time") >= max_t - 200)
w = Window.partitionBy("icao24").orderBy(F.col("api_time").desc())
latest = recent.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
latest.persist()
latest.createOrReplaceTempView("latest_flights")

# ---------- 4. Spark SQL ----------
kpis = spark.sql("""
    SELECT COUNT(*) AS total_aircraft,
           SUM(CASE WHEN on_ground = false THEN 1 ELSE 0 END) AS airborne,
           SUM(CASE WHEN on_ground = true  THEN 1 ELSE 0 END) AS grounded,
           ROUND(AVG(CASE WHEN on_ground = false THEN altitude_ft END), 0) AS avg_altitude_ft,
           ROUND(AVG(CASE WHEN on_ground = false THEN speed_kmh END), 0)   AS avg_speed_kmh,
           COUNT(DISTINCT origin_country) AS countries,
           MAX(api_time) AS snapshot_time
    FROM latest_flights""").first().asDict()
kpis["messages_read_from_kafka"] = total_messages
save("kpis.json", kpis)

save("countries.json", spark.sql("""
    SELECT origin_country AS country, COUNT(*) AS aircraft,
           ROUND(AVG(CASE WHEN on_ground = false THEN altitude_ft END), 0) AS avg_altitude_ft,
           ROUND(AVG(CASE WHEN on_ground = false THEN speed_kmh END), 0)   AS avg_speed_kmh
    FROM latest_flights GROUP BY origin_country
    ORDER BY aircraft DESC LIMIT 15"""))

save("altitude_bands.json", spark.sql("""
    SELECT CASE WHEN altitude_ft < 10000 THEN '1) 0-10k ft'
                WHEN altitude_ft < 20000 THEN '2) 10-20k ft'
                WHEN altitude_ft < 30000 THEN '3) 20-30k ft'
                WHEN altitude_ft < 40000 THEN '4) 30-40k ft'
                ELSE '5) 40k+ ft' END AS band,
           COUNT(*) AS aircraft
    FROM latest_flights
    WHERE on_ground = false AND altitude_ft IS NOT NULL
    GROUP BY band ORDER BY band"""))

# ---------- 5. MLlib K-means ----------
air = latest.filter("on_ground = false AND altitude_ft IS NOT NULL AND speed_kmh IS NOT NULL")
vec = VectorAssembler(inputCols=["latitude", "longitude", "altitude_ft", "speed_kmh"],
                      outputCol="raw_features").transform(air)
scaled = (StandardScaler(inputCol="raw_features", outputCol="features",
                         withMean=True, withStd=True).fit(vec).transform(vec))
model = KMeans(k=4, seed=42, featuresCol="features").fit(scaled)
pred = model.transform(scaled)
silhouette = ClusteringEvaluator().evaluate(pred)

clusters = [r.asDict() for r in pred.groupBy("prediction").agg(
    F.count("*").alias("aircraft"),
    F.round(F.avg("altitude_ft"), 0).alias("avg_altitude_ft"),
    F.round(F.avg("speed_kmh"), 0).alias("avg_speed_kmh"),
    F.round(F.avg("latitude"), 1).alias("avg_lat"),
    F.round(F.avg("longitude"), 1).alias("avg_lon")).orderBy("prediction").collect()]
save("clusters.json", {"silhouette": round(silhouette, 3), "clusters": clusters})

# ---------- 6. Positions for the map ----------
save("positions.json", pred.select(
    "icao24", "callsign", "origin_country", "latitude", "longitude",
    "altitude_ft", "speed_kmh", F.col("prediction").alias("cluster"))
    .orderBy(F.rand(42)).limit(4000))

elapsed = round(time.time() - t0, 1)
save("run_info.json", {"seconds": elapsed, "messages": total_messages})
print(f"DONE: messages={total_messages} aircraft={kpis['total_aircraft']} "
      f"airborne={kpis['airborne']} silhouette={silhouette:.3f} time={elapsed}s", flush=True)

# Keep the Spark UI (port 4040) open for 2 minutes so you can take screenshots
time.sleep(120)
spark.stop()