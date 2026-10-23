docker exec -it spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 `
  --conf spark.jars.ivy=/tmp/.ivy2 `
  --conf spark.driver.host=spark-master `
  --conf spark.driver.bindAddress=0.0.0.0 `
  --driver-memory 768m `
  --executor-memory 512m `
  --executor-cores 2 `
  --total-executor-cores 4 `
  /opt/spark_app/simple_job.py