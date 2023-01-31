from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,FloatType,IntegerType
from pyspark.sql.functions import from_json,col,sum,avg,max,count, window, expr
from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql.functions import decode, from_json, split, regexp_extract, sum,avg, min, max, mean, count, col, asc,desc, to_timestamp, to_date
from pyspark.sql.types import StructType,StructField,LongType,IntegerType,FloatType,StringType

spark = SparkSession.builder\
    .appName("parse_log_streaming")\
    .getOrCreate()
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka:9092") \
  .option("subscribe", "demo") \
  .option("escape", "'")\
  .load()

df.printSchema()


df1 = df.select(col("value"), col("timestamp"))
df1 = df1.select(
    decode("value", 'UTF-8').alias('value')
)

df1.printSchema()


# df1 = df.selectExpr("CAST(value AS STRING)", "timestamp")\
#         .select(col("value"), col("timestamp"))



# df1 = df.selectExpr("CAST(value AS STRING)", "timestamp")\
#         .select(col("value"), col("timestamp"))
        #.groupBy("value").count()
# df2 = df.selectExpr("CAST(value AS STRING)", "timestamp")\
#         .select(col("value"), col("timestamp"))\
#         .withWatermark("timestamp", "1 minutes")\
#         .groupBy("value", "timestamp")\
#         .agg(count("value"))
#         #.groupBy(col("value"), "timestamp").count()

# df2 = df.selectExpr("CAST(value AS STRING)", "timestamp")\
#         .select(col("value"), col("timestamp"))\
#         .withWatermark("timestamp", "10 seconds")\
#         .groupBy(
#             window("timestamp", "10 seconds"),
#             #"value"
#         )\
#         .agg(count("value"))\
#         .select(col("window"), col("count(value)"), expr("window.start"), expr("window.end"))\
        #.groupBy(col("value"), "timestamp").count()
# data_1 = df1.writeStream\
#     .format("console")\
#     .outputMode("complete")\
#     .trigger(processingTime="10 second")\
#     .start()

# data_2 = df2.writeStream\
#     .format("console")\
#     .outputMode("append")\
#     .trigger(processingTime="30 second")\
#     .option("truncate", False)\
#     .start()

#data_1.awaitTermination(10000)

# df.printSchema()
# data=df.writeStream\
#     .format("console")\
#     .outputMode("append")\
#     .trigger(processingTime="30 second")\
#     .start()
#data.awaitTermination()
# split_df = df1.select(from_json(col("value"), value_schema).alias("value"))
split_df = df1.select(
                        'value',
                        regexp_extract('value', r'^([^\s]+)\s', 1).alias('host'),
                        regexp_extract('value', r'^([^\s]+)\s([^\s]+)', 2).alias('available_request'),
                        regexp_extract('value', r'^([^\s]+)\s([^\s]+)\s([^\s]+)\s', 3).alias('user'),
                        regexp_extract('value', r'^.*\[(\d\d/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} [+-]\d{4})\]\s', 1).alias('timestamp'),
                        # regexp_extract('value', r'^.*"(\w+\s+([^\s]+)\s+HTTP[^\s]+)"\s', 1).alias('path'),
                        regexp_extract('value', r'[^"]+"([^"]+)"', 1).alias('path'),
                        regexp_extract('value', r'^.*"\s+(\d+)\s', 1).cast('integer').alias('status'),
                        regexp_extract('value', r'^.*\s+(\d+)\s', 1).cast('integer').alias('content_size'),
                        regexp_extract('value', r'^.*\s+(\d+)\s"([^"]+)"\s', 2).alias('referer'),
                        regexp_extract('value', r'^.*\s+(\d+)\s"([^"]+)"\s"([^"]+)"', 3).alias('agent')
                    ).select(
                        'value',
                        'host',
                        'available_request',
                        'user',
                        'timestamp',
                        to_timestamp('timestamp', "dd/MMM/yyyy:HH:mm:ss ZZZZ").alias("utc_ts"),
                        to_date(to_timestamp('timestamp', "dd/MMM/yyyy")).alias("utc_dt"),
                        'path',
                        regexp_extract('path', r'([^\s]+)\s', 1).alias('method'),
                        regexp_extract('path', r'([^\s]+)\s([^\s]+)\s', 2).alias('endpoint'),
                        regexp_extract('path', r'([^\s]+)\s([^\s]+)\s(HTTP[^\s]+)', 3).alias('http_version'),
                        'status',
                        'content_size',
                        'referer',
                        'agent'
                    )


data_1 = split_df.writeStream\
    .format("console")\
    .outputMode("append")\
    .trigger(processingTime="30 second")\
    .option("truncate", False)\
    .start()
data_1.awaitTermination(10000)
# data_2.awaitTermination(10000)
