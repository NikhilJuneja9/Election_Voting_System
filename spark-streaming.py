import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json,col
from pyspark.sql.functions import sum as _sum

from pyspark.sql.types import StructType,StructField,StringType,TimestampType,IntegerType
# to connect spark with postgres we will need a jdbc connector - recomm is java 8 one

if __name__ == "__main__":
    try:
        spark = (SparkSession.builder
                .appName(name = "Realtime-Voting-System")
                
                .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") # kafka spark connector 
                .config('spark.jars','/mnt/e/Realtime-Voting-System/spark-streaming.py')
                .config("spark.sql.adaptive.enabled", "false")
                .getOrCreate())
    except Exception as e:
        raise(e)

    vote_schema = StructType([
        StructField("voter_id", StringType(), True),
        StructField("candidate_id", StringType(), True),
        StructField("voting_time", TimestampType(), True),
        StructField("voter_name", StringType(), True),
        StructField("party_affiliation", StringType(), True),
        StructField("biography", StringType(), True),
        StructField("campaign_platform", StringType(), True),
        StructField("photo_url", StringType(), True),
        StructField("candidate_name", StringType(), True),
        StructField("date_of_birth", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("nationality", StringType(), True),
        StructField("registration_number", StringType(), True),
        StructField("address", StructType([
            StructField("street", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("country", StringType(), True),
            StructField("postcode", StringType(), True)
        ]), True),
        StructField("email", StringType(), True),
        StructField("phone_number", StringType(), True),
        StructField("cell_number", StringType(), True),
        StructField("picture", StringType(), True),
        StructField("registered_age", IntegerType(), True),
        StructField("vote", IntegerType(), True)
    ])

#This initializes a streaming DataFrame that will continuously receive data.
    votes_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "votes_topic") \
        .option("startingOffsets", "earliest") \
        .load() \
        .selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), vote_schema).alias("data")) \
        .select("data.*")


    votes_df = votes_df.withColumn("voting_time", col("voting_time").cast(TimestampType())) \
        .withColumn('vote', col('vote').cast(IntegerType()))
    enriched_votes_df = votes_df.withWatermark("voting_time", "1 minute")

    # Aggregate votes per candidate and turnout by location
    votes_per_candidate = enriched_votes_df.groupBy("candidate_id", "candidate_name", "party_affiliation",
                                                    "photo_url").agg(_sum("vote").alias("total_votes"))
    turnout_by_location = enriched_votes_df.groupBy("address.state").count().alias("total_votes")

try:
    # Starting streaming queries
    votes_per_candidate_to_kafka = (votes_per_candidate.selectExpr('to_json(struct(*)) as value')
                                    .writeStream
                                    .format('kafka')
                                    .option('kafka.bootstrap.servers', 'localhost:9092')
                                    .option('topic', 'aggregated_votes_per_candidate')
                                    .option('checkpointLocation', "file:///mnt/e/Realtime-Voting-System/checkpoints/checkpoint1")
                                    .outputMode('update')
                                    .start()
                                    )

    turnout_by_location_to_kafka = (turnout_by_location.selectExpr('to_json(struct(*)) as value')
                                    .writeStream
                                    .format('kafka')
                                    .option('kafka.bootstrap.servers', 'localhost:9092')
                                    .option('topic', 'aggregated_turnout_by_location')
                                    .option('checkpointLocation', "file:///mnt/e/Realtime-Voting-System/checkpoints/checkpoint2")
                                    .outputMode('update')
                                    .start()
                                    )

    # Await termination for the streaming queries
    votes_per_candidate_to_kafka.awaitTermination()
    turnout_by_location_to_kafka.awaitTermination()
except Exception as e:
    print(f"An error occurred: {str(e)}")