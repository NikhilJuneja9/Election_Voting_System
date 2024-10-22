import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json,col
from pyspark.sql.functions import sum as _sum



from pyspark.sql.types import StructType,StructField,StringType,TimestampType,IntegerType
# to connect spark with postgres we will need a jdbc connector - recomm is java 8 one

if __name__ == "__main__":
    spark = (SparkSession.builder
             .appName("Realtime-Voting-System")
             .config('spark.jars.packages','org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0')
             .config('spark.jars','E:\Realtime-Voting-System\postgresql-42.7.4.jar')
             .config('spark.sql.adaptive.enable','false')
             .getOrCreate())
    

    vote_schema = StructType(
    [StructField('voter_id',StringType(),nullable=True),
        StructField('candidate_id',StringType(),nullable=True),
        StructField('voting_time',TimestampType(),nullable=True),
        StructField('voter_name',StringType(),nullable=True),
        StructField('party_affiliation',StringType(),nullable=True),
        StructField('biography',StringType(),nullable=True),
        StructField('campaign_platform',StringType(),nullable=True),
        StructField('photo_url',StringType(),nullable=True),
        StructField('candidate_name',StringType(),nullable=True),
        StructField('date_of_birth',StringType(),nullable=True),
        StructField('gender',StringType(),nullable=True),
        StructField('nationality',StringType(),nullable=True),
        StructField('registration_number',StringType(),nullable=True),
        StructField('address',StructType([
            StructField('street',StringType(),nullable=True),
            StructField('city',StringType(),nullable=True),
            StructField('state',StringType(),nullable=True),
            StructField('country',StringType(),nullable=True),
            StructField('postcode',StringType(),nullable=True),
    ]),nullable=True),
        StructField('voter_id',StringType(),nullable=True),
        StructField('phone_number',StringType(),nullable=True),
        StructField('picture',StringType(),nullable=True),
        StructField('registered_age',IntegerType(),nullable=True),
        StructField('vote',IntegerType(),nullable=True)
        ]
    )


    votes_df = (
    spark.readStream
    .format('kafka')
    .option('kafka.bootstrap.servers','localhost:9092')
    .option('subscribe', 'votes-topic')
    .option('startingOffsets','earliest')
    .load()
    .selectExpr("CAST(value AS STRING)")
    .select(from_json(col('value'), vote_schema).alias('data'))
    .select('data.*')
    )


    # Data preprocessing , typecasting and watermarking

    votes_df = votes_df.withColumn('voting_time',col('voting_time').cast(TimestampType())).withColumn('vote',col('vote').cast(IntegerType()))

    enriched_votes_df = votes_df.withWatermark('voting_time','1 minute')# any data late than 1 minute will not be processed by the spark

    # Aggregate votes per candidate and turnout by location

    votes_per_candidate = enriched_votes_df.group_by('candidate_id','candidate_name','party_affiliation', 
                                                    'photo_url').agg(_sum('vote')).alias("total_votes")

    turnout_by_location = enriched_votes_df.groupby('address.state').count().alais('total_votes')

    votes_per_candidate_to_kafka = (votes_per_candidate.selectExpr('to_json(struct(*)) as value')
                                .writeStream
                                .format('kafka')
                                .option('kafka.bootstrap.servers', 'localhost:9092')
                                .option('topic','aggregated_votes_per_candidate')
                                .option('checkpointLocation', "E:\Realtime-Voting-System\checkpoints\checkpoint1")
                                .outputMode('update')
                                .start()
                                )

    turnout_by_location_to_kafka = (turnout_by_location.selectExpr('to_json(struct(*)) as value')
                                .writeStream
                                .format('kafka')
                                .option('kafka.bootstrap.servers', 'localhost:9092')
                                .option('topic','aggregated_turnout_by_location')
                                .option('checkpointLocation', "E:\Realtime-Voting-System\checkpoints\checkpoint2")
                                .outputMode('update')
                                .start()
                                )


    # Await termination for the streaming queries
    votes_per_candidate_to_kafka.awaitTermination()
    turnout_by_location_to_kafka.awaitTermination()