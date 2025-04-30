
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as _sum, count, hour, when, round as _round
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import time
import pyspark
from pyspark.sql import functions as F

print(pyspark.__version__)  # to check the version of pyspark
voter_population_by_city = {
    "Worcester": 75000,
    "Oxford": 120000,
    "Cambridge": 100000,
    "Sunderland": 200000,
    "Stoke-on-Trent": 180000,
    "Edinburgh": 400000,
    "Glasgow": 500000,
    "Bradford": 300000,
    "Lichfield": 25000,
    "Kingston upon Hull": 200000,
    "Coventry": 250000,
    "Lisburn": 80000,
    "Inverness": 50000,
    "Manchester": 400000,
    "Cardiff": 300000,
    "Londonderry": 60000,
    "Wakefield": 150000,
    "Norwich": 100000,
    "Dundee": 100000,
    "Salford": 120000,
    "Truro": 20000,
    "Leicester": 250000,
    "Derby": 200000,
    "Belfast": 300000,
    "Birmingham": 800000,
    "Winchester": 40000,
    "Wells": 10000,
    "Exeter": 80000,
    "Lincoln": 60000,
    "Sheffield": 400000,
    "Westminster": 100000,
    "Newport": 100000,
    "Plymouth": 150000,
    "Newry": 50000,
    "Canterbury": 60000,
    "Aberdeen": 150000,
    "Carlisle": 60000,
    "Chichester": 30000,
    "Hereford": 40000,
    "Stevenage": 80000,
    "Swansea": 150000,
    "Southampton": 200000,
    "St Davids": 2000,
    "Bristol": 300000,
    "Durham": 60000,
    "Salisbury": 40000,
    "Gloucester": 80000,
    "Peterborough": 100000,
    "Newcastle upon Tyne": 200000,
    "Ely": 10000,
    "Bangor": 20000,
    "Liverpool": 400000,
    "Armagh": 20000,
    "Brighton and Hove": 200000,
    "Wolverhampton": 150000,
    "Chester": 80000,
    "Nottingham": 250000,
    "Portsmouth": 150000,
    "Stirling": 40000,
    "City of London": 8000,
    "Preston": 100000,
    "Bath": 60000,
    "York": 100000,
    "St Albans": 60000,
    "Leeds": 500000,
    "Ripon": 20000
}
if __name__ == "__main__":
    # Initialize SparkSession
    spark = (SparkSession.builder
            .appName("ElectionAnalysis")
            .master("local[*]")
            .config("spark.jars.packages", 
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.postgresql:postgresql:42.7.5")
            .config("spark.sql.adaptive.enabled", "false")
            .getOrCreate())
    # Step 2: Convert dictionary to Spark DataFrame
    population_df = spark.createDataFrame(
    [(city, pop) for city, pop in voter_population_by_city.items()],
    schema=StructType([
        StructField("city", StringType(), True),
        StructField("population", IntegerType(), True)
    ])
)
    # Define schema
    vote_schema = StructType([
        StructField("voter_id", StringType(), True),
        StructField("candidate_id", StringType(), True),
        StructField("voting_time", TimestampType(), True),
        StructField("voting_method", StringType(), True),
        StructField("polling_station", StringType(), True),
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
        StructField("education_level", StringType(), True),
        StructField("party", StringType(), True),
        StructField("picture", StringType(), True),
        StructField("registered_age", IntegerType(), True),
        StructField("vote", IntegerType(), True),
        StructField("income_category", StringType(), True)
    ])

    # Read from Kafka
    votes_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "votes_topic") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load() \
        .selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), vote_schema).alias("data")) \
        .select("data.*")

    # Preprocessing
    enriched_votes_df = votes_df.withColumn("voting_time", col("voting_time").cast(TimestampType())) \
                                .withColumn("vote", col("vote").cast(IntegerType())) \
                                .withWatermark("voting_time", "10 seconds")

    # 1. Votes per candidate
    votes_per_candidate = enriched_votes_df.groupBy("candidate_id", "candidate_name", "party_affiliation", "photo_url") \
        .agg(_sum("vote").alias("total_votes"))

    # 2. Turnout by location
    turnout_by_location = enriched_votes_df.groupBy("address.state") \
        .agg(count("voter_id").alias("total_votes"))

    
    # 4. Top 3 polling stations by total votes cast
    top_polling_stations = enriched_votes_df.groupBy("polling_station") \
        .agg(count("voter_id").alias("votes_cast")) 
        

    # 5. Voters by education level and party affiliation
    voters_by_edu_party = enriched_votes_df.groupBy("education_level", "party_affiliation") \
        .agg(count("voter_id").alias("num_voters"))

    # 6. Turnout rate by hour of day
   # Group by the hour extracted from voting_time
    hourly_turnout = enriched_votes_df.groupBy(hour("voting_time").alias("hour_of_day")) \
    .agg(count("*").alias("total_votes")) \
    
        

    # Kafka writes (add topics & checkpoints as needed)
    def write_to_kafka(df, topic, checkpoint):
        return df.selectExpr("to_json(struct(*)) AS value") \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("topic", topic) \
            .option("checkpointLocation", checkpoint) \
            .outputMode("update") \
            .start()

    write_to_kafka(votes_per_candidate, "aggregated_votes_per_candidate", "F:/DE_Projects/Real_time _election/checkpoints/votes_per_candidate")
    write_to_kafka(turnout_by_location, "aggregated_turnout_by_location", "F:/DE_Projects/Real_time _election/checkpoints/turnout_by_location")
    write_to_kafka(top_polling_stations, "top_polling_stations", "F:/DE_Projects/Real_time _election/checkpoints/top_polling_stations")
    write_to_kafka(voters_by_edu_party, "voters_by_education_party", "F:/DE_Projects/Real_time _election/checkpoints/voters_by_edu_party")
    write_to_kafka(hourly_turnout, "turnout_by_hour", "F:/DE_Projects/Real_time _election/checkpoints/turnout_by_hour")

    spark.streams.awaitAnyTermination()

