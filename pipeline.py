# Import necessary modules
import findspark
from pyspark.sql import SparkSession
from twitterpipe_globals import *
from pyspark.sql.functions import decode, get_json_object
from pyspark.sql.functions import udf
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from nrclex import NRCLex

# Initialize variables
joined_query = None
timestamped_joined_query = None
timestamped_joined_query2 = None

# Initialize findspark and add packages
findspark.init()
findspark.add_packages('org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0')

# Create a SparkSession
spark = SparkSession.builder.appName('Spark1').getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

# Read data from Kafka topic as a DataFrame
tweet_sdf = spark.readStream \
                .format('kafka') \
                .option('kafka.bootstrap.servers', KAFKA_SERVER) \
                .option('subscribe', TWEET_TOPIC) \
                .option('startingOffsets', 'earliest') \
                .option("failOnDataLoss", "false") \
                .load()

# Read data from a second Kafka topic as a DataFrame
tweet_sdf2 = spark.readStream \
                .format('kafka') \
                .option('kafka.bootstrap.servers', KAFKA_SERVER) \
                .option('subscribe', TWEET_TOPIC2) \
                .option('startingOffsets', 'earliest') \
                .option("failOnDataLoss", "false") \
                .load()

# Select the necessary columns from the DataFrame and create a new DataFrame
selected_tweet_sdf = tweet_sdf \
                .select(decode(tweet_sdf.value, 'utf-8').alias('json_str')) \
                .select('*', get_json_object('json_str', '$.data.id').alias('id')) \
               

# Select the necessary columns from the DataFrame and create a new DataFrame
selected_tweet_sdf = tweet_sdf \
                .select(decode(tweet_sdf.value, 'utf-8').alias('json_str')) \
                .select('*', get_json_object('json_str', '$.data.id').alias('id')) \
                .select('*', get_json_object('json_str', '$.data.text').alias('text')) \
                .select('id', 'text')

# Select the necessary columns from the second DataFrame and create a new DataFrame
selected_tweet_sdf2 = tweet_sdf2 \
                .select(decode(tweet_sdf2.value, 'utf-8').alias('json_str')) \
                .select('*', get_json_object('json_str', '$.data.id').alias('id')) \
                .select('*', get_json_object('json_str', '$.data.text').alias('text')) \
                .select('id', 'text')


# Sensitivity analysis ---------------------------------------------------------

# Define a function that uses the Vader SentimentIntensityAnalyzer to score text
def vader_score(text):
    sent = SentimentIntensityAnalyzer().polarity_scores(text)
    sent2 = sent['compound']
    return sent2

# Define a function that uses the NRCLex to score text
def emo_score(text):
    emo =  NRCLex(text)
    emotions = []
    # Loop through the top emotions and append them to a list
    for lines in emo.top_emotions:
        emotions.append(lines[0])
        # Open the "emotions.txt" file, write the emotion and a comma, then close the file
        f = open("emotions.txt", "a")
        f.write(lines[0] + ',')
        f.close()
    # Return the first emotion from the list
    return emotions[0]

# Define a function that uses the NRCLex to score text (similar to emo_score function)
def emo_score2(text):
    emo =  NRCLex(text)
    emotions = []
    for lines in emo.top_emotions:
        emotions.append(lines[0])
        f = open("emotions2.txt", "a")
        f.write(lines[0] + ',')
        f.close()
    return emotions[0]

# Create a user-defined function (UDF) for the vader_score function
vader_score_udf = udf(vader_score)
# Register the UDF in the SparkSession
spark.udf.register('vader_score', vader_score_udf)

# Create a UDF for the emo_score function
emo_score_udf = udf(emo_score)
# Register the UDF in the SparkSession
spark.udf.register('emo_score', emo_score_udf)
# Create a UDF for the emo_score2 function
emo_score_udf2 = udf(emo_score2)
# Register the UDF in the SparkSession
spark.udf.register('emo_score2', emo_score_udf2)


# Select the necessary columns from the DataFrame, including the timestamp, and create a new DataFrame
timestamped_tweet_sdf = tweet_sdf \
                .select(tweet_sdf.timestamp,
                        decode(tweet_sdf.value, 'utf-8').alias('json_str')) \
                .select('*', get_json_object('json_str', '$.data.id').alias('id')) \
                .select('*', get_json_object('json_str', '$.data.text').alias('text')) \
                .select('timestamp', 'id', 'text')

# Select the timestamp, id, text columns and add columns for the vader_score and emo_score
# (calculated using the previously defined UDFs) and set the watermark to 10 minutes
timestamped_sentiment_sdf = timestamped_tweet_sdf \
                .select('timestamp', 'id', 'text', vader_score_udf('text').alias('vader_score'), emo_score_udf('text').alias('emo_score')) \
                .withWatermark('timestamp', '10 minutes')

# If True (which is always the case), create a DataStreamWriter using the console as the output
# format, and set the trigger and output mode
if True:
    timestamped_joined_writer = timestamped_sentiment_sdf.writeStream.format('console').trigger(processingTime='10 seconds').outputMode('append')
    # Start the query and save it to a variable
    timestamped_joined_query = timestamped_joined_writer.start()

# Select the necessary columns from the second DataFrame, including the timestamp, and create a new DataFrame
timestamped_tweet_sdf2 = tweet_sdf2 \
                .select(tweet_sdf2.timestamp,
                        decode(tweet_sdf2.value, 'utf-8').alias('json_str')) \
                .select('*', get_json_object('json_str', '$.data.id').alias('id')) \
                .select('*', get_json_object('json_str', '$.data.text').alias('text')) \
                .select('timestamp', 'id', 'text')

# Select the timestamp, id, text columns and add columns for the vader_score and emo_score2
# (calculated using the previously defined UDFs) and set the watermark to 10 minutes
timestamped_sentiment_sdf2 = timestamped_tweet_sdf2 \
                .select('timestamp', 'id', 'text' , vader_score_udf('text').alias('vader_score2'), emo_score_udf2('text').alias('emo_score2')) \
                .withWatermark('timestamp', '10 minutes')

# If True (which is always the case), create a DataStreamWriter using the console as the output
# format, and set the trigger and output mode
if True:
    timestamped_joined_writer2 = timestamped_sentiment_sdf2.writeStream.format('console').trigger(processingTime='10 seconds').outputMode('append')
    # Start the query and save it to a variable
    timestamped_joined_query2 = timestamped_joined_writer2.start()


# Await the termination of the first query and check if it is None
# If not None, await the termination of the query
timestamped_joined_query is None \
    or timestamped_joined_query.awaitTermination()

# Await the termination of the second query and check if it is None
# If not None, await the termination of the query

timestamped_joined_query2 is None \
    or timestamped_joined_query.awaitTermination()

