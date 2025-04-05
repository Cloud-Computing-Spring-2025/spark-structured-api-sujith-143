from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, desc, lit, when, rank, date_format, hour
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder.appName("MusicStreamingAnalysis").getOrCreate()

# Load datasets
listening_logs = spark.read.option("header", True).csv("input/listening_logs.csv")
songs_metadata = spark.read.option("header", True).csv("input/songs_metadata.csv")

# Convert necessary columns to correct types
listening_logs = listening_logs.withColumn("user_id", col("user_id").cast("int")) \
                               .withColumn("duration_sec", col("duration_sec").cast("int")) \
                               .withColumn("timestamp", col("timestamp").cast("timestamp"))

# Join listening logs with song metadata
enriched_logs = listening_logs.join(songs_metadata, "song_id", "left")

# Save enriched logs
enriched_logs.write.format("csv").mode("overwrite").save("output/enriched_logs/")

# 1. Find each user’s favorite genre
user_genre_count = enriched_logs.groupBy("user_id", "genre").agg(count("song_id").alias("play_count"))
window_spec = Window.partitionBy("user_id").orderBy(col("play_count").desc())

user_fav_genre = user_genre_count.withColumn("rank", rank().over(window_spec)) \
                                 .filter(col("rank") == 1) \
                                 .drop("rank")

user_fav_genre.write.format("csv").mode("overwrite").save("output/user_favorite_genres/")

# 2. Calculate average listen time per song
avg_listen_time = enriched_logs.groupBy("song_id").agg(avg("duration_sec").alias("avg_duration_sec"))

avg_listen_time.write.format("csv").mode("overwrite").save("output/avg_listen_time_per_song/")

# 3. List the top 10 most played songs this week
current_week_logs = enriched_logs.filter(date_format("timestamp", "yyyy-MM-dd") >= "2025-03-17")
top_songs_week = current_week_logs.groupBy("song_id").agg(count("song_id").alias("play_count")) \
                                  .join(songs_metadata, "song_id", "left") \
                                  .orderBy(desc("play_count")) \
                                  .limit(10)

top_songs_week.write.format("csv").mode("overwrite").save("output/top_songs_this_week/")

# 4. Recommend “Happy” songs to users who mostly listen to “Sad” songs
user_sad_count = enriched_logs.filter(col("mood") == "Sad").groupBy("user_id").agg(count("song_id").alias("sad_count"))
user_total_count = enriched_logs.groupBy("user_id").agg(count("song_id").alias("total_count"))

user_mood_preference = user_sad_count.join(user_total_count, "user_id") \
                                     .withColumn("sad_ratio", col("sad_count") / col("total_count")) \
                                     .filter(col("sad_ratio") > 0.5)

happy_songs = songs_metadata.filter(col("mood") == "Happy").select("song_id", "title", "artist")

recommendations = user_mood_preference.join(happy_songs, how="cross").limit(3 * user_mood_preference.count())

recommendations.write.format("csv").mode("overwrite").save("output/happy_recommendations/")

# 5. Compute genre loyalty score for each user
user_genre_total = enriched_logs.groupBy("user_id", "genre").agg(count("song_id").alias("play_count"))
user_total_plays = enriched_logs.groupBy("user_id").agg(count("song_id").alias("total_plays"))

user_genre_loyalty = user_genre_total.join(user_total_plays, "user_id") \
                                     .withColumn("loyalty_score", col("play_count") / col("total_plays")) \
                                     .filter(col("loyalty_score") > 0.8)

user_genre_loyalty.write.format("csv").mode("overwrite").save("output/genre_loyalty_scores/")

# 6. Identify night owl users (listening between 12 AM and 5 AM)
night_owl_users = enriched_logs.withColumn("hour", hour(col("timestamp"))) \
                               .filter((col("hour") >= 0) & (col("hour") < 5)) \
                               .select("user_id").distinct()

night_owl_users.write.format("csv").mode("overwrite").save("output/night_owl_users/")

print("All tasks completed. Outputs saved in 'output/' folder.")

# Stop Spark Session
spark.stop()
