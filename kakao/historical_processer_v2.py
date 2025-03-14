from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, explode, concat_ws, split, 
    to_date, from_utc_timestamp
)
from datetime import datetime, timedelta

BUCKET = "wt-grepp-lake"
PLATFORM = "kakao"
RAW = "s3a://{bucket}/historical/optimized/{target}/platform={platform}/"


def read_to_parquet(spark, target):
    url = RAW.format(bucket=BUCKET, target=target, platform=PLATFORM)
    df = spark.read.parquet(url)
    print(f"Data successfully read to {url}")

    if target == "titles" or target == "finished_titles":
        return df.select(
            explode(col("data.cardGroups")).alias("cardGroups"), 
            col("data.title").alias("weekday_str")
        ).select(explode(col("cardGroups.cards")).alias("cards"), col("weekday_str")) \
        .select(col("cards.content").alias("content"), col("weekday_str")) \
        .select(
            lit(PLATFORM).alias("platform"),
            col("content.id").alias("id"), 
            col("content.title").alias("title"), 
            concat_ws(" / ", col("content.authors.name")).alias("author"),
            col("content.featuredCharacterImageA").alias("image_url"), 
            col("weekday_str"),
            lit((target == "finished_titles")).alias("is_completed"),
            explode("content.seoKeywords").alias("genre_name")
        )
    elif target == "title_info":
        return df.select(
            lit(PLATFORM).alias("platform"),
            col("data.id").alias("id"),
            col("data.statistics.viewCount").alias("views")
        )
    elif target == "episodes":
        return df.select(
            lit(PLATFORM).alias("platform"),
            col("episodes.contentId").alias("title_id"),
            col("episodes.id").alias("id"),
            col("episodes.title").alias("title"),
            col("episodes.asset.thumbnailImage").alias("image_url"),
            col("episodes.serialStartDateTime").alias("start_date")
        )
    elif target == "episode_likes":
        return df.select(
            lit(PLATFORM).alias("platform"),
            split(col("filename"), "/").getItem(9).alias("title_id"),
            col("data.episodeId").alias("id"),
            col("data.likeCount").alias("likes")
        )
    elif target == "comments":
        return df.select(
            lit(PLATFORM).alias("platform"),
            col("title_id"),
            split(col("episode_id"), "\.").getItem(0).alias("id"),
            col("meta.pagination.totalCount").alias("comments")
        )

    return df


def save_to_parquet(df, date, target):
    path = f"s3a://{BUCKET}/processed/{target}/{date}"
    df.write.partitionBy("platform").format("parquet").mode("append").save(path)
    print(f"Data successfully saved to {path}")


def convert_weekday(df):
    return df.withColumn("release_day", 
                         when(col("weekday_str") == "월", 0)
                         .when(col("weekday_str") == "화", 1)
                         .when(col("weekday_str") == "수", 2)
                         .when(col("weekday_str") == "목", 3)
                         .when(col("weekday_str") == "금", 4)
                         .when(col("weekday_str") == "토", 5)
                         .when(col("weekday_str") == "일", 6)
                         .otherwise(7)) 


def convert_timestamp(df):
    return df.withColumn("timestamp", from_utc_timestamp(col("start_date"), "UTC")) \
             .withColumn("updated_date", to_date(col("timestamp")))


def convert_titles(spark, titles, finished_titles, title_info, date):
    titles.createOrReplaceTempView("titles_table")
    finished_titles.createOrReplaceTempView("finished_titles_table")
    title_info.createOrReplaceTempView("title_info_table")

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW combined_titles AS
        SELECT * FROM titles_table
        UNION ALL
        SELECT * FROM finished_titles_table
    """)

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW joined_titles AS
        SELECT ct.*, ti.views
        FROM combined_titles ct
        LEFT JOIN title_info_table ti
            ON ct.platform = ti.platform
            AND ct.id = ti.id
    """)
    
    joined_titles_df = spark.sql(f"""
        SELECT DISTINCT 
            platform, id, title, author, views, image_url, release_day, is_completed
        FROM joined_titles
    """)
    joined_genres_df = spark.sql(f"""
        SELECT DISTINCT 
            platform, id AS title_id, SUBSTRING(genre_name, 2) AS genre_name
        FROM joined_titles
    """)

    save_to_parquet(joined_titles_df, date, "titles")
    save_to_parquet(joined_genres_df, date, "genres")


def convert_episodes(spark, episodes, episode_likes, comments, date):
    episodes.createOrReplaceTempView("episodes_table")
    episode_likes.createOrReplaceTempView("episode_likes_table")
    comments.createOrReplaceTempView("comments_table")

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW joined_episodes AS
        SELECT e.*, c.comments, el.likes
        FROM episodes_table e
        LEFT JOIN episode_likes_table el
            ON e.platform = el.platform
            AND e.title_id = el.title_id
            AND e.id = el.id
        LEFT JOIN comments_table c
            ON el.platform = c.platform
            AND el.title_id = c.title_id
            AND el.id = c.id
    """)

    joined_episodes_df = spark.sql(f"""
        SELECT DISTINCT 
            platform, title_id, id, title, likes, comments, image_url, updated_date
        FROM joined_episodes
        WHERE updated_date IS NOT NULL
    """)

    save_to_parquet(joined_episodes_df, date, "episodes")


def create_spark_session():
    return SparkSession.builder \
        .appName(f"S3 {PLATFORM} Data Processer") \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()


def run_spark(target):
    spark = create_spark_session()
    date_str = target.strftime("year=%Y/month=%m/day=%d")
    
    print("Data processing to titles and genres")
    titles_df = read_to_parquet(spark, "titles")
    titles_df = convert_weekday(titles_df)

    finished_titles_df = read_to_parquet(spark, "finished_titles")
    finished_titles_df = convert_weekday(finished_titles_df)

    title_info_df = read_to_parquet(spark, "title_info")
    convert_titles(spark, titles_df, finished_titles_df, title_info_df, date_str)

    print("Data processing to episodes")
    episodes_df = read_to_parquet(spark, "episodes")
    episodes_df = convert_timestamp(episodes_df)
    
    episode_likes_df = read_to_parquet(spark, "episode_likes")
    comments_df = read_to_parquet(spark, "comments")
    convert_episodes(spark, episodes_df, episode_likes_df, comments_df, date_str)


run_spark(datetime(2025, 2, 27))
