from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, explode, split, 
    to_date, from_unixtime
)
from datetime import datetime, timedelta

BUCKET = "wt-grepp-lake"
PLATFORM = "naver"
RAW = "s3a://{bucket}/historical/optimized/{target}/platform={platform}/"


def read_to_parquet(spark, target):
    url = RAW.format(bucket=BUCKET, target=target, platform=PLATFORM)
    df = spark.read.parquet(url)
    print(f"Data successfully read to {url}")

    if target == "titles" or target == "finished_titles":
        return df.select(
            lit(PLATFORM).alias("platform"),
            col("title.titleId").alias("id"), 
            col("title.titleName").alias("title"), 
            col("title.author").alias("author"),
            col("title.viewCount").alias("views"),
            col("title.thumbnailUrl").alias("image_url"), 
            col("weekday_str"),
            lit((target == "finished_titles")).alias("is_completed")
        )
    elif target == "title_info":
        return df.select(
            lit(PLATFORM).alias("platform"),
            col("gfpAdCustomParam.titleId").alias("title_id"),
            explode("gfpAdCustomParam.tags").alias("genre_name")
        )
    elif target == "episodes":
        return df.select(
            lit(PLATFORM).alias("platform"),
            col("title_id"),
            col("article.no").alias("id"),
            col("article.subtitle").alias("title"),
            col("article.thumbnailUrl").alias("image_url"),
            col("article.serviceDateDescription").alias("start_date")
        )
    elif target == "episode_likes":
        return df.select(
            col("contents").getItem(0).alias("content")
        ).select(
            explode(col("content.reactions")).alias("reaction"),
            col("content.contentsId").alias("contentsId")
        ).select(
            lit(PLATFORM).alias("platform"),
            split(col("contentsId"), "_").getItem(0).alias("title_id"),
            split(col("contentsId"), "_").getItem(1).alias("id"),
            col("reaction.count").alias("likes")
        )
    elif target == "comments":
        return df.select(
            col("result.commentList").getItem(0).alias("comment"), 
            col("result.count.total").alias("comments")
        ).select(
            lit(PLATFORM).alias("platform"),
            split(col("comment.objectId"), "_").getItem(0).alias("title_id"),
            split(col("comment.objectId"), "_").getItem(1).alias("id"),
            col("comments")
        )

    return df


def save_to_parquet(df, date, target):
    path = f"s3a://{BUCKET}/processed/{target}/{date}"
    df.write.partitionBy("platform").format("parquet").mode("append").save(path)
    print(f"Data successfully saved to {path}")


def convert_weekday(df):
    return df.withColumn("release_day", 
                         when(col("weekday_str") == "MONDAY", 0)
                         .when(col("weekday_str") == "TUESDAY", 1)
                         .when(col("weekday_str") == "WEDNESDAY", 2)
                         .when(col("weekday_str") == "THURSDAY", 3)
                         .when(col("weekday_str") == "FRIDAY", 4)
                         .when(col("weekday_str") == "SATURDAY", 5)
                         .when(col("weekday_str") == "SUNDAY", 6)
                         .otherwise(7)) 


def convert_timestamp(df):
    return df.withColumn("updated_date", to_date(col("start_date"), "yy.MM.dd"))


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

    titles_df = spark.sql(f"""
        SELECT DISTINCT 
            platform, id, title, author, views, image_url, release_day, is_completed
        FROM combined_titles
    """)
    genres_df = spark.sql(f"""
        SELECT DISTINCT 
            platform, title_id, genre_name 
        FROM title_info_table
    """)

    save_to_parquet(titles_df, date, "titles")
    save_to_parquet(genres_df, date, "genres")


def convert_episodes(spark, episodes, episode_likes, comments, date):
    episodes.createOrReplaceTempView("episodes_table")
    episode_likes.createOrReplaceTempView("episode_likes_table")
    comments.createOrReplaceTempView("comments_table")

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW joined_episodes AS
        SELECT e.*, el.likes, c.comments
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

    save_to_parquet(joined_episodes_df,  date, "episodes")


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
