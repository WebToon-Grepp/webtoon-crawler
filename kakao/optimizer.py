from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, explode, concat_ws, split, 
    to_date, from_utc_timestamp, input_file_name
)
from datetime import datetime

NOW = datetime.now()

BUCKET = "wt-grepp-lake"
PLATFORM = "kakao"
RAW = "s3a://{bucket}/raw/{platform}/{target}/{target_date}"


def get_comments(spark, date):
    url = RAW.format(bucket=BUCKET, platform=PLATFORM, target="comments", target_date=date)
    df = spark.read.json(f"{url}/*/*.json", multiLine=True)
    df = df.withColumn("filename", input_file_name()) \
           .withColumn("title_id", split(col("filename"), "/").getItem(9)) \
           .withColumn("episode_id", split(col("filename"), "/").getItem(10))
    return df.select(
            lit(PLATFORM).alias("platform"),
            col("title_id"),
            split(col("episode_id"), "\.").getItem(0).alias("id"),
            col("meta.pagination.totalCount").alias("comments")
        )


def get_episode_likes(spark, date):
    url = RAW.format(bucket=BUCKET, platform=PLATFORM, target="episode_likes", target_date=date)
    df = spark.read.json(f"{url}/*/*.json", multiLine=True)
    df = df.withColumn("filename", input_file_name())
    return df.select(
            lit(PLATFORM).alias("platform"),
            split(col("filename"), "/").getItem(9).alias("title_id"),
            col("data.episodeId").alias("id"),
            col("data.likeCount").alias("likes")
        )


def get_episodes(spark, date):
    url = RAW.format(bucket=BUCKET, platform=PLATFORM, target="episodes", target_date=date)
    df = spark.read.json(f"{url}/*/*.json", multiLine=True)
    return df.select(explode(col("data.episodes")).alias("episodes")) \
        .select(
            lit(PLATFORM).alias("platform"),
            col("episodes.contentId").alias("title_id"),
            col("episodes.id").alias("id"),
            col("episodes.title").alias("title"),
            col("episodes.asset.thumbnailImage").alias("image_url"),
            col("episodes.useStartDateTime").alias("start_date")
        )


def get_title_info(spark, date):
    url = RAW.format(bucket=BUCKET, platform=PLATFORM, target="title_info", target_date=date)
    df = spark.read.json(f"{url}/*.json", multiLine=True)
    return df.select(
            lit(PLATFORM).alias("platform"),
            col("data.id").alias("id"),
            col("data.statistics.viewCount").alias("views")
        )


def get_titles(spark, date):
    url = RAW.format(bucket=BUCKET, platform=PLATFORM, target="titles", target_date=date)
    df = spark.read.json(f"{url}/*.json", multiLine=True)
    return df.select(explode(col("data")).alias("data")) \
        .select(
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
            lit(False).alias("is_completed"),
            explode("content.seoKeywords").alias("genre_name")
        )


def save_to_parquet(df, target):
    date_str = NOW.strftime("year=%Y/month=%m/day=%d")
    path = f"s3a://wt-grepp-lake/optimized/{target}/{date_str}"
    df.coalesce(50).write.partitionBy("platform").format("parquet").mode("append").save(path)
    print(f"Data successfully optimized to {path}")


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


def create_spark_session():
    return SparkSession.builder \
        .appName("S3 Data Optimizer") \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()


def run():
    spark = create_spark_session()
    date_str = NOW.strftime("%Y/%m/%d")
    
    titles_df = get_titles(spark, date_str)
    titles_df = convert_weekday(titles_df)
    title_info_df = get_title_info(spark, date_str)

    episodes_df = get_episodes(spark, date_str)
    episodes_df = convert_timestamp(episodes_df)
    episode_likes_df = get_episode_likes(spark, date_str)
    comments_df = get_comments(spark, date_str)

    save_to_parquet(titles_df, "titles")
    save_to_parquet(title_info_df, "title_info")

    save_to_parquet(episodes_df, "episodes")
    save_to_parquet(episode_likes_df, "episode_likes")
    save_to_parquet(comments_df, "comments")


run()
