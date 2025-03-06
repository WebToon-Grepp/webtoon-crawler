from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, explode, split, 
    to_date, from_unixtime
)
from datetime import datetime

NOW = datetime.now()

BUCKET = "wt-grepp-lake"
PLATFORM = "naver"
RAW = "s3a://{bucket}/raw/{platform}/{target}/{target_date}"


def get_comments(spark, date):
    url = RAW.format(bucket=BUCKET, platform=PLATFORM, target="comments", target_date=date)
    df = spark.read.json(f"{url}/*/*.json", multiLine=True)
    return df.select(
            col("result.commentList").getItem(0).alias("comment"), 
            col("result.count.total").alias("comments")
        ).select(
            lit(PLATFORM).alias("platform"),
            split(col("comment.objectId"), "_").getItem(0).alias("title_id"),
            split(col("comment.objectId"), "_").getItem(1).alias("id"),
            col("comments")
        )


def get_episode_likes(spark, date):
    url = RAW.format(bucket=BUCKET, platform=PLATFORM, target="episode_likes", target_date=date)
    df = spark.read.json(f"{url}/*/*.json", multiLine=True)
    return df.select(
            col("contents").getItem(0).alias("content"),
            col("timestamp").alias("start_date")
        ).select(
            explode(col("content.reactions")).alias("reaction"),
            col("content.contentsId").alias("contentsId"),
            col("start_date")
        ).select(
            lit(PLATFORM).alias("platform"),
            split(col("contentsId"), "_").getItem(0).alias("title_id"),
            split(col("contentsId"), "_").getItem(1).alias("id"),
            col("reaction.count").alias("likes"),
            col("start_date")
        )


def get_episodes(spark, date):
    url = RAW.format(bucket=BUCKET, platform=PLATFORM, target="episodes", target_date=date)
    df = spark.read.json(f"{url}/*/*.json", multiLine=True)
    return df.select(
            explode(col("articleList")).alias("article"), 
            col("titleId").alias("title_id")
        ).select(
            lit(PLATFORM).alias("platform"),
            col("title_id"),
            col("article.no").alias("id"),
            col("article.subtitle").alias("title"),
            col("article.thumbnailUrl").alias("image_url")
        )


def get_title_info(spark, date):
    url = RAW.format(bucket=BUCKET, platform=PLATFORM, target="title_info", target_date=date)
    df = spark.read.json(f"{url}/*.json", multiLine=True)
    return df.select(
            lit(PLATFORM).alias("platform"),
            col("gfpAdCustomParam.titleId").alias("title_id"),
            explode("gfpAdCustomParam.tags").alias("genre_name")
        )


def get_titles(spark, date, dayInt):
    days = ["MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY", "SUNDAY"]

    url = RAW.format(bucket=BUCKET, platform=PLATFORM, target="titles", target_date=date)
    df = spark.read.json(f"{url}/*.json", multiLine=True)
    return df.select(explode(col(f"titleListMap.{days[dayInt]}")).alias("title")) \
        .select(
            lit(PLATFORM).alias("platform"),
            col("title.titleId").alias("id"), 
            col("title.titleName").alias("title"), 
            col("title.author").alias("author"),
            col("title.viewCount").alias("views"),
            col("title.thumbnailUrl").alias("image_url"), 
            lit(days[dayInt]).alias("weekday_str"),
            lit(False).alias("is_completed")
        )


def save_to_parquet(df, target):
    date_str = NOW.strftime("year=%Y/month=%m/day=%d")
    path = f"s3a://wt-grepp-lake/optimized/{target}/{date_str}"
    df.coalesce(50).write.partitionBy("platform").format("parquet").mode("append").save(path)
    print(f"Data successfully optimized to {path}")


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
    return df.withColumn("timestamp", from_unixtime(col("start_date") / 1000)) \
             .withColumn("updated_date", to_date(col("timestamp")))


def create_spark_session():
    return SparkSession.builder \
        .appName("S3 Data Reader") \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()


def run():
    spark = create_spark_session()
    date_str = NOW.strftime("%Y/%m/%d")
    dayInt = NOW.weekday()

    titles_df = get_titles(spark, date_str, dayInt)
    titles_df = convert_weekday(titles_df)
    title_info_df = get_title_info(spark, date_str)

    episodes_df = get_episodes(spark, date_str)
    episode_likes_df = get_episode_likes(spark, date_str)
    episode_likes_df = convert_timestamp(episode_likes_df)
    comments_df = get_comments(spark, date_str)

    save_to_parquet(titles_df, "titles")
    save_to_parquet(title_info_df, "title_info")

    save_to_parquet(episodes_df, "episodes")
    save_to_parquet(episode_likes_df, "episode_likes")
    save_to_parquet(comments_df, "comments")


run()
