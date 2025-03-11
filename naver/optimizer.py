from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, explode
)
from datetime import datetime, timedelta

BUCKET = "wt-grepp-lake"
PLATFORM = "naver"
RAW = "s3a://{bucket}/raw/{platform}/{target}/{target_date}"


def get_comments(spark, date):
    url = RAW.format(bucket=BUCKET, platform=PLATFORM, target="comments", target_date=date)
    return spark.read.json(f"{url}/*/*.json", multiLine=True)


def get_episode_likes(spark, date):
    url = RAW.format(bucket=BUCKET, platform=PLATFORM, target="episode_likes", target_date=date)
    return spark.read.json(f"{url}/*/*.json", multiLine=True)


def get_episode_info(spark, date):
    url = RAW.format(bucket=BUCKET, platform=PLATFORM, target="episode_info", target_date=date)
    return spark.read.json(f"{url}/*/*.json", multiLine=True)


def get_episodes(spark, date):
    url = RAW.format(bucket=BUCKET, platform=PLATFORM, target="episodes", target_date=date)
    df = spark.read.json(f"{url}/*/*.json", multiLine=True)
    return df.select(
            explode(col("articleList")).alias("article"), 
            col("titleId").alias("title_id")
        )


def get_title_info(spark, date):
    url = RAW.format(bucket=BUCKET, platform=PLATFORM, target="title_info", target_date=date)
    return spark.read.json(f"{url}/*.json", multiLine=True)


def get_titles(spark, date, dayInt):
    days = ["MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY", "SUNDAY"]

    url = RAW.format(bucket=BUCKET, platform=PLATFORM, target="titles", target_date=date)
    df = spark.read.json(f"{url}/*.json", multiLine=True)
    return df.select(
        explode(col(f"titleListMap.{days[dayInt]}")).alias("title"),
        lit(days[dayInt]).alias("weekday_str")
    )


def save_to_parquet(df, date, target):
    path = f"s3a://{BUCKET}/optimized/{target}/{date}/platform={PLATFORM}"
    df.coalesce(50).write.format("parquet").mode("append").save(path)
    print(f"Data successfully optimized to {path}")


def create_spark_session():
    return SparkSession.builder \
        .appName(f"S3 {PLATFORM} Data Optimizer") \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()


def run_spark(target):
    spark = create_spark_session()
    date_get = target.strftime("%Y/%m/%d")
    date_save = target.strftime("year=%Y/month=%m/day=%d")
    dayInt = target.weekday()

    titles_df = get_titles(spark, date_get, dayInt)
    save_to_parquet(titles_df, date_save, "titles")

    title_info_df = get_title_info(spark, date_get)
    save_to_parquet(title_info_df, date_save, "title_info")

    episodes_df = get_episodes(spark, date_get)
    save_to_parquet(episodes_df, date_save, "episodes")
    
    episode_info_df = get_episode_info(spark, date_get)
    save_to_parquet(episode_info_df, date_save, "episode_info")
    
    episode_likes_df = get_episode_likes(spark, date_get)
    save_to_parquet(episode_likes_df, date_save, "episode_likes")

    comments_df = get_comments(spark, date_get)
    save_to_parquet(comments_df, date_save, "comments")


def run_until_today():
    start_date = datetime(2025, 2, 28) # 프로젝트 시작 날짜
    end_date = datetime.today() 

    current_date = start_date
    while current_date < end_date:
        run_spark(current_date)
        current_date += timedelta(days=1) 


run_spark(datetime.now())