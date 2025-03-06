from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, split, input_file_name
)
from datetime import datetime

NOW = datetime.now()

BUCKET = "wt-grepp-lake"
PLATFORM = "kakao"
RAW = "s3a://{bucket}/raw/{platform}/{target}/{target_date}"


def get_comments(spark, date):
    url = RAW.format(bucket=BUCKET, platform=PLATFORM, target="comments", target_date=date)
    df = spark.read.json(f"{url}/*/*.json", multiLine=True)
    return df.withColumn("filename", input_file_name()) \
             .withColumn("title_id", split(col("filename"), "/").getItem(9)) \
             .withColumn("episode_id", split(col("filename"), "/").getItem(10))


def get_episode_likes(spark, date):
    url = RAW.format(bucket=BUCKET, platform=PLATFORM, target="episode_likes", target_date=date)
    df = spark.read.json(f"{url}/*/*.json", multiLine=True)
    return df.withColumn("filename", input_file_name())


def get_episodes(spark, date):
    url = RAW.format(bucket=BUCKET, platform=PLATFORM, target="episodes", target_date=date)
    df = spark.read.json(f"{url}/*/*.json", multiLine=True)
    return df.select(explode(col("data.episodes")).alias("episodes"))


def get_title_info(spark, date):
    url = RAW.format(bucket=BUCKET, platform=PLATFORM, target="title_info", target_date=date)
    return spark.read.json(f"{url}/*.json", multiLine=True)


def get_titles(spark, date):
    url = RAW.format(bucket=BUCKET, platform=PLATFORM, target="titles", target_date=date)
    df = spark.read.json(f"{url}/*.json", multiLine=True)
    return df.select(explode(col("data")).alias("data"))


def save_to_parquet(df, target):
    date_str = NOW.strftime("year=%Y/month=%m/day=%d")
    path = f"s3a://wt-grepp-lake/optimized/{target}/{date_str}/platform={PLATFORM}"
    df.coalesce(50).write.format("parquet").mode("append").save(path)
    print(f"Data successfully optimized to {path}")


def create_spark_session():
    return SparkSession.builder \
        .appName(f"S3 {PLATFORM} Data Optimizer") \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()


def run():
    spark = create_spark_session()
    date_str = NOW.strftime("%Y/%m/%d")
    
    titles_df = get_titles(spark, date_str)
    save_to_parquet(titles_df, "titles")

    title_info_df = get_title_info(spark, date_str)
    save_to_parquet(title_info_df, "title_info")

    episodes_df = get_episodes(spark, date_str)
    save_to_parquet(episodes_df, "episodes")

    episode_likes_df = get_episode_likes(spark, date_str)
    save_to_parquet(episode_likes_df, "episode_likes")

    comments_df = get_comments(spark, date_str)
    save_to_parquet(comments_df, "comments")


run()
