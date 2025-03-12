from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, explode
)
import boto3
from datetime import datetime

BUCKET = "wt-grepp-lake"
PLATFORM = "naver"
RAW = "s3a://{bucket}/raw/{platform}/{target}/{target_date}/temp"


def optimize_many_files(spark, target, date, output_path):
    s3_client = boto3.client('s3')
    
    paginator = s3_client.get_paginator('list_objects_v2')
    response_iterator = paginator.paginate(
        Bucket=BUCKET,
        Prefix=f"raw/{PLATFORM}/{target}/{date}"
    )

    file_paths = []
    for page in response_iterator:
        file_paths.extend([f"s3a://{BUCKET}/{item['Key']}" for item in page.get('Contents', [])])

    print(f"Total {target} files to {len(file_paths)}")
    
    batch_size = int(len(file_paths) / 50)
    if batch_size < 1:
        batch_size = len(file_paths)
    for i in range(0, len(file_paths), batch_size):
        batch_paths = file_paths[i:i+batch_size]
        df = spark.read.json(batch_paths, multiLine=True)
        
        df.coalesce(2).write.format("parquet").mode("append").save(output_path)
        print(f"Processing batch to {output_path} with {len(batch_paths)} files")


def get_comments(spark, date):
    url = RAW.format(bucket=BUCKET, platform=PLATFORM, target="comments", target_date=date)
    optimize_many_files(spark, "comments", date, url)
    return spark.read.parquet(url)


def get_episode_likes(spark, date):
    url = RAW.format(bucket=BUCKET, platform=PLATFORM, target="episode_likes", target_date=date)
    optimize_many_files(spark, "episode_likes", date, url)
    return spark.read.parquet(url)


def get_episode_info(spark, date):
    url = RAW.format(bucket=BUCKET, platform=PLATFORM, target="episode_info", target_date=date)
    optimize_many_files(spark, "episode_info", date, url)
    return spark.read.parquet(url)


def get_episodes(spark, date):
    url = RAW.format(bucket=BUCKET, platform=PLATFORM, target="episodes", target_date=date)
    optimize_many_files(spark, "episodes", date, url)
    df = spark.read.parquet(url)
    return df.select(
            explode(col("articleList")).alias("article"), 
            col("titleId").alias("title_id")
        )


def get_title_info(spark, date):
    url = RAW.format(bucket=BUCKET, platform=PLATFORM, target="title_info", target_date=date)
    optimize_many_files(spark, "title_info", date, url)
    return spark.read.parquet(url)


def get_finished_titles(spark, date):
    url = RAW.format(bucket=BUCKET, platform=PLATFORM, target="finished_titles", target_date=date)
    optimize_many_files(spark, "finished_titles", date, url)
    df = spark.read.parquet(url)
    return df.select(explode(col(f"titleList")).alias("title"))


def get_titles(spark, date):
    days = ["MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY", "SUNDAY"]

    url = RAW.format(bucket=BUCKET, platform=PLATFORM, target="titles", target_date=date)
    optimize_many_files(spark, "finished_titles", date, url)
    df = spark.read.parquet(url)
    
    df_exploded = None
    for day in days:
        exploded_day = df.select(
            explode(col(f"titleListMap.{day}")).alias("title"),
            lit(day).alias("weekday_str")
        )
        if df_exploded is None:
            df_exploded = exploded_day
        else:
            df_exploded = df_exploded.union(exploded_day)

    return df_exploded


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

    # titles_df = get_titles(spark, date_get)
    # save_to_parquet(titles_df, date_save, "titles")

    # finished_titles_df = get_finished_titles(spark, date_get)
    # save_to_parquet(finished_titles_df, date_save, "finished_titles")

    # title_info_df = get_title_info(spark, date_get)
    # save_to_parquet(title_info_df, date_save, "title_info")

    # episodes_df = get_episodes(spark, date_get)
    # save_to_parquet(episodes_df, date_save, "episodes")
    
    episode_info_df = get_episode_info(spark, date_get)
    save_to_parquet(episode_info_df, date_save, "episode_info")
    
    episode_likes_df = get_episode_likes(spark, date_get)
    save_to_parquet(episode_likes_df, date_save, "episode_likes")

    comments_df = get_comments(spark, date_get)
    save_to_parquet(comments_df, date_save, "comments")


run_spark(datetime(2025, 2, 27))