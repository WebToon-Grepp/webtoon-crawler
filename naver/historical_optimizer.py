from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, explode
)
import boto3
from datetime import datetime

BUCKET = "wt-grepp-lake"
PLATFORM = "naver"
RAW = "s3a://{bucket}/historical/merged/{platform}/{target}"

def get_s3_file_paths(prefix):
    s3_client = boto3.client('s3')

    paginator = s3_client.get_paginator('list_objects_v2')
    response_iterator = paginator.paginate(Bucket=BUCKET, Prefix=prefix)

    file_paths = []
    for page in response_iterator:
        file_paths.extend([f"s3a://{BUCKET}/{item['Key']}" for item in page.get('Contents', [])])

    return file_paths


def optimize_many_files(spark, target, output_path):
    historical_files = get_s3_file_paths(f"historical/merged/{PLATFORM}/{target}")
    if len(historical_files) == 103:
        print(f"{target} has already been processed ({len(historical_files)} files found)")
        return

    date = datetime(2025, 2, 27).strftime("%Y/%m/%d")
    raw_files = get_s3_file_paths(f"raw/{PLATFORM}/{target}/{date}")
    print(f"Total {target} files to {len(raw_files)}")

    continue_count = int((len(historical_files) - 1) / 2)
    batch_size = int(len(raw_files) / 50)
    if len(raw_files) < 5000:
        batch_size = len(raw_files)

    for i in range(0, len(raw_files), batch_size):
        if continue_count > 0:
            continue_count -= 1
            continue

        batch_paths = raw_files[i:i+batch_size]
        df = spark.read.json(batch_paths, multiLine=True)
        
        df.coalesce(2).write.format("parquet").mode("append").save(output_path)
        print(f"Processing batch to {output_path} with {len(batch_paths)} files")


def get_comments(spark):
    url = RAW.format(bucket=BUCKET, platform=PLATFORM, target="comments")
    optimize_many_files(spark, "comments", url)
    return spark.read.parquet(url)


def get_episode_likes(spark):
    url = RAW.format(bucket=BUCKET, platform=PLATFORM, target="episode_likes")
    optimize_many_files(spark, "episode_likes", url)
    return spark.read.parquet(url)


def get_episode_info(spark):
    url = RAW.format(bucket=BUCKET, platform=PLATFORM, target="episode_info")
    optimize_many_files(spark, "episode_info", url)
    return spark.read.parquet(url)


def get_episodes(spark):
    url = RAW.format(bucket=BUCKET, platform=PLATFORM, target="episodes")
    optimize_many_files(spark, "episodes", url)
    df = spark.read.parquet(url)
    return df.select(
            explode(col("articleList")).alias("article"), 
            col("titleId").alias("title_id")
        )


def get_title_info(spark):
    url = RAW.format(bucket=BUCKET, platform=PLATFORM, target="title_info")
    optimize_many_files(spark, "title_info", url)
    return spark.read.parquet(url, multiLine=True)


def get_finished_titles(spark):
    url = RAW.format(bucket=BUCKET, platform=PLATFORM, target="finished_titles")
    optimize_many_files(spark, "finished_titles", url)
    df = spark.read.parquet(url, multiLine=True)
    return df.select(
            explode(col(f"titleList")).alias("title"),
            lit("FINISHED").alias("weekday_str")
        ) 


def get_titles(spark):
    days = ["MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY", "SUNDAY"]

    url = RAW.format(bucket=BUCKET, platform=PLATFORM, target="titles")
    optimize_many_files(spark, "titles", url)
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


def save_to_parquet(df, target):
    path = f"s3a://{BUCKET}/historical/optimized/{target}/platform={PLATFORM}"
    df.coalesce(10).write.format("parquet").mode("append").save(path)
    print(f"Data successfully optimized to {path}")


def create_spark_session():
    return SparkSession.builder \
        .appName(f"S3 {PLATFORM} Data Optimizer") \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()


def run_spark():
    spark = create_spark_session()

    titles_df = get_titles(spark)
    save_to_parquet(titles_df, "titles")

    finished_titles_df = get_finished_titles(spark)
    save_to_parquet(finished_titles_df, "finished_titles")

    title_info_df = get_title_info(spark)
    save_to_parquet(title_info_df, "title_info")

    episodes_df = get_episodes(spark)
    save_to_parquet(episodes_df, "episodes")
    
    episode_info_df = get_episode_info(spark)
    save_to_parquet(episode_info_df, "episode_info")
    
    episode_likes_df = get_episode_likes(spark)
    save_to_parquet(episode_likes_df, "episode_likes")

    comments_df = get_comments(spark)
    save_to_parquet(comments_df, "comments")


run_spark()