from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, explode, concat_ws, split, 
    to_date, from_utc_timestamp, input_file_name, expr
)
from datetime import datetime

TIME = datetime(2025, 2, 27)

BUCKET = "wt-grepp-lake"
PLATFORM = "kakao"
RAW = "s3a://{bucket}/raw/{platform}/{target}/{target_date}"

BACKUP = False
SHOW = False

# target_folders 입력
target_folders = [ ]


def get_comments(spark, date):
    paths = [RAW.format(bucket=BUCKET, platform=PLATFORM, target="comments", target_date=date) + f"/{folder}/*.json" for folder in target_folders]
    df = spark.read.json(paths, multiLine=True)

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
    paths = [RAW.format(bucket=BUCKET, platform=PLATFORM, target="episode_likes", target_date=date) + f"/{folder}/*.json" for folder in target_folders]
    df = spark.read.json(paths, multiLine=True)

    df = df.withColumn("filename", input_file_name())
    return df.select(
            lit(PLATFORM).alias("platform"),
            split(col("filename"), "/").getItem(9).alias("title_id"),
            col("data.episodeId").alias("id"),
            col("data.likeCount").alias("likes")
        )


def get_episodes(spark, date):
    paths = [RAW.format(bucket=BUCKET, platform=PLATFORM, target="episodes", target_date=date) + f"/{folder}/*.json" for folder in target_folders]
    df = spark.read.json(paths, multiLine=True)

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
            col("data.statistics.viewCount").alias("views"),
            explode(expr("filter(data.badges, badge -> badge.type == 'WEEKDAYS')")).alias("badge")
        ).select(
            "platform", "id", "views",
            col("badge.title").alias("weekday")
        )


def get_finished_titles(spark, date):
    url = RAW.format(bucket=BUCKET, platform=PLATFORM, target="finished_titles", target_date=date)
    df = spark.read.json(f"{url}/*.json", multiLine=True)
    return df.select(explode(col("data")).alias("data")) \
        .select(
            explode(col("data.cardGroups")).alias("cardGroups")
        ).select(explode(col("cardGroups.cards")).alias("cards")) \
        .select(col("cards.content").alias("content")) \
        .select(
            lit(PLATFORM).alias("platform"),
            col("content.id").alias("id"), 
            col("content.title").alias("title"), 
            concat_ws(" / ", col("content.authors.name")).alias("author"),
            col("content.featuredCharacterImageA").alias("image_url"),
            lit(True).alias("is_completed"),
            explode("content.seoKeywords").alias("genre_name")
        )


def save_to_parquet(df, target):
    date_str = TIME.strftime("year=%Y/month=%m/day=%d")
    path = f"s3a://wt-grepp-lake/processed/{target}/{date_str}"
    df.write.partitionBy("platform").format("parquet").mode("append").save(path)
    print(f"Data successfully saved to {path}")


def backup_to_parquet(df, target):
    path = f"s3a://wt-grepp-lake/temp/{PLATFORM}/{target}"
    df.coalesce(50).write.format("parquet").mode("overwrite").save(path)
    print(f"Data successfully backup to {path}")


def convert_weekday(df):
    return df.withColumn("release_day", 
                         when(col("weekday") == "mon", 0)
                         .when(col("weekday") == "tue", 1)
                         .when(col("weekday") == "wed", 2)
                         .when(col("weekday") == "thu", 3)
                         .when(col("weekday") == "fri", 4)
                         .when(col("weekday") == "sat", 5)
                         .when(col("weekday") == "sun", 6)
                         .otherwise(7)) 


def convert_timestamp(df):
    return df.withColumn("timestamp", from_utc_timestamp(col("start_date"), "UTC")) \
             .withColumn("updated_date", to_date(col("timestamp")))


def convert_titles(spark, finished_titles, finished_title_info):
    finished_titles.createOrReplaceTempView("titles_table")
    finished_title_info.createOrReplaceTempView("title_info_table")

    joined_titles_df = spark.sql("""
        SELECT DISTINCT 
            t.platform, t.id, t.title, t.author, ti.views, t.image_url, ti.release_day, t.is_completed
        FROM titles_table t
        LEFT JOIN title_info_table ti
            ON t.platform = ti.platform
            AND t.id = ti.id
    """)
    genres_df = spark.sql("""
        SELECT DISTINCT 
            platform, id AS title_id, SUBSTRING(genre_name, 2) AS genre_name
        FROM titles_table
    """)

    if SHOW:
        joined_titles_df.show(50)
        genres_df.show(50)

    save_to_parquet(joined_titles_df, "titles")
    save_to_parquet(genres_df, "genres")


def convert_episodes(spark, episodes, episode_likes, comments):
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

    joined_episodes_df = spark.sql("""
        SELECT DISTINCT 
            platform, title_id, id, title, likes, comments, image_url, updated_date
        FROM joined_episodes
    """)

    if SHOW:
        joined_episodes_df.show(50)

    save_to_parquet(joined_episodes_df, "episodes")


def create_spark_session():
    return SparkSession.builder \
        .appName("S3 Data Reader") \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()


def run():
    spark = create_spark_session()

    finished_titles_df = get_finished_titles(spark, "2025/02/27")
    finished_title_info_df = get_title_info(spark, "2025/02/27")
    finished_title_info_df = convert_weekday(finished_title_info_df)

    finished_episodes_df = get_episodes(spark, "2025/02/27")
    finished_episodes_df = convert_timestamp(finished_episodes_df)
    finished_episode_likes_df = get_episode_likes(spark, "2025/02/27")
    finished_comments_df = get_comments(spark, "2025/02/27")


    if BACKUP:
        backup_to_parquet(finished_titles_df, "finished_titles")
        backup_to_parquet(finished_title_info_df, "finished_title_info")

        backup_to_parquet(finished_episodes_df, "finished_episodes")
        backup_to_parquet(finished_episode_likes_df, "finished_episode_likes")
        backup_to_parquet(finished_comments_df, "finished_comments")

    convert_titles(spark, finished_titles_df, finished_title_info_df)
    convert_episodes(spark, finished_episodes_df, finished_episode_likes_df, finished_comments_df)


run()