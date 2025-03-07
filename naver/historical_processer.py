from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, explode, split, 
    to_date, from_unixtime, array
)
from datetime import datetime

TIME = datetime(2025, 2, 27)

BUCKET = "wt-grepp-lake"
PLATFORM = "naver"
RAW = "s3a://{bucket}/raw/{platform}/{target}/{target_date}"

BACKUP = False
SHOW = False

# target_folders 입력
target_folders = [ ]


def get_comments(spark, date):
    paths = [RAW.format(bucket=BUCKET, platform=PLATFORM, target="comments", target_date=date) + f"/{folder}/*.json" for folder in target_folders]
    df = spark.read.json(paths, multiLine=True)
    
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
    paths = [RAW.format(bucket=BUCKET, platform=PLATFORM, target="episode_likes", target_date=date) + f"/{folder}/*.json" for folder in target_folders]
    df = spark.read.json(paths, multiLine=True)
    
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
    paths = [RAW.format(bucket=BUCKET, platform=PLATFORM, target="episodes", target_date=date) + f"/{folder}/*.json" for folder in target_folders]
    df = spark.read.json(paths, multiLine=True)
    
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
            explode("gfpAdCustomParam.tags").alias("genre_name"),
            explode(
                when(
                    col("gfpAdCustomParam.weekdays").isNull() | (size(col("gfpAdCustomParam.weekdays")) == 0), 
                    array(lit("day"))
                )
                .otherwise(col("gfpAdCustomParam.weekdays"))
            ).alias("weekday")
        )


def get_titles(spark, date):
    url = RAW.format(bucket=BUCKET, platform=PLATFORM, target="titles", target_date=date)
    df = spark.read.json(f"{url}/*.json", multiLine=True)

    df = df.selectExpr("stack(7, 'MONDAY', titleListMap.MONDAY, "  
                               "'TUESDAY', titleListMap.TUESDAY, "
                               "'WEDNESDAY', titleListMap.WEDNESDAY, "
                               "'THURSDAY', titleListMap.THURSDAY, "
                               "'FRIDAY', titleListMap.FRIDAY, "
                               "'SATURDAY', titleListMap.SATURDAY, "
                               "'SUNDAY', titleListMap.SUNDAY) as (day, titles)")
    
    return df.select(explode(col("titles")).alias("title")) \
        .select(
            lit(PLATFORM).alias("platform"),
            col("title.titleId").alias("id"),
            col("title.titleName").alias("title"),
            col("title.author").alias("author"),
            col("title.viewCount").alias("views"),
            col("title.thumbnailUrl").alias("image_url"),
            lit(False).alias("is_completed")
        )


def get_finished_titles(spark, date):
    url = RAW.format(bucket=BUCKET, platform=PLATFORM, target="finished_titles", target_date=date)
    df = spark.read.json(f"{url}/*.json", multiLine=True)
    return df.select(explode(col(f"titleList")).alias("title")) \
        .select(
            lit(PLATFORM).alias("platform"),
            col("title.titleId").alias("id"),
            col("title.titleName").alias("title"),
            col("title.author").alias("author"),
            col("title.viewCount").alias("views"),
            col("title.thumbnailUrl").alias("image_url"),
            lit(True).alias("is_completed")
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
                         when(col("weekday") == "월", 0)
                         .when(col("weekday") == "화", 1)
                         .when(col("weekday") == "수", 2)
                         .when(col("weekday") == "목", 3)
                         .when(col("weekday") == "금", 4)
                         .when(col("weekday") == "토", 5)
                         .when(col("weekday") == "일", 6)
                         .otherwise(7)) 


def convert_timestamp(df):
    return df.withColumn("timestamp", from_unixtime(col("start_date") / 1000)) \
             .withColumn("updated_date", to_date(col("timestamp")))


def convert_titles(spark, finished_titles, finished_title_info):
    finished_titles.createOrReplaceTempView("titles_table")
    finished_title_info.createOrReplaceTempView("title_info_table")

    title_info_distinct_df = spark.sql("""
        SELECT DISTINCT
            title_id, release_day
        FROM title_info_table
    """)
    title_info_distinct_df.createOrReplaceTempView("title_info_distinct_table")

    titles_df = spark.sql("""
        SELECT DISTINCT 
            t.platform, t.id, t.title, t.author, t.views, t.image_url, ti.release_day, t.is_completed
        FROM titles_table t
        LEFT JOIN title_info_distinct_table ti
        ON t.id = ti.title_id
    """)
    genres_df = spark.sql("""
        SELECT DISTINCT platform, title_id, genre_name FROM title_info_table
    """)

    if SHOW:
        titles_df.show(50)
        genres_df.show(50)

    save_to_parquet(titles_df, "titles")
    save_to_parquet(genres_df, "genres")


def convert_episodes(spark, episodes, episode_likes, comments):
    episodes.createOrReplaceTempView("episodes_table")
    episode_likes.createOrReplaceTempView("episode_likes_table")
    comments.createOrReplaceTempView("comments_table")

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW joined_episodes AS
        SELECT e.*, el.likes, el.updated_date, c.comments
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
    
    titles_df = get_titles(spark, "2025/02/27")
    finished_titles_df = get_finished_titles(spark, "2025/02/27")
    finished_title_info_df = get_title_info(spark, "2025/02/27")
    finished_title_info_df = convert_weekday(finished_title_info_df)
    
    finished_episodes_df = get_episodes(spark, "2025/02/27")
    finished_episode_likes_df = get_episode_likes(spark, "2025/02/27")
    finished_episode_likes_df = convert_timestamp(finished_episode_likes_df)
    finished_comments_df = get_comments(spark, "2025/02/27")

    if BACKUP:
        backup_to_parquet(finished_titles_df, "finished_titles")
        backup_to_parquet(finished_title_info_df, "finished_title_info")

        backup_to_parquet(finished_episodes_df, "finished_episodes")
        backup_to_parquet(finished_episode_likes_df, "finished_episode_likes")
        backup_to_parquet(finished_comments_df, "finished_comments")
    
    convert_titles(spark, titles_df, finished_title_info_df)
    convert_titles(spark, finished_titles_df, finished_title_info_df)
    convert_episodes(spark, finished_episodes_df, finished_episode_likes_df, finished_comments_df)


run()