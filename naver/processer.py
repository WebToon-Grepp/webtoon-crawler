from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, col, lit, from_unixtime, col, to_date
from datetime import datetime

NOW = datetime.now()
PLATFORM = "naver"
BUCKET_RAW = "s3a://wt-grepp-lake/raw/{platform}/{target}/{target_date}"
BUCKET_PROCESSED = "s3a://wt-grepp-lake/processed/{platform}/{target}/{target_date}"


def get_comments(spark, date):
    url = BUCKET_RAW.format(platform=PLATFORM, target="comments", target_date=date)
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
    url = BUCKET_RAW.format(platform=PLATFORM, target="episode_likes", target_date=date)
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
    url = BUCKET_RAW.format(platform=PLATFORM, target="episodes", target_date=date)
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
    url = BUCKET_RAW.format(platform=PLATFORM, target="title_info", target_date=date)
    df = spark.read.json(f"{url}/*.json", multiLine=True)
    return df.select(
            lit(PLATFORM).alias("platform"),
            col("gfpAdCustomParam.titleId").alias("title_id"),
            explode("gfpAdCustomParam.tags").alias("genre_name")
        )


def get_titles(spark, date, dayInt):
    days = ["MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY", "SUNDAY"]

    url = BUCKET_RAW.format(platform=PLATFORM, target="titles", target_date=date)
    df = spark.read.json(f"{url}/*.json", multiLine=True)
    return df.select(explode(col(f"titleListMap.{days[dayInt]}")).alias("title")) \
        .select(
            lit(PLATFORM).alias("platform"),
            col("title.titleId").alias("id"), 
            col("title.titleName").alias("title"), 
            col("title.author").alias("author"),
            col("title.viewCount").alias("views"),
            col("title.thumbnailUrl").alias("image_url"), 
            lit(days[dayInt]).alias("release_day"),
            lit(False).alias("is_completed")
        )


def save_to_parquet(df, target):
    date_str = NOW.strftime("year=%Y/month=%m/day=%d")
    path = f"s3a://wt-grepp-lake/processed/{target}/{date_str}/{PLATFORM}/{target}"
    df.write.format("parquet").mode("overwrite").save(path)
    print(f"Data successfully saved to {path}")


def backup_to_parquet(df, target):
    path = f"s3a://wt-grepp-lake/temp/{PLATFORM}/{target}"
    df.coalesce(50).write.format("parquet").mode("overwrite").save(path)
    print(f"Data successfully backup to {path}")


def join_episodes(spark, episodes, comments, episode_likes):
    episodes.createOrReplaceTempView("episodes_table")
    episode_likes.createOrReplaceTempView("episode_likes_table")
    comments.createOrReplaceTempView("comments_table")

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW joined_episodes AS
        SELECT e.*, c.comments, el.likes, el.updated_date
        FROM episodes_table e
        JOIN comments_table c
            ON e.platform = c.platform
            AND e.title_id = c.title_id
            AND e.id = c.id
        JOIN episode_likes_table el
            ON c.platform = el.platform
            AND c.title_id = el.title_id
            AND c.id = el.id
    """)

    joined_episodes_df = spark.sql("""
        SELECT DISTINCT 
            platform, title_id, id, title, likes, comments, image_url, updated_date
        FROM joined_episodes
    """)

    joined_episodes_df.show(50)
    save_to_parquet(joined_episodes_df, "episodes")


def run():
    spark = SparkSession.builder \
        .appName("S3 {PLATFORM} Data Processing") \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    date_str = NOW.strftime("%Y/%m/%d")
    dayInt = NOW.weekday()

    titles_df = get_titles(spark, date_str, dayInt)
    title_info_df = get_title_info(spark, date_str)
    episodes_df = get_episodes(spark, date_str)
    episode_likes_df = get_episode_likes(spark, date_str)
    episode_likes_df = episode_likes_df.withColumn("timestamp", from_unixtime(col("start_date") / 1000)) \
                                       .withColumn("updated_date", to_date(col("timestamp")))
    comments_df = get_comments(spark, date_str)

    backup_to_parquet(titles_df, "s3a://wt-grepp-lake/raw/temp/naver_titles_df")
    backup_to_parquet(title_info_df, "s3a://wt-grepp-lake/raw/temp/naver_title_info_df")
    backup_to_parquet(episodes_df, "s3a://wt-grepp-lake/raw/temp/naver_episodes_df")
    backup_to_parquet(episode_likes_df, "s3a://wt-grepp-lake/raw/temp/naver_episode_likes_df")
    backup_to_parquet(comments_df, "s3a://wt-grepp-lake/raw/temp/naver_comments_df")
    
    titles_df.show(50)
    title_info_df.show(50)
    save_to_parquet(titles_df, "titles")
    save_to_parquet(title_info_df, "genres")

    join_episodes(spark, episodes_df, comments_df, episode_likes_df)

run()
