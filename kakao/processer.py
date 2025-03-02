from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql.functions import explode, col, lit, concat_ws, to_date, from_utc_timestamp
from datetime import datetime

NOW = datetime.now()
PLATFORM = "kakao"
BUCKET_RAW = "s3a://wt-grepp-lake/raw/{platform}/{target}/{target_date}"


def get_comments(spark, title_id, episode_id, date):
    url = BUCKET_RAW.format(platform=PLATFORM, target="comments", target_date=date)
    df = spark.read.json(f"{url}/{title_id}/{episode_id}.json", multiLine=True)
    return df.select(
            lit(PLATFORM).alias("platform"),
            lit(title_id).alias("title_id"),
            lit(episode_id).alias("id"),
            col("meta.pagination.totalCount").alias("comments")
        )


def get_episode_likes(spark, date):
    url = BUCKET_RAW.format(platform=PLATFORM, target="episode_likes", target_date=date)
    df = spark.read.json(f"{url}/*/*.json", multiLine=True)
    return df.select(
            lit(PLATFORM).alias("platform"),
            col("data.episodeId").alias("id"),
            col("data.likeCount").alias("likes")
        )


def get_episodes(spark, date):
    url = BUCKET_RAW.format(platform=PLATFORM, target="episodes", target_date=date)
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
    url = BUCKET_RAW.format(platform=PLATFORM, target="title_info", target_date=date)
    df = spark.read.json(f"{url}/*.json", multiLine=True)
    return df.select(
            lit(PLATFORM).alias("platform"),
            col("data.id").alias("id"),
            col("data.statistics.viewCount").alias("views")
        )


def get_titles(spark, date):
    url = BUCKET_RAW.format(platform=PLATFORM, target="titles", target_date=date)
    df = spark.read.json(f"{url}/*.json", multiLine=True)
    return df.select(explode(col("data")).alias("data")) \
        .select(
            explode(col("data.cardGroups")).alias("cardGroups"), 
            col("data.title").alias("release_day")
        ).select(explode(col("cardGroups.cards")).alias("cards"), col("release_day")) \
        .select(col("cards.content").alias("content"), col("release_day")) \
        .select(
            lit(PLATFORM).alias("platform"),
            col("content.id").alias("id"), 
            col("content.title").alias("title"), 
            concat_ws(" / ", col("content.authors.name")).alias("author"),
            col("content.featuredCharacterImageA").alias("image_url"), 
            col("release_day"),
            lit(False).alias("is_completed"),
            explode("content.seoKeywords").alias("genre_name")
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


def join_titles(spark, titles, title_info):
    titles.createOrReplaceTempView("titles_table")
    title_info.createOrReplaceTempView("title_info_table")

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW joined_titles AS
        SELECT t.*, ti.views
        FROM titles_table t
        JOIN title_info_table ti
            ON t.platform = ti.platform
            AND t.id = ti.id
    """)

    joined_titles_df = spark.sql("""
        SELECT DISTINCT 
            platform, id, title, author, views, image_url, release_day, is_completed
        FROM joined_titles
    """)
    joined_genres_df = spark.sql("""
        SELECT platform, id AS title_id, genre_name FROM joined_titles
    """)

    joined_titles_df.show(50)
    joined_genres_df.show(50)
    save_to_parquet(joined_titles_df, "titles")
    save_to_parquet(joined_genres_df, "genres")


def join_episodes(spark, episodes, comments, episode_likes):
    episodes.createOrReplaceTempView("episodes_table")
    episode_likes.createOrReplaceTempView("episode_likes_table")
    comments.createOrReplaceTempView("comments_table")

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW joined_episodes AS
        SELECT e.*, c.comments, el.likes
        FROM episodes_table e
        JOIN comments_table c
            ON e.platform = c.platform
            AND e.title_id = c.title_id
            AND e.id = c.id
        JOIN episode_likes_table el
            ON c.platform = el.platform
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
    
    titles_df = get_titles(spark, date_str)
    title_info_df = get_title_info(spark, date_str)
    episodes_df = get_episodes(spark, date_str)
    episodes_df = episodes_df.withColumn("timestamp", from_utc_timestamp(col("start_date"), "UTC")) \
                             .withColumn("updated_date", to_date(col("timestamp")))
    episode_likes_df = get_episode_likes(spark, date_str)
    
    comments_schema = StructType([
        StructField("platform", StringType(), True),
        StructField("title_id", StringType(), True),
        StructField("id", LongType(), True),
        StructField("comments", LongType(), True)
    ])
    comments_df = spark.createDataFrame([], comments_schema)
    episode_group = episodes_df.select("platform", "title_id", "id") \
        .groupBy("platform", "title_id", "id").count()

    for row in episode_group.toLocalIterator():
        title_id = row["title_id"]
        episode_id = row["id"]

        cdf = get_comments(spark, title_id, episode_id, date_str)
        comments_df = comments_df.union(cdf)

    backup_to_parquet(titles_df, "titles")
    backup_to_parquet(title_info_df, "title_info")
    backup_to_parquet(episodes_df, "episodes")
    backup_to_parquet(episode_likes_df, "episode_likes")
    backup_to_parquet(comments_df, "comments")
    
    join_titles(spark, titles_df, title_info_df)
    join_episodes(spark, episodes_df, comments_df, episode_likes_df)


run()
