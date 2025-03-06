from pyspark.sql import SparkSession
from datetime import datetime

NOW = datetime.now()

BUCKET = "wt-grepp-lake"
PLATFORM = "kakao"
RAW = "s3a://{bucket}/optimized/{target}/{target_date}/platform={platform}/"


def read_to_parquet(spark, target, date):
    url = RAW.format(bucket=BUCKET, target=target, target_date=date, platform=PLATFORM)
    return spark.read.parquet(url)


def save_to_parquet(df, target):
    date_str = NOW.strftime("year=%Y/month=%m/day=%d")
    path = f"s3a://wt-grepp-lake/processed/{target}/{date_str}"
    df.write.partitionBy("platform").format("parquet").mode("append").save(path)
    print(f"Data successfully saved to {path}")


def convert_titles(spark, titles, title_info):
    titles.createOrReplaceTempView("titles_table")
    title_info.createOrReplaceTempView("title_info_table")

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW joined_titles AS
        SELECT t.*, ti.views
        FROM titles_table t
        LEFT JOIN title_info_table ti
            ON t.id = ti.id
    """)

    joined_titles_df = spark.sql(f"""
        SELECT DISTINCT 
            '{PLATFORM}' AS platform,
            id, title, author, views, image_url, release_day, is_completed
        FROM joined_titles
    """)
    joined_genres_df = spark.sql(f"""
        SELECT DISTINCT 
            '{PLATFORM}' AS platform,
            id AS title_id, SUBSTRING(genre_name, 2) AS genre_name
        FROM joined_titles
    """)

    save_to_parquet(joined_titles_df, "titles")
    save_to_parquet(joined_genres_df, "genres")


def convert_episodes(spark, episodes, episode_likes, comments):
    episodes.createOrReplaceTempView("episodes_table")
    episode_likes.createOrReplaceTempView("episode_likes_table")
    comments.createOrReplaceTempView("comments_table")

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW joined_episodes AS
        SELECT e.*, c.comments, el.likes
        FROM episodes_table e
        LEFT JOIN episode_likes_table el
            ON e.title_id = el.title_id
            AND e.id = el.id
        LEFT JOIN comments_table c
            ON el.title_id = c.title_id
            AND el.id = c.id
    """)

    joined_episodes_df = spark.sql(f"""
        SELECT DISTINCT 
            '{PLATFORM}' AS platform,
            title_id, id, title, likes, comments, image_url, updated_date
        FROM joined_episodes
        WHERE updated_date IS NOT NULL
    """)

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
    date_str = NOW.strftime("year=%Y/month=%m/day=%d")
    
    titles_df = read_to_parquet(spark, "titles", date_str)
    title_info_df = read_to_parquet(spark, "title_info", date_str)

    episodes_df = read_to_parquet(spark, "episodes", date_str)
    episode_likes_df = read_to_parquet(spark, "episode_likes", date_str)
    comments_df = read_to_parquet(spark, "comments", date_str)

    convert_titles(spark, titles_df, title_info_df)
    convert_episodes(spark, episodes_df, episode_likes_df, comments_df)


run()
