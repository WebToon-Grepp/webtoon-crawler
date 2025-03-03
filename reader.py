from pyspark.sql import SparkSession
from datetime import datetime

NOW = datetime.now()

BUCKET = "wt-grepp-lake"

def generate_path(target, date=False, platform=None):
    base_path = f"s3a://{BUCKET}/processed/{target}"

    if platform:
        date_str = NOW.strftime("year=%Y/month=%m/day=%d")
        return f"{base_path}/{date_str}/platform={platform}"

    if date:
        date_str = NOW.strftime("year=%Y/month=%m/day=%d")
        return f"{base_path}/{date_str}"
    
    return base_path

def read_and_print_files(spark, path):
    df = spark.read.parquet(path)
    files = df.collect()
    for file in files[:25]:
        print(file)

def reader(spark, target, date=False, platform=False):
    path = generate_path(target, date=date)

    if platform:
        for p in ["naver", "kakao"]:
            platform_path = generate_path(target, date=date, platform=p)
            print(f"Reading data from: {platform_path}")
            read_and_print_files(spark, platform_path)
    else:
        print(f"Reading data from: {path}")
        read_and_print_files(spark, path)


def create_spark_session():
    return SparkSession.builder \
        .appName("S3 Data Reader") \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()


def run():
    spark = create_spark_session()
    for target in ["titles", "genres", "episodes"]:
        # 전체 보기
        # reader(spark, target)

        # 특정 날짜보기
        # reader(spark, target, date=True)

        # 특정 날짜/플랫폼별 보기
        reader(spark, target, date=True, platform=True) 
run()
