# Webtoon Crawler
webtoon-crawler는 네이버와 카카오 웹툰에서 원시 데이터를 크롤링하는 독립적인 Python 모듈입니다. 이 모듈은 Airflow 없이 단독으로 실행할 수 있으며, 나중에 Airflow DAGs에서 하나의 태스크로 실행될 수 있습니다.

- 독립 실행 가능: Airflow를 사용하지 않고 독립적으로 실행할 수 있습니다.
- Airflow 통합 가능: Airflow DAG에 통합하여 자동화된 작업 흐름을 구축할 수 있습니다.

## Usage

### Git Clone/Pull
GitHub에서 프로젝트를 클론하거나 최신 상태로 업데이트하려면 아래 명령어를 사용합니다.

```bash
# GitHub에서 프로젝트 클론
git clone https://github.com/WebToon-Grepp/webtoon-crawler.git

# 최신 상태로 업데이트 (Pull)
git pull origin master
```

### 크롤러 실행 (fetcher.py)
`fetcher.py`는 특정 요일이나 모든 히스토리 데이터를 가져오는 기능을 제공합니다.

- 일별 데이터 가져오기
```python
# 특정 날짜의 데이터 가져오기
from platform.fetcher import fetch_daily_data

fetch_daily_data(0) # 원하는 요일 입력 0~6 (월~일)
```
- 모든 히스토리 데이터 가져오기
```python
# 모든 히스토리 데이터 가져오기
from platform.fetcher import fetch_all_historical_data

fetch_all_historical_data()
```

### 데이터 전처리 (optimizer)
크롤링한 데이터를 하나로 합치는 전처리하는 작업입니다. `spark-submit`을 사용해 전처리 작업을 Spark에서 실행하며, AWS S3에 접근할 수 있는 키 정보가 필요합니다.

```bash
# 네이버 웹툰 데이터 전처리
spark-submit --master local --packages org.apache.hadoop:hadoop-aws:3.2.2 \
--conf spark.hadoop.fs.s3a.access.key=<access_key> \
--conf spark.hadoop.fs.s3a.secret.key=<secret_key> \
naver/optimizer.py

# 카카오 웹툰 데이터 전처리
spark-submit --master local --packages org.apache.hadoop:hadoop-aws:3.2.2 \
--conf spark.hadoop.fs.s3a.access.key=<access_key> \
--conf spark.hadoop.fs.s3a.secret.key=<secret_key> \
kakao/optimizer.py
```
#### 상수 값 설정
- **BUCKET**: S3 버킷 이름을 설정하는 상수입니다. 본인의 버킷 이름에 맞게 수정해야 합니다.
- **SHOW**: 데이터를 50개씩 보여줄지 설정하는 상수입니다. SHOW = True로 설정하면 50개씩 출력됩니다.

### 데이터 후처리 (processer)
크롤링한 데이터를 실사용 전에 후처리하는 작업입니다. `spark-submit`을 사용해 후처리 작업을 Spark에서 실행하며, AWS S3에 접근할 수 있는 키 정보가 필요합니다.

```bash
# 네이버 웹툰 데이터 후처리
spark-submit --master local --packages org.apache.hadoop:hadoop-aws:3.2.2 \
--conf spark.hadoop.fs.s3a.access.key=<access_key> \
--conf spark.hadoop.fs.s3a.secret.key=<secret_key> \
naver/processer.py

# 카카오 웹툰 데이터 후처리
spark-submit --master local --packages org.apache.hadoop:hadoop-aws:3.2.2 \
--conf spark.hadoop.fs.s3a.access.key=<access_key> \
--conf spark.hadoop.fs.s3a.secret.key=<secret_key> \
kakao/processer.py
```
#### 상수 값 설정
- **BUCKET**: S3 버킷 이름을 설정하는 상수입니다. 본인의 버킷 이름에 맞게 수정해야 합니다.

### 데이터 읽기 (reader)
후처리된 데이터를 읽어 출력하는 작업입니다. spark-submit을 사용하여 실행하며, AWS S3에 접근할 수 있는 키 정보가 필요합니다.

```bash
# 웹툰 데이터 읽기
spark-submit --master local --packages org.apache.hadoop:hadoop-aws:3.2.2 \
--conf spark.hadoop.fs.s3a.access.key=<access_key> \
--conf spark.hadoop.fs.s3a.secret.key=<secret_key> \
reader.py
```
#### 상수 값 설정
- **BUCKET**: S3 버킷 이름을 설정하는 상수입니다. 본인의 버킷 이름에 맞게 수정해야 합니다.

### 크롤러 실행 (crawler)
전체 웹툰 데이터를 크롤링할 때 사용하는 기본 명령어입니다. `-t` 옵션을 사용하여 크롤링할 플랫폼을 지정합니다.

```bash
# 네이버 웹툰 크롤링
python main.py -t naver

# 카카오 웹툰 크롤링
python main.py -t kakao
```

## Output Structure
모든 데이터는 `output` 디렉토리에 저장됩니다. 이 디렉토리는 데이터 처리 및 읽기 작업에서 생성되는 결과 파일들이 포함되는 기본 경로입니다. 

```plaintext
output/
│
└── raw/ # 추후 활용을 위한 데이터
    ├── naver/
    │   ├── titles/               # 전체 웹툰 목록
    │   │   └── YYYY/MM/DD/titles.json
    │   ├── finished_titles/      # 완결 웹툰 목록
    │   │   └── YYYY/MM/DD/finished_titles_{page_no}.json
    │   ├── title_info/           # 웹툰 상세 정보
    │   │   └── YYYY/MM/DD/{title_id}.json
    │   ├── episodes/             # 회차 목록
    │   │   └── YYYY/MM/DD/{title_id}/episodes_{page_no}.json
    │   ├── episode_info/         # 회차별 상세 정보
    │   │   └── YYYY/MM/DD/{title_id}/{episode_id}.json
    │   ├── comments/             # 댓글 데이터
    │   │   └── YYYY/MM/DD/{title_id}/{episode_id}.json
    │   └── episode_likes/        # 회차별 좋아요 수
    │       └── YYYY/MM/DD/{title_id}/{episode_id}.json
    │
    └── kakao/
        ├── titles/               # 요일별 웹툰 목록
        │   └── YYYY/MM/DD/titles_{day}.json
        ├── finished_titles/      # 완결 웹툰 목록
        │   └── YYYY/MM/DD/finished_titles.json
        ├── title_info/           # 웹툰 상세 정보
        │   └── YYYY/MM/DD/{title_id}.json
        ├── episodes/             # 회차 목록
        │   └── YYYY/MM/DD/{title_id}/episodes.json
        ├── comments/             # 댓글 데이터
        │   └── YYYY/MM/DD/{title_id}/{episode_id}.json
        └── episode_likes/        # 회차별 좋아요 수
            └── YYYY/MM/DD/{title_id}/{episode_id}.json
```

## Commit Convention
webtoon-crawler의 커밋 메시지는 기능/모듈명과 세부 내용을 포함해 아래와 같은 형식으로 작성합니다. 이를 통해 어떤 기능이 추가되었는지 쉽게 확인할 수 있습니다.

```
<기능/모듈명>:: <커밋 내용>
```

예시:
- crawler:: 웹툰 크롤러 모듈 초기화
- crawler:: 네이버 웹툰 크롤링 기능 추가
- crawler:: 데이터 후처리 로직 추가
