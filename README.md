# Webtoon Crawler
webtoon-crawler는 네이버와 카카오 웹툰에서 원시 데이터를 크롤링하는 독립적인 Python 모듈입니다. 이 모듈은 Airflow 없이 단독으로 실행할 수 있으며, 나중에 Airflow DAGs에서 하나의 태스크로 실행될 수 있습니다.

- 독립 실행 가능: Airflow를 사용하지 않고 독립적으로 실행할 수 있습니다.
- Airflow 통합 가능: Airflow DAG에 통합하여 자동화된 작업 흐름을 구축할 수 있습니다.

## Commit Convention
webtoon-crawler의 커밋 메시지는 기능/모듈명과 세부 내용을 포함해 아래와 같은 형식으로 작성합니다. 이를 통해 어떤 기능이 추가되었는지 쉽게 확인할 수 있습니다.

```
<기능/모듈명>:: <커밋 내용>
```

예시:
- crawler:: 웹툰 크롤러 모듈 초기화
- crawler:: 네이버 웹툰 크롤링 기능 추가
- crawler:: 데이터 후처리 로직 추가
