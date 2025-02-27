from enum import Enum

class NaverWebtoonEndpoint(Enum):
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "ko",
        "Origin": "https://comic.naver.com",
        "Referer": "https://comic.naver.com",
        "Cookie": "NNB=VXK3FQFT3E7GO; _ga_3X9JZ731KT=GS1.1.1732789211.1.0.1732789219.0.0.0; BNB_FINANCE_HOME_TOOLTIP_MYASSET=true; ASID=da94a24700000193f10f70df00000065; _fbp=fb.1.1735613220956.406915121579615872; nstore_session=lzXFQ4L4jq2ke06EUC+lw9Ji; nstore_pagesession=i3LCvlqWpt3wLlsL0Ed-212646; tooltipDisplayed=true; ba.uuid=eb9674e6-7d63-4a7a-a4a1-24b38c36a0f8; cto_bundle=res0IV9BeWFUTGF4QjJPY21WcVpxUSUyQjJuJTJCN3hVVU8yZlpnJTJCNk0yV0xUa2lQT1R1WEFLRjZqczA3NGV6TkhIOHdBbktuV21lWDl5YWo5VEZzaUJiMEJTaFVicnVTWGJVVGF1Q1dwVDU0eEpoTUtobkVDdkhIT29GNlMlMkJwZGNEaUVGRDE1MjJyWm9WZm8xYlo5Zm5LUzBzYUZsZyUzRCUzRA; _ga_EEN65PKS7L=GS1.1.1738489343.1.1.1738489389.0.0.0; nid_inf=157710679; NID_AUT=BQc8rsAw6isYL17xasj83IZkONd6w4PamRUgn9Bq6swW2TFWt1RUznKHunzOc/3E; NID_JKL=ro0ZBwGpm7iy+O5gPDJL5XU1MJQrV7LBJo6ue58ObVI=; NAC=y8dEBggX3uSZ; _gid=GA1.2.254045013.1740445547; _ga_KQ24M8MXRT=GS1.1.1740445546.1.1.1740445597.0.0.0; _ga=GA1.1.34882428.1732789212; XSRF-TOKEN=ce7ec7d1-e759-4ee8-9891-e8b73a78cfa0; _ga_EFBDNNF91G=GS1.1.1740558938.5.0.1740558938.0.0.0; BUC=O-SdSA6_kXUXnANfBCKIRJqSTbronYGuygaKtuWXQ3A=; NID_SES=AAAB16yDrj3b8dIEPWCU2XxUDc5M7auU9pRDgF1wRlxMOnwIvUmjNHUbGCUUXfNKP51Aqd9P528jiqI1nXVHuV/7RNgmYotbfEPss2bUQ7tSh9rQSEuVlJXUBIkwfV9g1OYYsYpRrmklO+XMyK4lgZOISF8RJyBKCLHN2U0p4yFlJyV8x5Ce18QFkqtTN6oWvMNzVrlIa9lXP0KzumALREWKkAeQrY80+q5JFhIYMscFfKk2KnRI0Ea733ndrKODUT6l+74Sa3qx+c2Jx9Z2A4g3MJIEdpLXU+ESqy6mKMMwy51Zt6YDus4mafFf1NnO22hh/wmeaJY8NrNYLb2pknY8VIeduZxBfz6GOxdhsJzbaUjbU/R4n6dK2FgAfQq/DNibKjpbiFcaeTIfIXQJN3LkXuTv/R3Lc1LDTz8C8tGh+c/SyOSVTK32NM0w+WXUEQ4Y93EYooGZnRGCM1SQShq8Q0fAHn7HBR24rOJEUutzbwlGc81uUFXCWcpCed3JQ3FLZ3Veq0gCKUdUXdXfhDvbHKz4AIj9J4g8sej8InS6pcM3AmJn6Qzrdj86VnAV3ezeU2rKxI0PSL3CS/omf903P1Zmu97u9RaFeTqZWNq+QHqkWdfbeECm3qKkOyFWjj1hPw=="
    }

    TITLES = "https://comic.naver.com/api/webtoon/titlelist/weekday?order=view"
    FINISHED_TITLES = "https://comic.naver.com/api/webtoon/titlelist/finished?order=view&page={page_no}"

    TITLE_INFO = "https://comic.naver.com/api/article/list/info?titleId={title_id}"
    EPISODE_LIST = "https://comic.naver.com/api/article/list?titleId={title_id}&page={page_no}"
    EPISODE_INFO = "https://comic.naver.com/api/userAction/info?titleId={title_id}&no={episode_id}"
    COMMENTS = "https://apis.naver.com/commentBox/cbox/web_naver_list_jsonp.json?ticket=comic&templateId=webtoon&pool=cbox3&lang=ko&country=KR&objectId={title_id}_{episode_id}"
    EPISODE_LIKES = "https://comic.like.naver.com/v1/search/contents?q=COMIC[{title_id}_{episode_id}]"

class KakaoWebtoonEndpoint(Enum):
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "ko",
        "Origin": "https://webtoon.kakao.com",
        "Referer": "https://webtoon.kakao.com"
    }

    TITLES = [
        "https://gateway-kw.kakao.com/section/v2/timetables/days?placement=timetable_mon",
        "https://gateway-kw.kakao.com/section/v2/timetables/days?placement=timetable_tue", 
        "https://gateway-kw.kakao.com/section/v2/timetables/days?placement=timetable_wed", 
        "https://gateway-kw.kakao.com/section/v2/timetables/days?placement=timetable_thu", 
        "https://gateway-kw.kakao.com/section/v2/timetables/days?placement=timetable_fri", 
        "https://gateway-kw.kakao.com/section/v2/timetables/days?placement=timetable_sat", 
        "https://gateway-kw.kakao.com/section/v2/timetables/days?placement=timetable_sun"
    ]
    FINISHED_TITLES = "https://gateway-kw.kakao.com/section/v2/timetables/days?placement=timetable_completed"

    TITLE_INFO = "https://gateway-kw.kakao.com/decorator/v2/decorator/contents/{title_id}"
    EPISODE_LIST = "https://gateway-kw.kakao.com/episode/v2/views/content-home/contents/{title_id}/episodes?sort=-NO&offset=0&limit={total_count}"
    COMMENTS = "https://gateway-kw.kakao.com/comment/v1/comments?relationType=EPISODE&relationId={episode_id}&sort=-LIKE&offset=0&limit={total_count}"
    EPISODE_LIKES = "https://gateway-kw.kakao.com/popularity/v1/reviews/episode/{episode_id}?contentId={title_id}"

