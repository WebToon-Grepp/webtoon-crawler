from enum import Enum

class NaverWebtoonEndpoint(Enum):
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "ko",
        "Origin": "https://comic.naver.com",
        "Referer": "https://comic.naver.com"
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

