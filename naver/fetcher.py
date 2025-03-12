import os
import json
import requests
from endpoint import NaverWebtoonEndpoint
from datetime import datetime
import time
import shutil
import concurrent.futures

headers = NaverWebtoonEndpoint.HEADERS.value

def clear_output_folder():
    folder_path = 'output/raw/naver'
    if os.path.exists(folder_path):
        shutil.rmtree(folder_path)
        print(f"{folder_path} 폴더 초기화 완료")
    os.makedirs(folder_path, exist_ok=True)

def fetch_titles():
    titles = []
    url = NaverWebtoonEndpoint.TITLES.value
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        titleListMap = data.get("titleListMap", [])

        for day, titleList in titleListMap.items():
            for title in titleList:
                if isinstance(title, dict):
                    titles.append(title.get('titleId'))
                else:
                    print(f"Unexpected data format for title: {title}")
        
        current_date = datetime.now().strftime('%Y/%m/%d')
        file_path = f'output/raw/naver/titles/{current_date}/titles.json'

        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        
        print(f"전체 웹툰 목록을 {file_path}에 저장했습니다.")

        return titles
    
    except requests.RequestException as e:
        print(e)
        return None

def fetch_finished_titles():
    finished_titles = []
    url = NaverWebtoonEndpoint.FINISHED_TITLES.value.format(page_no=1)
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        totalPages = data.get("pageInfo", {}).get("totalPages", None)

        current_date = datetime.now().strftime('%Y/%m/%d')
        output_dir = f'output/raw/naver/finished_titles/{current_date}/'
        os.makedirs(output_dir, exist_ok=True)
        
        for page_no in range(1, totalPages + 1):
            url = NaverWebtoonEndpoint.FINISHED_TITLES.value.format(page_no=page_no)
            
            try:
                response = requests.get(url)
                response.raise_for_status()
                data = response.json()

                titleList = data.get('titleList', [])
                for title in titleList:
                    finished_titles.append(title.get('titleId'))

                file_path = f'{output_dir}finished_titles_{page_no}.json'

                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=4)

                # print(f"페이지 {page_no}의 데이터를 {file_path}에 저장했습니다.")
            
            except requests.RequestException as e:
                print(e)

        print(f"완결 웹툰 목록을 {file_path}에 저장했습니다.")
        return finished_titles
    
    except requests.RequestException as e:
        print(e)

def fetch_title_info(title_id):
    url = NaverWebtoonEndpoint.TITLE_INFO.value.format(title_id=title_id)
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()

        current_date = datetime.now().strftime('%Y/%m/%d')
        file_path = f'output/raw/naver/title_info/{current_date}/{title_id}.json'

        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        
        #print(f"{title_id}의 상세 정보를 {file_path}에 저장했습니다.")

    except requests.RequestException as e:
        print(e)

def fetch_episode_list(title_id, page_no=None, retry_count=1):
    if page_no is None:
        all_no_values = []
        url = NaverWebtoonEndpoint.EPISODE_LIST.value.format(title_id=title_id, page_no=1)

        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
            totalPages = data.get("pageInfo", {}).get("totalPages", None)
            totalCount = data.get("totalCount", None)

            current_date = datetime.now().strftime('%Y/%m/%d')
            output_dir = f'output/raw/naver/episodes/{current_date}/{title_id}/'
            os.makedirs(output_dir, exist_ok=True)

            for page_no in range(1, totalPages + 1):
                url = NaverWebtoonEndpoint.EPISODE_LIST.value.format(title_id=title_id, page_no=page_no)
                
                try:
                    response = requests.get(url, headers=headers)
                    response.raise_for_status()
                    data = response.json()

                    file_path = f'{output_dir}episodes_{page_no}.json'

                    with open(file_path, 'w', encoding='utf-8') as f:
                        json.dump(data, f, ensure_ascii=False, indent=4)

                    # print(f"{title_id}의 페이지 {page_no}의 데이터를 {file_path}에 저장했습니다.")
                
                except requests.RequestException as e:
                    print(e)

            #print(f"{title_id}의 {page_no}까지의 회차 목록을 {output_dir}에 저장했습니다.")

        except requests.RequestException as e:
            if e.response is not None and e.response.status_code == 401 and retry_count == 1:
                naver_cookie = os.getenv('NAVER_COOKIE', None)
                if naver_cookie:
                    headers["Cookie"] = naver_cookie
                    return fetch_episode_list(title_id, None, retry_count - 1)
                else:
                    print("NAVER_COOKIE가 존재하지 않습니다.")
            else:
                print(e)
        
        return totalCount

    else:
        url = NaverWebtoonEndpoint.EPISODE_LIST.value.format(title_id=title_id, page_no=page_no)

        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()

            current_date = datetime.now().strftime('%Y/%m/%d')
            output_dir = f'output/raw/naver/episodes/{current_date}/{title_id}/'
            os.makedirs(output_dir, exist_ok=True)

            file_path = f'{output_dir}episodes_{page_no}.json'

            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)

            article_list = data.get("articleList", [])
            if article_list:
                print(f"{title_id}의 페이지 {page_no}의 회차 목록을 {file_path}에 저장했습니다.")
                return article_list[0].get("no")
            else:
                return None

        except requests.RequestException as e:
            if e.response is not None and e.response.status_code == 401 and retry_count == 1:
                naver_cookie = os.getenv('NAVER_COOKIE', None)
                if naver_cookie:
                    headers["Cookie"] = naver_cookie
                    return fetch_episode_list(title_id, 1, retry_count - 1)
                else:
                    print("NAVER_COOKIE가 존재하지 않습니다.")
            else:
                print(e)

def fetch_episode_info(title_id, episode_id):
    current_date = datetime.now().strftime('%Y/%m/%d')
    output_dir = f'output/raw/naver/episode_info/{current_date}/{title_id}/'
    os.makedirs(output_dir, exist_ok=True)

    url = NaverWebtoonEndpoint.EPISODE_INFO.value.format(title_id=title_id, episode_id=episode_id)
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        file_path = f'{output_dir}{episode_id}.json'

        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        
        #print(f"{title_id}의 에피소드 {episode_id}의 info 데이터를 {file_path}에 저장했습니다.")

    except requests.RequestException as e:
        print(f"{title_id}의 에피소드 {episode_id}에 info 데이터가 없습니다.")

def fetch_comments(title_id, episode_id):
    current_date = datetime.now().strftime('%Y/%m/%d')
    output_dir = f'output/raw/naver/comments/{current_date}/{title_id}/'
    os.makedirs(output_dir, exist_ok=True)

    url = NaverWebtoonEndpoint.COMMENTS.value.format(title_id=title_id, episode_id=episode_id)
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.text
        json_data = json.loads(data[data.index('(')+1:-2])  # JavaScript 데이터 처리

        if json_data.get("result", {}).get("count", {}).get("total") == 0:
            print(f"{title_id}의 에피소드 {episode_id}에 댓글이 없습니다.")
        
        file_path = f'{output_dir}{episode_id}.json'

        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, ensure_ascii=False, indent=4)

        #print(f"{title_id}의 에피소드 {episode_id}의 comment 데이터를 {file_path}에 저장했습니다.")

    except requests.RequestException as e:
        print(e)

def fetch_episode_likes(title_id, episode_id):
    current_date = datetime.now().strftime('%Y/%m/%d')
    output_dir = f'output/raw/naver/episode_likes/{current_date}/{title_id}/'
    os.makedirs(output_dir, exist_ok=True)

    url = NaverWebtoonEndpoint.EPISODE_LIKES.value.format(title_id=title_id, episode_id=episode_id)

    for attempt in range(5):
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            if not data['contents'][0]['reactions'] and not data['contents'][0]['reactionMap']:
                print(f"{title_id}의 에피소드 {episode_id}에 likes 데이터가 없습니다.")
            
            file_path = f'{output_dir}{episode_id}.json'
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)

            print(f"{title_id}의 에피소드 {episode_id}의 likes 데이터를 {file_path}에 저장했습니다.")
            break

        except requests.RequestException as e:
            print(f"시도 {attempt + 1} 실패: {e}")
            if attempt < 4:
                time.sleep(1)
            else:
                print(f"{title_id}의 에피소드 {episode_id}에서 오류가 계속 발생하여 더 이상 시도하지 않습니다.")

def fetch_titles_daily(dayInt):
    days = ["MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY", "SUNDAY"]
    day_name = days[dayInt]

    titles = []
    url = NaverWebtoonEndpoint.TITLES.value

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()

        current_date = datetime.now().strftime('%Y/%m/%d')
        file_path = f'output/raw/naver/titles/{current_date}/titles.json'

        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

        title_list = data.get("titleListMap", {}).get(day_name, [])

        for title in title_list:
            if isinstance(title, dict):
                titles.append(title.get('titleId'))
            else:
                print(f"Unexpected data format for title: {title}")

        print(f"{days[dayInt]} 전체 웹툰 목록을 {file_path}에 저장했습니다.")

        return titles

    except requests.RequestException as e:
        print(e)
        return None

def fetch_all_data():
    clear_output_folder()

    titles = fetch_titles()
    finished_titles = fetch_finished_titles()
    final_titles = titles + finished_titles

    start = time.time()

    title_to_episode_count = {}

    def process_webtoon(title_id):
        fetch_title_info(title_id)
        total_episode_no = fetch_episode_list(title_id)

        for i in range(1, total_episode_no + 1):
            fetch_episode_info(title_id, i)
            fetch_comments(title_id, i)
        
        return total_episode_no

    with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        future_to_title = {}

        for title in final_titles:
            future = executor.submit(process_webtoon, title)
            future_to_title[future] = title

        for future in concurrent.futures.as_completed(future_to_title):
            title = future_to_title[future]
            try:
                total_episode_no = future.result()
                title_to_episode_count[title] = total_episode_no
                print(f"{title} episode info/comments 데이터 처리 완료")
            except Exception as e:
                print(f"{title} 처리 중 오류 발생: {e}")
    
    end = time.time()
    print("episode info/comments 데이터 처리 수행시간: %f 초" % (end - start))
    print()

    start = time.time()

    with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count()) as likes_executor:
        likes_futures = []

        for title in final_titles:
            total_episode_no = title_to_episode_count.get(title, 0)
            if total_episode_no == 0:
                print(f"{title}의 에피소드 수를 가져오지 못했습니다.")
                break

            for i in range(1, total_episode_no + 1):
                future = likes_executor.submit(fetch_episode_likes, title, i)
                likes_futures.append(future)

                if len(likes_futures) % 5 == 0:
                    time.sleep(1)

        for future in concurrent.futures.as_completed(likes_futures):
            try:
                future.result()
            except Exception as e:
                print(f"likes 데이터 처리 중 오류 발생: {e}")
    
    end = time.time()
    print("likes 데이터 처리 수행시간: %f 초" % (end - start))

def fetch_all_historical_data():
    clear_output_folder()
    
    titles = fetch_titles()
    finished_titles = fetch_finished_titles()
    final_titles = titles + finished_titles

    start = time.time()

    title_to_episode_count = {}

    def process_webtoon(title_id):
        fetch_title_info(title_id)
        total_episode_no = fetch_episode_list(title_id)

        for i in range(1, total_episode_no + 1):
            fetch_episode_info(title_id, i)
            fetch_comments(title_id, i)
        
        return total_episode_no

    with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        future_to_title = {}

        for title in final_titles:
            future = executor.submit(process_webtoon, title)
            future_to_title[future] = title

        for future in concurrent.futures.as_completed(future_to_title):
            title = future_to_title[future]
            try:
                total_episode_no = future.result()
                title_to_episode_count[title] = total_episode_no
                print(f"{title} episode info/comments 데이터 처리 완료")
            except Exception as e:
                print(f"{title} 처리 중 오류 발생: {e}")
    
    end = time.time()
    print("episode info/comments 데이터 처리 수행시간: %f 초" % (end - start))
    print()

    start = time.time()

    with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count()) as likes_executor:
        likes_futures = []

        for title in final_titles:
            total_episode_no = title_to_episode_count.get(title, 0)
            if total_episode_no == 0:
                print(f"{title}의 에피소드 수를 가져오지 못했습니다.")
                break

            for i in range(1, total_episode_no + 1):
                future = likes_executor.submit(fetch_episode_likes, title, i)
                likes_futures.append(future)

                if len(likes_futures) % 5 == 0:
                    time.sleep(1)

        for future in concurrent.futures.as_completed(likes_futures):
            try:
                future.result()
            except Exception as e:
                print(f"likes 데이터 처리 중 오류 발생: {e}")
    
    end = time.time()
    print("likes 데이터 처리 수행시간: %f 초" % (end - start))

def fetch_daily_data(dayInt):
    clear_output_folder()
    
    titles = fetch_titles_daily(dayInt)

    for title in titles:
        fetch_title_info(title)
        latest_episode_id = fetch_episode_list(title, 1)
        fetch_episode_info(title, latest_episode_id)
        fetch_comments(title, latest_episode_id)
        fetch_episode_likes(title, latest_episode_id)
        print()
