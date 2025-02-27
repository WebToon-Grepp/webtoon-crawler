import os
import json
import requests
from endpoint import NaverWebtoonEndpoint
from datetime import datetime
import time
import shutil

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
        
        print(f"{title_id}의 상세 정보를 {file_path}에 저장했습니다.")

    except requests.RequestException as e:
        print(e)

def fetch_episode_list(title_id, page_no=None):
    if page_no is None:
        url = NaverWebtoonEndpoint.EPISODE_LIST.value.format(title_id=title_id, page_no=1)

        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
            totalPages = data.get("pageInfo", {}).get("totalPages", None)

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

            print(f"{title_id}의 {page_no}까지의 회차 목록을 {output_dir}에 저장했습니다.")

        except requests.RequestException as e:
            print(e)
    
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
            print(e)

def fetch_episode_info(title_id, episode_id = 1):
    current_date = datetime.now().strftime('%Y/%m/%d')
    output_dir = f'output/raw/naver/episode_info/{current_date}/{title_id}/'
    os.makedirs(output_dir, exist_ok=True)

    while True:
        url = NaverWebtoonEndpoint.EPISODE_INFO.value.format(title_id=title_id, episode_id=episode_id)
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            file_path = f'{output_dir}{episode_id}.json'

            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
            
            # print(f"{title_id}의 에피소드 {episode_id}의 info 데이터를 {file_path}에 저장했습니다.")
            episode_id += 1

        except requests.RequestException as e:
            print(f"{title_id}의 에피소드 {episode_id - 1}까지의 info 데이터를 {file_path}에 저장했습니다.")
            print(f"{title_id}의 에피소드 {episode_id}에 info 데이터가 더이상 없습니다. 종료합니다.")
            break

def fetch_comments(title_id, episode_id=None):
    current_date = datetime.now().strftime('%Y/%m/%d')
    output_dir = f'output/raw/naver/comments/{current_date}/{title_id}/'
    os.makedirs(output_dir, exist_ok=True)

    if episode_id is not None:
        url = NaverWebtoonEndpoint.COMMENTS.value.format(title_id=title_id, episode_id=episode_id)
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.text
            json_data = json.loads(data[data.index('(')+1:-2])  # JavaScript 데이터 처리

            if json_data.get("result", {}).get("count", {}).get("total") == 0:
                print(f"{title_id}의 에피소드 {episode_id}에 댓글이 없습니다.")
                print("코드를 수정할 필요가 있습니다.")
            
            file_path = f'{output_dir}{episode_id}.json'

            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, ensure_ascii=False, indent=4)

            print(f"{title_id}의 에피소드 {episode_id}의 comment 데이터를 {file_path}에 저장했습니다.")

        except requests.RequestException as e:
            print(e)
    
    else:
        episode_id = 1
        while True:
            url = NaverWebtoonEndpoint.COMMENTS.value.format(title_id=title_id, episode_id=episode_id)
            try:
                response = requests.get(url, headers=headers)
                response.raise_for_status()
                data = response.text
                json_data = json.loads(data[data.index('(')+1:-2])  # JavaScript 데이터 처리

                if json_data.get("result", {}).get("count", {}).get("total") == 0:
                    print(f"{title_id}의 에피소드 {episode_id - 1}까지의 comment 데이터를 {file_path}에 저장했습니다.")
                    print(f"{title_id}의 에피소드 {episode_id}에 댓글이 없습니다. 종료합니다.")
                    break

                file_path = f'{output_dir}{episode_id}.json'
                
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(json_data, f, ensure_ascii=False, indent=4)

                episode_id += 1

            except requests.RequestException as e:
                print(e)
                break

def fetch_episode_likes(title_id, episode_id=None):
    current_date = datetime.now().strftime('%Y/%m/%d')
    output_dir = f'output/raw/naver/episode_likes/{current_date}/{title_id}/'
    os.makedirs(output_dir, exist_ok=True)

    if episode_id is not None:
        url = NaverWebtoonEndpoint.EPISODE_LIKES.value.format(title_id=title_id, episode_id=episode_id)
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            if not data['contents'][0]['reactions'] and not data['contents'][0]['reactionMap']:
                print(f"{title_id}의 에피소드 {episode_id}에 likes 데이터가 더이상 없습니다.")
                print("코드를 수정할 필요가 있습니다.")
            
            file_path = f'{output_dir}{episode_id}.json'
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)

            print(f"{title_id}의 에피소드 {episode_id}의 likes 데이터를 {file_path}에 저장했습니다.")

        except requests.RequestException as e:
            print(e)
    
    else:
        episode_id = 1
        while True:
            url = NaverWebtoonEndpoint.EPISODE_LIKES.value.format(title_id=title_id, episode_id=episode_id)
            try:
                response = requests.get(url)
                response.raise_for_status()
                data = response.json()

                if not data['contents'][0]['reactions'] and not data['contents'][0]['reactionMap']:
                    print(f"{title_id}의 에피소드 {episode_id - 1}까지의 likes 데이터를 {file_path}에 저장했습니다.")
                    print(f"{title_id}의 에피소드 {episode_id}에 좋아요 데이터가 더이상 없습니다. 종료합니다.")
                    break

                file_path = f'{output_dir}{episode_id}.json'
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=4)

                episode_id += 1

                time.sleep(0.1)

            except requests.RequestException as e:
                print(e)
                break

def fetch_titles_daily(dayInt):
    days = ["MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY", "SUNDAY"]
    day_name = days[dayInt]

    titles = []
    url = NaverWebtoonEndpoint.TITLES.value

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()

        titleListMap = {
            "titleListMap": {
                day_name: data.get("titleListMap", {}).get(day_name, [])
            },
            "dayOfWeek": day_name
        }

        current_date = datetime.now().strftime('%Y/%m/%d')
        file_path = f'output/raw/naver/titles/{current_date}/{day_name}.json'

        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(titleListMap, f, ensure_ascii=False, indent=4)

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

    for title in final_titles:
        fetch_title_info(title)
        fetch_episode_list(title)
        fetch_episode_info(title)
        fetch_comments(title)
        fetch_episode_likes(title)
        print()

def fetch_all_historical_data():
    clear_output_folder()
    
    titles = fetch_titles()
    finished_titles = fetch_finished_titles()
    
    final_titles = titles + finished_titles

    for title in final_titles:
        fetch_title_info(title)
        fetch_episode_list(title)
        fetch_episode_info(title)
        fetch_comments(title)
        fetch_episode_likes(title)

def fetch_daily_data(dayInt):
    titles = fetch_titles_daily(dayInt)

    for title in titles:
        fetch_title_info(title)
        latest_episode_id = fetch_episode_list(title, 1)
        fetch_episode_info(title, latest_episode_id)
        fetch_comments(title, latest_episode_id)
        fetch_episode_likes(title, latest_episode_id)
        print()
