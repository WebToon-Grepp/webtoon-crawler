import os
import json
import requests
from endpoint import NaverWebtoonEndpoint

headers = NaverWebtoonEndpoint.HEADERS.value

def fetch_titles():
    titles = []
    url = NaverWebtoonEndpoint.TITLES.value
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        titleListMaps = data.get("titleListMap", [])

        days = ["MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY", "SUNDAY"]
        for day in days:
            print(f"{day} LIMIT 10")
            for title in titleListMaps[day][:10]:
                titles.append(title['titleId'])
                print(f"{title['titleId']}: {title['titleName']} - {title['author']}")
            print("...\n")
        return titles
    except requests.RequestException as e:
        print(e)
        return None

def fetch_finished_titles(page_no=1):
    url = NaverWebtoonEndpoint.FINISHED_TITLES.value.format(page_no=page_no)
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        titleList = data.get("titleList", [])
        
        print("FINISHED PAGE INFO")
        print(data["pageInfo"])
        print()

        print("FINISHED LIMIT 10")
        for title in titleList[:10]:
            print(f"{title['titleId']}: {title['titleName']} - {title['author']}")
        print("...\n")

    except requests.RequestException as e:
        print(e)

def fetch_title_info(title_id):
    url = NaverWebtoonEndpoint.TITLE_INFO.value.format(title_id=title_id)
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        print(f"{data['titleId']}: {data['titleName']}")
        print(f"    관심웹툰 - {data['favoriteCount']}")
        print(f"    {data['age']['type']} - {data['curationTagList'][0]['tagName']}")
        print()

    except requests.RequestException as e:
        print(e)

def fetch_episode_list(title_id, page_no=1):
    url = NaverWebtoonEndpoint.EPISODE_LIST.value.format(title_id=title_id, page_no=page_no)
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        articleList = data.get("articleList", [])

        print(f"{title_id} PAGE INFO")
        print(data["pageInfo"])
        print()

        print(f"{title_id} EPISODE LIMIT 10")
        for article in articleList[:10]:
            print(f"{article['no']}: {article['subtitle']} - {article['serviceDateDescription']}")
            print(f"    별점 - {article['starScore']}")
        print("...\n")

    except requests.RequestException as e:
        print(e)

def fetch_episode_info(title_id, episode_id=1):
    url = NaverWebtoonEndpoint.EPISODE_INFO.value.format(title_id=title_id, episode_id=episode_id)
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()

        print(f"{title_id} EPISODE {episode_id} INFO")
        print(f"관심웹툰 - {data['favoriteInfo']['favoriteCount']}")
        print(f"별점 - {data['starInfo']['starScoreCount']} - {round(data['starInfo']['averageStarScore'], 2)}")
        print(f"조회수 - {data['viewCount']}")
        print()

    except requests.RequestException as e:
        print(e)

def fetch_comments(title_id, episode_id=1):
    url = NaverWebtoonEndpoint.COMMENTS.value.format(title_id=title_id, episode_id=episode_id)
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.text
        json_data = json.loads(data[data.index('(')+1:-2]) # return to javascript change value
        
        print(f"{title_id} EPISODE {episode_id} COMMENT INFO")
        print(json_data["result"]["count"])
        print()

    except requests.RequestException as e:
        print(e)

def fetch_episode_likes(title_id, episode_id=1):
    url = NaverWebtoonEndpoint.EPISODE_LIKES.value.format(title_id=title_id, episode_id=episode_id)
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()

        print(f"{title_id} EPISODE {episode_id} LIKE INFO")
        print(data["contents"][0]["reactions"][0])
        print()

    except requests.RequestException as e:
        print(e)

def fetch_all_data():
    titles = fetch_titles()
    finished_titles = fetch_finished_titles()
    
    for title in titles[:3]:
        fetch_title_info(title)
        fetch_episode_list(title)
        fetch_episode_info(title)
        fetch_comments(title)
        fetch_episode_likes(title)

