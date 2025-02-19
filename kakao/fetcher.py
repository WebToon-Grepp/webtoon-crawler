import os
import json
import requests
from endpoint import KakaoWebtoonEndpoint

headers = KakaoWebtoonEndpoint.HEADERS.value

def fetch_titles():
    title_list = []
    urls = KakaoWebtoonEndpoint.TITLES.value
    
    try:
        for url in urls:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json().get("data", [])
            cardGroups = data[0].get("cardGroups", [])
            cards = cardGroups[0].get("cards", [])

            print(url)
            for content in cards[:10]:
                title_list.append(content['content']['id'])
                print(f"{content['content']['id']}: {content['content']['title']} - {content['content']['authors'][0]['name']}")
            print("...\n")
            print()
        return title_list
    except requests.RequestException as e:
        print(e)
        return None

def fetch_finished_titles():
    title_list = []
    url = KakaoWebtoonEndpoint.FINISHED_TITLES.value
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json().get("data", [])
        cardGroups = data[0].get("cardGroups", [])
        cards = cardGroups[0].get("cards", [])
        
        for content in cards[:10]:
            title_list.append(content['content']['id'])
            print(f"{content['content']['id']}: {content['content']['title']} - {content['content']['authors'][0]['name']}")
        print("...\n")
        print()

        return title_list

    except requests.RequestException as e:
        print(e)
        return None

def fetch_title_info(title_id):
    url = KakaoWebtoonEndpoint.TITLE_INFO.value.format(title_id=title_id)
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json().get("data", {})
        
        print(f"{data['id']}: {data['title']}")
        print(f"    조회수 - {data['statistics']['viewCount']}")
        print(f"    좋아요 - {data['statistics']['likeCount']}")
        print(f"    {data['genre']}")
        print()

    except requests.RequestException as e:
        print(e)

def fetch_episode_list(title_id, total_count=10):
    episode_list = []
    url = KakaoWebtoonEndpoint.EPISODE_LIST.value.format(title_id=title_id, total_count=total_count)
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json().get("data", {})
        episodes = data.get("episodes", [])

        print(f"{title_id} EPISODE LIMIT 10")
        for episode in episodes[:10]:
            episode_list.append(episode['id'])
            print(f"{episode['id']}: {episode['title']} - {episode['serialStartDateTime']}")
        print("...\n")
        return episode_list
    except requests.RequestException as e:
        print(e)

def fetch_comments(title_id, episode_id, total_count=10):
    url = KakaoWebtoonEndpoint.COMMENTS.value.format(episode_id=episode_id, total_count=total_count)
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()

        print(f"{title_id} EPISODE {episode_id} COMMENT INFO")
        print(data["meta"]["pagination"])
        print()

    except requests.RequestException as e:
        print(e)

def fetch_episode_likes(title_id, episode_id=1):
    url = KakaoWebtoonEndpoint.EPISODE_LIKES.value.format(title_id=title_id, episode_id=episode_id)
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()

        print(f"{title_id} EPISODE {episode_id} LIKE INFO")
        print(data["data"])
        print()

    except requests.RequestException as e:
        print(e)

def fetch_all_data():
    titles = fetch_titles()
    finished_titles = fetch_finished_titles()
    
    for title in titles[:3]:
        fetch_title_info(title)
        episodes = fetch_episode_list(title)
        fetch_comments(title, episodes[0])
        fetch_episode_likes(title, episodes[0])

