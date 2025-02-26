import shutil
import os
import json
import requests
from datetime import datetime
from endpoint import KakaoWebtoonEndpoint

DEFAULT_TOTAL_COUNT = 9999


def save_json(data, subfolder_name, file_name, platform="kakao"):
    date_str = datetime.now().strftime("%Y/%m/%d")
    output_folder = os.path.join("output", "raw", platform, subfolder_name, date_str)

    file_path = os.path.join(output_folder, f"{file_name}.json")
    dir_path = os.path.dirname(file_path)
    if os.path.exists(dir_path):
        shutil.rmtree(dir_path)
    os.makedirs(os.path.dirname(file_path))

    try:
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"Data successfully saved to {file_path}")
    except (OSError, json.JSONDecodeError) as e:
        print(f"Error saving data to {file_path}: {e}")


def fetch_json(url, subfolder_name, file_name):
    try:
        headers = KakaoWebtoonEndpoint.HEADERS.value
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        json_data = response.json()
        save_json(json_data, subfolder_name, file_name)

        return json_data

    except requests.RequestException as e:
        print(f"Error fetching data from {url}: {e}")
        return None


def fetch_titles_from_url(url, subfolder_name, file_name):
    titles = []
    try:
        json_data = fetch_json(url, subfolder_name, file_name)
        data = json_data.get("data", [])
        if data:
            card_groups = data[0].get("cardGroups", [])
            if card_groups:
                for content in card_groups[0].get("cards", []):
                    titles.append(content['content']['id'])
        return titles
    except requests.RequestException as e:
        print(f"Error fetching titles from {url}: {e}")
        return None


def fetch_titles(day):
    url = KakaoWebtoonEndpoint.TITLES.value[day]
    return fetch_titles_from_url(url, "titles", "titles")


def fetch_finished_titles():
    url = KakaoWebtoonEndpoint.FINISHED_TITLES.value
    return fetch_titles_from_url(url, "finished_titles", "finished_titles")


def fetch_title_info(title_id):
    try:
        url = KakaoWebtoonEndpoint.TITLE_INFO.value.format(title_id=title_id)
        fetch_json(url, "title_info", title_id)
    except requests.RequestException as e:
        print(f"Error fetching title info for {title_id}: {e}")


def fetch_episodes(title_id, total_count=DEFAULT_TOTAL_COUNT):
    episodes = []
    try:
        url = KakaoWebtoonEndpoint.EPISODE_LIST.value.format(title_id=title_id, total_count=total_count)
        json_data = fetch_json(url, "episodes", "episodes")

        data = json_data.get("data", {})
        if data:
            for episode in data.get("episodes", []):
                episodes.append(episode['id'])

        return episodes
    except requests.RequestException as e:
        print(f"Error fetching episodes for title {title_id}: {e}")
        return None


def fetch_comments(title_id, episode_id, total_count=DEFAULT_TOTAL_COUNT):
    try:
        url = KakaoWebtoonEndpoint.COMMENTS.value.format(episode_id=episode_id, total_count=total_count)
        fetch_json(url, "comments", os.path.join(str(title_id), str(episode_id)))
    except requests.RequestException as e:
        print(f"Error fetching comments for title {title_id} and episode {episode_id}: {e}")


def fetch_episode_likes(title_id, episode_id):
    try:
        url = KakaoWebtoonEndpoint.EPISODE_LIKES.value.format(title_id=title_id, episode_id=episode_id)
        fetch_json(url, "episode_likes", os.path.join(str(title_id), str(episode_id)))
    except requests.RequestException as e:
        print(f"Error fetching episode likes for title {title_id} and episode {episode_id}: {e}")


def fetch_data_for_titles(titles):
    for title in titles:
        fetch_title_info(title)
        episodes = fetch_episodes(title)
        
        for episode in episodes:
            fetch_comments(title, episode)
            fetch_episode_likes(title, episode)


# Airflow Past Data DAG Call Function
def fetch_all_historical_data():
    for day in range(7):
        titles = fetch_titles(day)
        fetch_data_for_titles(titles)

    finished_titles = fetch_finished_titles()
    fetch_data_for_titles(finished_titles)


# Airflow Current Data DAG Call Function
def fetch_daily_data(day):
    titles = fetch_titles(day)
    fetch_data_for_titles(titles)


# Main Call Function
def fetch_all_data():
    titles = fetch_titles(0)
    fetch_data_for_titles(titles)

