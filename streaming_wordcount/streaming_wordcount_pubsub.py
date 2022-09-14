"""
以下記事を参考にWikipediano

https://asanonaoki.com/blog/wikipediaのapiを使ってランダムに記事本文を取得する/
"""

import yaml
with open('./settings/config.yml') as file:
    config = yaml.safe_load(file)

from google.cloud import pubsub_v1
import numpy as np
import time
from datetime import datetime
import requests

PROJECT_ID = config['project_id']  # プロジェクトID
TOPIC_ID = config['topic_id']  # トピックID
S = requests.Session()  # API取得用のセッション
URL = "https://en.wikipedia.org/w/api.php"  # APIのURL
PARAMS = {  # API取得用のパラメータ
    "action": "query",
    "format": "json",
    "list": "random",
    "rnlimit": "1",
    "rnnamespace": "0"
}

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

i = 0
while True:
    i += 1
    # 1秒待つ
    time.sleep(1)
    # API取得
    res = S.get(url=URL, params=PARAMS)
    res_dict = res.json()
    data_str = res_dict["query"]["random"][0]['title']
    # データをByte化
    data = data_str.encode("utf-8")
    # データをPub/Subにパブリッシュ
    future = publisher.publish(topic_path, data)
    print(f"Published messages to {topic_path}. result={future.result()}")