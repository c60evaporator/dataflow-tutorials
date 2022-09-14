import yaml
with open('./settings/config.yml') as file:
    config = yaml.safe_load(file)

from google.cloud import pubsub_v1
import numpy as np
import time
from datetime import datetime

PROJECT_ID = config['project_id']  # プロジェクトID
TOPIC_ID = config['topic_id']  # トピックID

P_TANAKA = 0.1  # 田中さんに投票する確率
P_NAKAMURA = 0.2  # 中村さんに投票する確率
P_SUZUKI = 0.3  # 鈴木さんに投票する確率
P_SATO = 0.4  # 佐藤さんに投票する確率

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

i = 0
while True:
    i += 1
    data_str = '{' + f'\"Date\":\"{datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]}\"'  # 時刻(ミリ秒単位)
    data_str += f",\"Number\":\"{i}\""  # 実行順
    # 1秒待つ
    time.sleep(1)
    # 0〜1の間のランダム変数を生成
    vote_rand = np.random.rand()
    # ランダム変数に応じて投票結果を決定
    if vote_rand < P_TANAKA:
        vote = 'Tanaka'
    elif vote_rand < P_TANAKA + P_NAKAMURA:
        vote = 'Nakamura'
    elif vote_rand < P_TANAKA + P_NAKAMURA + P_SUZUKI:
        vote = 'Suzuki'
    elif vote_rand < P_TANAKA + P_NAKAMURA + P_SUZUKI + P_SATO:
        vote = 'Sato'
    data_str += f',\"Vote\":\"{vote}\"' + '}'
    # データをByte化
    data = data_str.encode("utf-8")
    # データをPub/Subにパブリッシュ
    future = publisher.publish(topic_path, data)
    print(f"Published messages to {topic_path}. result={future.result()}")