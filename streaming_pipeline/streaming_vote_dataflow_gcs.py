import yaml
with open('./settings/config.yml') as file:
    config = yaml.safe_load(file)

import argparse
from datetime import datetime
import logging
import random

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows

DEFAULT_PROJECT_ID = config['project_id']  # プロジェクトID
DEFAULT_BUCKET = config['dataflow_output_backet']  # 一時ファイル出力先のバケット
DEFAULT_JOB_NAME = 'streaming-vote-dataflow'  # デフォルトのジョブ名
DEFAULT_REGION = 'us-central1'  # デフォルトのリージョン
DEFAULT_TOPIC = config['topic_id']  # デフォルトのPub/SubトピックID
DEFAULT_SUBSCRIPTION = config['subscription_id']  # デフォルトのPub/SubサブスクリプションID
DEFAULT_WINDOW_MINUTE = 0.5  # 集計のウィンドウ (分単位)
DEFAULT_NUM_SHARDS = 5  # シャーディング数 (GCSへの高速書込のためのファイル分散化数)

class GroupMessagesByFixedWindows(beam.PTransform):
    """
    Pub/Subメッセージをタイムスタンプでウィンドウ処理し、シャーディングを指定するクラス
    """

    def __init__(self, window_size, num_shards=5):
        # Set window size to 60 seconds.
        self.window_size = int(window_size * 60)
        self.num_shards = num_shards

    def expand(self, pcoll):
        return (
            pcoll
            # タイムスタンプに応じてウィンドウを振り分け
            | "Window into fixed intervals"
            >> beam.WindowInto(FixedWindows(self.window_size))
            # Dataflowで処理した時刻を表すタイムスタンプを追加
            | "Add timestamp to windowed elements" >> beam.ParDo(AddTimestamp())
            # GCSへの書込シャーディング(高速書込のための分散化)用にランダムなキーを割り振る
            | "Add key" >> beam.WithKeys(lambda _: random.randint(0, self.num_shards - 1))
            # Group windowed elements by key. All the elements in the same window must fit
            # memory for this. If not, you need to use `beam.util.BatchElements`.
            | "Group by key" >> beam.GroupByKey()
        )


class AddTimestamp(beam.DoFn):
    """Dataflowで処理した時刻を表すタイムスタンプを追加するクラス"""
    def process(self, element, publish_time=beam.DoFn.TimestampParam):
        """Processes each windowed element by extracting the message body and its
        publish time into a tuple.
        """
        yield (
            element.decode("utf-8"),
            f'\n\"DataflowTimestamp\":\"{datetime.utcfromtimestamp(float(publish_time)).strftime("%Y-%m-%d %H:%M:%S.%f")}\"',
        )

class WriteToGCS(beam.DoFn):
    """シャーディング用キーに基づきGCSに分散書き込みするクラス"""
    def __init__(self, output_path):
        self.output_path = output_path

    def process(self, key_value, window=beam.DoFn.WindowParam):
        """Write messages in a batch to Google Cloud Storage."""
        ts_format = "%H:%M"
        window_start = window.start.to_utc_datetime().strftime(ts_format)
        window_end = window.end.to_utc_datetime().strftime(ts_format)
        shard_id, batch = key_value
        filename = "-".join([self.output_path, window_start, window_end, str(shard_id)])
        print('AAAAAAAAAAAAAAAAAAAA')

        with beam.io.gcsio.GcsIO().open(filename=filename, mode="w") as f:
            f.write(f"{batch}\n".encode("utf-8"))

def run(argv=None):
    parser = argparse.ArgumentParser()
    # 入力となるPub/SubのトピックID
    parser.add_argument(
        "--input_topic",
        default=f'projects/{DEFAULT_PROJECT_ID}/topics/{DEFAULT_TOPIC}',
        help="The Cloud Pub/Sub topic to read from."
        '"projects/<PROJECT_ID>/topics/<TOPIC_ID>".',
    )
    # 集計のウィンドウ (分単位)
    parser.add_argument(
        "--window_size",
        type=float,
        default=DEFAULT_WINDOW_MINUTE,
        help="Output file's window size in minutes.",
    )
    # 出力先のパス
    parser.add_argument(
        "--output_path",
        default=f'gs://{DEFAULT_BUCKET}/samples/output',
        help="Path of the output GCS file including the prefix.",
    )
    # シャードの数
    parser.add_argument(
        "--num_shards",
        type=int,
        default=DEFAULT_NUM_SHARDS,
        help="Number of shards to use when writing windowed elements to GCS.",
    )
    # 入力した引数をknown_args(本スクリプトで使用する入出力用の引数)とpipeline_args(Apache Beam実行時のオプション)に分割
    known_args, pipeline_args = parser.parse_known_args(argv)
    
    # Apache Beam実行時オプションのデフォルト値入力
    # プロジェクトIDのデフォルト値
    if len([s for s in pipeline_args if '--project' in s]) == 0:
        pipeline_args += ['\xa0', f'--project={DEFAULT_PROJECT_ID}']
    # リージョンのデフォルト値
    if len([s for s in pipeline_args if '--region' in s]) == 0:
        pipeline_args += ['\xa0', f'--region={DEFAULT_REGION}']
    # 一時ファイルの GCS パスのデフォルト値
    if len([s for s in pipeline_args if '--temp_location' in s]) == 0:
        pipeline_args += ['\xa0', f'--temp_location=gs://{DEFAULT_BUCKET}/temp']
    # Dataflow ランナーのデフォルト値 (DataflowRunner)
    if len([s for s in pipeline_args if '--runner' in s]) == 0:
        pipeline_args += ['\xa0', f'--runner=DataflowRunner']

    # Apache Beam実行時オプションの適用
    print(f'known_args={known_args}')
    print(f'Beam pipeline options={pipeline_args}')
    # Set `save_main_session` to True so DoFns can access globally imported modules.
    pipeline_options = PipelineOptions(
        pipeline_args, streaming=True, save_main_session=True
    )

    with beam.Pipeline(options=pipeline_options) as p:
        # Pub/Subメッセージ読み込み
        messages = p | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(topic=known_args.input_topic)
        
        # データの変換
        # フォーマットを「単語名: カウント数」の形式に変更
        def format_result(shard_key, data):
            data_transform = data[0][0] + str(shard_key)
            return (shard_key, data_transform)

        transformed = (
            messages
            # タイムスタンプでウィンドウ処理
            | "Window into" >> GroupMessagesByFixedWindows(known_args.window_size, known_args.num_shards)
            # 改行で分割
            | 'Format' >> beam.MapTuple(format_result)
        )
        # GCSに書き出し
        transformed | "Write to GCS" >> beam.ParDo(WriteToGCS(known_args.output_path))

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()