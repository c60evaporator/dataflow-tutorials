import yaml
with open('./settings/config.yml') as file:
    config = yaml.safe_load(file)

import argparse
from datetime import datetime
import logging
import ast

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

class TransformToDictByFixedWindows(beam.PTransform):
    """
    Pub/Subメッセージをタイムスタンプでウィンドウ処理してデータをdict化し、一括でグルーピングするクラス
    (クラス化せずに直接パイプラインを記述するとウインドウ作成後のGroupByKeyによるグルーピングがうまくいかない)
    """

    def __init__(self, window_size):
        # Set window size to 60 seconds.
        self.window_size = int(window_size * 60)

    def expand(self, pcoll):
        """各種処理のパイプラインを記載"""
        return (
            pcoll
            # タイムスタンプに応じてウィンドウを振り分け
            | "Window into fixed intervals"
            >> beam.WindowInto(FixedWindows(self.window_size))
            # Dataflowで処理した時刻を表すタイムスタンプを追加 (この時刻に応じてウィンドウが決まる)
            | "Add timestamp to windowed elements" >> beam.ParDo(AddTimestamp())
            # 各レコードをdict形式に変換
            | 'Format to dict' >> beam.Map(lambda record: ast.literal_eval(record))
            # グルーピング用にウィンドウのスタート時間をキーに設定
            | "Add key" >> beam.WithKeys(lambda _: 1)
            # グルーピングしてウィンドウ内のデータを一つにまとめる
            | "Group by key" >> beam.GroupByKey()
        )

class AddTimestamp(beam.DoFn):
    """
    Dataflowで処理した時刻を表すタイムスタンプを追加するクラス
    """
    def process(self, element, publish_time=beam.DoFn.TimestampParam):
        yield element.decode("utf-8")[:-1] \
            + f',\"DataflowTimestamp\":\"{datetime.utcfromtimestamp(float(publish_time)).strftime("%Y-%m-%d %H:%M:%S.%f")}\"' + '}'

class WriteToGCS(beam.DoFn):
    """キーに基づきGCSに分散書き込みするクラス"""
    def __init__(self, output_path):
        self.output_path = output_path

    def process(self, key_value, window=beam.DoFn.WindowParam):
        """Write messages in a batch to Google Cloud Storage."""
        ts_format = "%H:%M"
        window_start = window.start.to_utc_datetime().strftime(ts_format)
        window_end = window.end.to_utc_datetime().strftime(ts_format)
        tmp_key, batch = key_value
        filename = "-".join([self.output_path, window_start, window_end])

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
        transformed = (
            messages
            | "Window into" >> TransformToDictByFixedWindows(known_args.window_size)
        )
        # GCSに書き出し
        transformed | "Write to GCS" >> beam.ParDo(WriteToGCS(known_args.output_path))

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()