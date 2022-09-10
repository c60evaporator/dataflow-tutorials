import yaml
with open('./settings/config.yml') as file:
    config = yaml.safe_load(file)

import argparse
import logging
import re
from datetime import datetime

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import WorkerOptions

DEFAULT_PROJECT_ID = config['project_id']  # プロジェクトID
DEFAULT_BUCKET = config['dataflow_output_backet']  # トピックID
DEFAULT_JOB_NAME = 'compute-word-frequency'  # デフォルトのジョブ名
DEFAULT_REGION = 'us-central1'  # デフォルトのリージョン

# BigQueryのテーブル名(`データセット名.テーブル名`のフォーマット)
BQ_TABLE = 'dataflow_test.dataflow_wordcount'
# BigQueryのスキーマ
BQ_SCHEMA = ({'fields': [{'name': 'date', 'type': 'DATETIME', 'mode': 'REQUIRED'},
                         {'name': 'word_name', 'type': 'STRING', 'mode': 'REQUIRED'},
                         {'name': 'count', 'type': 'INTEGER', 'mode': 'REQUIRED'}
                         ]})


class WordExtractingDoFn(beam.DoFn):
    """各行の内容を単語ごとに分解するためのクラス"""

    def process(self, element):
        """Returns an iterator over the words of this element.
        The element is a line of text.  If the line is blank, note that, too.
        Args:
          element: the element being processed
        Returns:
          The processed element.
        """
        return re.findall(r'[\w\']+', element, re.UNICODE)


def run(argv=None):
    parser = argparse.ArgumentParser()
    # 現在時刻を取得
    date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    # 入力ファイルのパス
    parser.add_argument(
        '--input',
        default='gs://dataflow-samples/shakespeare/kinglear.txt',
        help='Input file to process.')
    # 出力先のパス
    parser.add_argument(
        '--output',
        default=f'gs://{DEFAULT_BUCKET}/results/',  # GCS に出力する
        help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)

    # プロジェクトID
    if '--project' not in pipeline_args:
        pipeline_args += ['--project', DEFAULT_PROJECT_ID]
    # リージョン
    if '--region' not in pipeline_args:
        pipeline_args += ['--region', DEFAULT_REGION]
    #  一時ファイルの GCS パス
    if '--temp_location' not in pipeline_args:
        pipeline_args += ['--temp_location', f'gs://{DEFAULT_BUCKET}/temp']
    pipeline_options = PipelineOptions(pipeline_args)

    # GCP オプション
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    # ワーカーオプション
    pipeline_options.view_as(
        WorkerOptions).autoscaling_algorithm = 'THROUGHPUT_BASED'  # 自動スケーリングを有効化する
    # 標準オプション
    pipeline_options.view_as(
        StandardOptions).runner = 'DataflowRunner'  # Dataflow ランナーを指定

    # パイプラインの記述
    with beam.Pipeline(options=pipeline_options) as p:
        # テキストファイル読み込み
        lines = p | 'Read' >> ReadFromText(known_args.input)

        # 単語をカウントする処理
        counts = (
            lines  # 行ごとに処理
            | 'Split' >> (beam.ParDo(WordExtractingDoFn()).with_output_types(str))  # 行内の単語を分割する
            | 'PairWithOne' >> beam.Map(lambda x: (x, 1))  # カウント用に単語と1を(キー, 1)のように紐付ける
            | 'GroupAndSum' >> beam.CombinePerKey(sum))  # 単語(キー)ごとにカウント数を合計

        # フォーマットを「単語名: カウント数」の形式に変更
        def format_result(word, count):
            return {'date': date, 'word_name': word, 'count': count}
        output = counts | 'Format' >> beam.MapTuple(format_result)

        # BigQueryに出力
        #output | 'Write' >> WriteToText(known_args.output)
        output | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
            BQ_TABLE,
            schema=BQ_SCHEMA,
            insert_retry_strategy='RETRY_ON_TRANSIENT_ERROR',
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_APPEND')


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
