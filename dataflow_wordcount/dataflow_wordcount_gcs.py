import yaml
with open('./settings/config.yml') as file:
    config = yaml.safe_load(file)

import argparse
import logging
import re

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import WorkerOptions

DEFAULT_PROJECT_ID = config['project_id']  # プロジェクトID
DEFAULT_BUCKET = config['dataflow_output_backet']  # トピックID
DEFAULT_JOB_NAME = 'compute-word-frequency'  # デフォルトのジョブ名
DEFAULT_REGION = 'us-central1'  # デフォルトのリージョン

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
    pipeline_options = PipelineOptions(pipeline_args)

    # GCP オプションの可視化 (デバッグ用なのでなくてもOK)
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    # ワーカーオプション
    pipeline_options.view_as(
        WorkerOptions).autoscaling_algorithm = 'THROUGHPUT_BASED'  # 自動スケーリングを有効化する

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
            return '%s: %d' % (word, count)
        output = counts | 'Format' >> beam.MapTuple(format_result)

        # GCSに出力
        output | 'Write' >> WriteToText(known_args.output)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()