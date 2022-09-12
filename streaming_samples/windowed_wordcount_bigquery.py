"""
以下のサンプルコードをVSCodeのデバッグモードで動くよう改良
https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/windowed_wordcount.py
"""
import yaml
with open('./settings/config.yml') as file:
    config = yaml.safe_load(file)

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""A streaming word-counting workflow.
Important: streaming pipeline support in Python Dataflow is in development
and is not yet available for use.
"""

# pytype: skip-file

import argparse
import logging

import apache_beam as beam
from apache_beam.transforms import window
from apache_beam.options.pipeline_options import PipelineOptions

DEFAULT_PROJECT_ID = config['project_id']  # プロジェクトID
DEFAULT_BUCKET = config['dataflow_output_backet']  # 一時ファイル出力先のバケット
DEFAULT_JOB_NAME = 'streaming-vote-dataflow'  # デフォルトのジョブ名
DEFAULT_REGION = 'us-central1'  # デフォルトのリージョン
DEFAULT_TOPIC = config['topic_id']  # デフォルトのPub/SubトピックID
DEFAULT_SUBSCRIPTION = config['subscription_id']  # デフォルトのPub/SubサブスクリプションID
DEFAULT_WINDOW_MINUTE = 0.5  # 集計のウィンドウ (分単位)

# BigQueryのテーブル名(`データセット名.テーブル名`のフォーマット)
BQ_TABLE = 'dataflow_test.window_wordcount'

BQ_SCHEMA = ({'fields': [{'name': 'word', 'type': 'STRING', 'mode': 'REQUIRED'},
                         {'name': 'count', 'type': 'INTEGER', 'mode': 'REQUIRED'},
                         {'name': 'window_start', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
                         {'name': 'window_end', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'}
                         ]})

TABLE_SCHEMA = (
    'word:STRING, count:INTEGER, '
    'window_start:TIMESTAMP, window_end:TIMESTAMP')


def find_words(element):
    import re
    element_str = element.decode("utf-8")
    return re.findall(r'[A-Za-z\']+', element_str)


class FormatDoFn(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        ts_format = '%Y-%m-%d %H:%M:%S.%f UTC'
        window_start = window.start.to_utc_datetime().strftime(ts_format)
        window_end = window.end.to_utc_datetime().strftime(ts_format)
        return [{
            'word': element[0],
            'count': element[1],
            'window_start': window_start,
            'window_end': window_end
        }]


def main(argv=None):
    """Build and run the pipeline."""

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input_topic',
        default=f'projects/{DEFAULT_PROJECT_ID}/topics/{DEFAULT_TOPIC}',
        help='Input PubSub topic of the form "/topics/<PROJECT>/<TOPIC>".')
    parser.add_argument(
        '--output_table',
        default=BQ_TABLE,  # デフォルトで指定するBigQueryのテーブル
        help=(
            'Output BigQuery table for results specified as: '
            'PROJECT:DATASET.TABLE or DATASET.TABLE.'))
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

        # Read the text from PubSub messages.
        lines = p | beam.io.ReadFromPubSub(known_args.input_topic)

        # Get the number of appearances of a word.
        def count_ones(word_ones):
            (word, ones) = word_ones
            return (word, sum(ones))

        transformed = (
            lines
            | 'Split' >> (beam.FlatMap(find_words).with_output_types(str))
            | 'PairWithOne' >> beam.Map(lambda x: (x, 1))
            | beam.WindowInto(window.FixedWindows(2 * 60, 0))
            | 'Group' >> beam.GroupByKey()
            | 'Count' >> beam.Map(count_ones)
            | 'Format' >> beam.ParDo(FormatDoFn()))

        # Write to BigQuery.
        # pylint: disable=expression-not-assigned
        transformed | 'Write' >> beam.io.WriteToBigQuery(
            known_args.output_table,
            schema=BQ_SCHEMA,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  main()