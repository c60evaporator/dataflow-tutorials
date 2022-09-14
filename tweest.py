# 参考 https://qiita.com/SuzuTomo2001/items/e4ca40d02f0b931467da
import json
import sys
import threading
import time


from requests_oauthlib import OAuth1Session

auth = None

last_time = 0

re_t_network = 16
re_t_http = 5
re_t_420 = 60

def __reset_backoff_time():
    global re_t_network
    global re_t_http
    global re_t_420
    re_t_network = 16
    re_t_http = 5
    re_t_420 = 60


class TweestError(Exception):
    pass


def set_auth(CK,CS,AT,AS):
    # authの作成
    global auth
    auth = OAuth1Session(CK,CS,AT,AS)


def __streaming_thread(request,func):
    global last_time
    last_time = time.time()
    for line in request.iter_lines(decode_unicode=True):

        last_time = time.time()
        if line :
            # 取得したJsonデータ(バイト列)を辞書形式に変換
            func(json.loads(line))


def start(params,func):
    while True:
        try:
            # リクエストを送る
            r = auth.post('https://stream.twitter.com/1.1/statuses/filter.json',
                            params = params,
                            stream = True)

            r.encoding = 'utf-8'

            # リクエストのステータスコードを確認
            if r.status_code == 200:
                __reset_backoff_time()

                thread = threading.Thread(target=__streaming_thread,args=([r,func]))
                thread.setDaemon(True)
                thread.start()

                # 90秒間受信データがない場合、whileを抜け再接続
                while time.time() - last_time < 90:
                    time.sleep(90 - (time.time() - last_time))

            elif r.status_code == 401:
                raise TweestError('404 : Unauthorized')
            elif r.status_code == 403:
                raise TweestError('403 : Forbidden')
            elif r.status_code == 406:
                raise TweestError('406 : Not Acceptable')
            elif r.status_code == 413:
                raise TweestError('413 : Too Long')
            elif r.status_code == 416:
                raise TweestError('416 : Range Unacceptable')
            elif r.status_code == 420:
                # 420エラーの場合、待機時間を2倍に伸ばす(制限なし)
                print(f'420 : Rate Limited. Recconecting... wait {re_t_420}s')
                time.sleep(re_t_http)
                re_t_http *= 2
            elif r.status_code == 503:
                # 再接続が必要なHTTPエラーの場合、待機時間を2倍に伸ばす(最大320秒)
                print(f'503 : Service Unavailable. Reconnecting... wait {re_t_http}s')
                time.sleep(re_t_http)
                re_t_http *= 2
                if re_t_http > 320:
                    raise TweestError('503 : Service Unavailable.')

            else:
                raise TweestError(f'HTTP ERRORE : {r.status_code}')

        except KeyboardInterrupt: # Ctrl + C で強制終了できる
            break
        except ConnectionError:
            time.sleep(re_t_network)
            re_t_network += 16
            if re_t_network > 250:
                raise TweestError('Network Error')
        except:
            raise