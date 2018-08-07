#!/usr/bin/env python
# -*- coding: utf-8 -*-
# author: abekthink

import hashlib
import json
import os
import time
import traceback
import threading
import requests
import plparser

import urllib.parse as urlparse

# crawl radio guide
RADIO_GUIDE_SOURCE_FILE = "radio_guide_source.json"
RADIO_GUIDE_OUTPUT_FILE = "radio_guide.json"


class Timer(object):
    def __init__(self):
        pass

    def start(self):
        self._start_ts = time.time()

    def stop(self):
        self._stop_ts = time.time()
        return self._stop_ts - self._start_ts


class SessionThrottle(object):
    def __init__(self, requests_per_second=None):
        self.requests_per_second = requests_per_second
        self.total_requests = 0
        self.start_time = time.time()

    def set_requests_per_second(self, requests_per_second):
        self.requests_per_second = requests_per_second

    def run(self):
        if self.requests_per_second:
            now_time = time.time()
            total_seconds = self.total_requests / self.requests_per_second
            self.total_requests += 1
            delay = total_seconds - (now_time - self.start_time)
            if delay > 0.1:
                time.sleep(delay)
                return delay
        return 0


def url_net_loc(url):
    r = urlparse.urlsplit(url)
    return r.netloc


def is_url_localhost(url):
    loc = url_net_loc(url)
    return loc == 'localhost'


class HttpClient(object):
    def __init__(self, **kwargs):
        self.session_throttle = SessionThrottle(kwargs.get('requests_per_second', 0))
        self.http_timeout = kwargs.get('http_timeout', 10)

    def set_requests_per_second(self, requests_per_second):
        self.session_throttle.set_requests_per_second(requests_per_second)

    def set_http_timeout(self, http_timeout):
        self.http_timeout = http_timeout

    def get_url(self, url, proxy=False, method='GET', data=None, max_size=None, http_timeout=None,
                stream=False, throttle=True, ensure_utf8=True):
        print('GET URL works url = %s' % url)
        if throttle:
            delay = self.session_throttle.run()
            if delay:
                print('[INFO] GET URL throttle, delay for %.2f. url = %s' % (delay, url))

        headers = {'User-Agent': 'UniversalFeedParser/3.3 +http://feedparser.org/'}

        if stream:
            # 如果是流式音频的话，按照mozila来请求的话
            # shoutcast站点会直接返回网页而不是流式内容.
            # headers['User-Agent'] = 'curl/7.35.0'
            # headers['User-Agent'] = 'Mozilla/5.0'
            headers['User-Agent'] = 'iTunes/9.2.1 (Macintosh; Intel Mac OS X 10.5.8) AppleWebKit/533.16'
        if stream and proxy:
            print("[INFO] 'proxy' is not compatible with 'stream'")
            proxy = False
        if proxy and is_url_localhost(url):
            print("[INFO] 'proxy' is not compatible with url: 'localhost'")
            proxy = False
        ss = requests.session()
        res = None

        # fix url
        if url.startswith("mms"):
            print('[ERROR] URL is invalid. url = %s' % url)
            return None
        url = fix_url(url)

        try:
            timeout = http_timeout or self.http_timeout
            # connect timeout and read timeout.
            kwargs = {
                'timeout': (timeout * 0.5, timeout * 0.8),
                'headers': headers
            }
            if stream:
                kwargs['stream'] = stream
            if method == 'POST':
                kwargs['data'] = data

            if proxy:
                kwargs['proxies'] = {'http': 'http://%s:%d' % ('localhost', 64441)}

            res = ss.request(method, url, **kwargs)
        except requests.exceptions.InvalidURL as e:
            error_tag = 'InvalidURL'
            assert (res is None)
        except requests.ConnectionError as e:
            error_tag = 'ConnectionError'
            assert (res is None)
        except requests.TooManyRedirects as e:
            error_tag = 'TooManyRedirects'
            assert (res is None)
        except:
            traceback.print_exc()
            error_tag = 'OtherError'
            assert (res is None)

        # connection problem.
        if res is None:
            ss.close()
            print('[ERROR] GET URL connect. url = %s, error_tag = %s' % (url, error_tag))
            return None

        # not modified.
        if res.status_code == 304:
            ss.close()
            print('[INFO] GET URL content not modified. url = %s' % url)

        # http server problem.
        if res.status_code != 200 and res.status_code != 304:
            ss.close()
            print('[ERROR] GET URL http. code = %d, url = %s' % (res.status_code, url))
            return None

        # how to interpret data.
        if stream:
            value = parse_stream_url_data(res)
            if not value:
                ss.close()
                print('[ERROR] GET URL data fn. url = %s' % url)
                return None
        else:
            value = res.content
        ss.close()

        bytes().decode()
        if value is None:
            return None

        # control size.
        size = len(value)
        if max_size and size > max_size:
            print('[ERROR] GET URL content exceeds max_size. size = %d, url = %s' % (size, url))
            return None

        # ensure utf8.
        if ensure_utf8:
            # utf-8 or not.
            try:
                value = value.decode('utf-8')
            except:
                print('[ERROR] GET URL content not utf-8. url = %s' % url)
                return None

        # update headers.
        if 'Last-Modified' in res.headers:
            headers['If-Modified-Since'] = res.headers['Last-Modified']
        if 'ETag' in res.headers:
            headers['If-None-Match'] = res.headers['ETag']

        return value


def get_sha1_key(s):
    return hashlib.sha1(s).hexdigest()


def parse_playlist_data(data):
    if not data:
        return []
    try:
        pls = plparser.parse(filedata=data)
        xs = map(lambda x: x.File.strip(), pls.Tracks)
    except:
        traceback.print_exc()
        xs = []
    return xs


def parse_stream_url_data(res):
    headers = res.headers
    ct = headers.get('content-type', '')

    # radionet: tedtalks
    # http://video.ted.com/talk/podcast/2016X/None/ToniMac_2016X.mp3
    # content-type = 'application/octet-stream'
    a = res.url
    path = urlparse.urlsplit(res.url).path
    ext = os.path.splitext(path)[-1]
    if ct and ct == 'application/octet-stream' and ext in ('.mp3',):
        ct = 'audio/mp3'
    # print ct, ext

    if ct and not ct.startswith('audio/') and not ct.startswith('video/'):
        print('>>>>> PARSE STREAM URL DATA FAILED. ct = %s, url = %s' % (ct, res.url))
        return None

    d = {}
    s = ''
    fields = ('icy-genre', 'icy-name', 'icy-url', 'icy-description')
    for f in fields:
        if f in headers:
            try:
                v = headers[f].decode('utf-8').encode('utf-8')
            except:
                v = ''
            if f == 'icy-url' and v:
                v = fix_url(v)
            d[f] = v

    d['url'] = res.url
    d['server'] = headers.get('server', '')
    d['icy-ct'] = ct
    value = json.dumps(d)
    return value


def fix_url(url):
    result = url
    if not (url.startswith('http://') or url.startswith('https://')):
        result = 'http://' + url
    return result


class OutputFile(object):
    def __init__(self, output_file_name):
        if output_file_name:
            self.file = open(output_file_name, "w")
            self.mutex = threading.Lock()

    def write_json(self, json_data):
        if self.mutex.acquire(True):
            try:
                line = json.dumps(json_data) + "\n"
                self.file.write(line.encode('utf-8'))
                self.file.flush()
            finally:
                self.mutex.release()

    def destroy(self):
        try:
            self.file.flush()
            self.file.close()
        except:
            traceback.print_stack()


if __name__ == "__main__":
    output_file = OutputFile("a.output")
    output_file.write_json({"a":123, "b":"123"})
    output_file.write_json({"a":323, "b":"323"})
    output_file.write_json({"a":133, "b":"133"})
    output_file.destroy()
