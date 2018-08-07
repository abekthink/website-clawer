#!/usr/bin/env python
# -*- coding: utf-8 -*-
# author: abekthink

import sys
import argparse
import urllib
import re

from asynclib import Producer, Consumer
from util import *


if sys.version_info[0] > 2:
    quote_plus = urllib.parse.quote_plus
else:
    quote_plus = urllib.quote_plus


ROOT_URL = "http://www.radioguide.fm"
GENRES_PATH = "/genre"

GENRE_PAGE_RE = (
    r"<li>.*?<div\s+class=\"inner\">.*?<a\s+href=\"(/genre/.*?)\".*?>(.*?)</a>.*?</div>.*?</li>"
)

STATION_RE = (
    r"<li\s+class=\"clearfix\">.*?"
    r"<div class=\"station-info2\">.*?"
    r"<a\s+href=\"(/.*?)\".*?>.*?"
    r"<strong>(.*?)</strong>.*?</div>"
    r".*?</li>"
)

GENRE_NAME_RE = (
    r"<a\s+href=\"/genre/.*?\">(.*?)</a>"
)

STATION_DETAIL_RE = (
    r"<div\s+class=\"player\">.*?"
    r"<span\s+class=\"logo\">.*?"
    r"<img\s+src=\"(.*?)\"\s+alt=\"(.*?)\">"
    r".*?</span>"
    r".*?</div>.*?"
    r"<div\s+class=\"station-info\">.*?"
    r"<strong>Country:</strong>\s+<a\s+href=\"/.*?\">(.*?)</a>.*?"
    r"<strong>Genre\(s\):</strong>(.*?)\|.*?"
    r"<strong>Rating:</strong>.*?<div.*?title=\"Rating:\s+(\d\.*\d*)\".*?>.*?</div>.*?"
    r"</div>"
)

STATION_FRAME_PATH_RE = (
    r"<iframe\s+name=\"playerContainer\".*?src=\"(.*?)\".*?>.*?</iframe>"
)

STATION_SOURCE_URL_RE = (
    r"\"setMedia\".*?{(.*?):.*?\"(.*?)\".*?}"
)

STATION_EMBEDED_SOURCE_URL_RE = (
    r"<embed.*?src=\"(.*?)\".*?>.*?<style>.*?</style>"
)


class StationProducer(Producer):
    def __init__(self, queue_size=2048000):
        Producer.__init__(self, queue_size)
        kwargs = {
            'requests_per_second': 5,
            'http_timeout': 60
        }
        self.http_client = HttpClient(**kwargs)

    def produce(self):
        print("[INFO]producer: begin to get all station info list(including title and page_url)")

        total_time1 = time.time()
        genres = self.get_genres(ROOT_URL + GENRES_PATH)
        genres_total = len(genres.items())

        station_total = 0
        for genre, genre_path in genres.items():
            time1 = time.time()
            station_pages = self.get_stations(genre, genre_path)
            station_genre_total = 0

            for station in station_pages:
                station_genre_total += 1
                yield station

            station_total += station_genre_total
            time2 = time.time()
            print("[INFO]producer: genres name: %s, station number: %d" % (genre, station_genre_total))
            print("[INFO]producer: get the station pages of the targeted genre using %d seconds" % (time2 - time1))

        total_time2 = time.time()
        print("[INFO]producer: genres number: %d, station number: %d" % (genres_total, station_total))
        print("[INFO]producer: get all station info list(including title and page_url) using %d seconds"
              % (total_time2 - total_time1))

        cur_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print("[INFO]producer: end to get all station info list(including title and page_url) at %s" % cur_time)

    def get_genres(self, website_url):
        html = self.http_client.get_url(website_url)
        if not html:
            print("[ERROR]producer: can not get the page of genres for %s" % website_url)
        result = re.findall(GENRE_PAGE_RE, html, flags=re.DOTALL)
        return dict(map(lambda x: (x[1], x[0]), result))

    def get_stations(self, genre, path):
        pathes = path.split("/")
        path = "/".join([quote_plus(p) for p in pathes])
        genre_url = ROOT_URL + path
        print("[INFO]producer: retrieving genre %s" % genre_url)

        index = 0
        while True:
            index += 1
            genre_page_url = "%s?page=%d" % (genre_url, index)
            # print("producer: retrieving the genre page %s" % origin)
            html = self.http_client.get_url(genre_page_url)
            if html:
                stations = re.findall(STATION_RE, html, flags=re.DOTALL)
                if len(stations) == 0:
                    break
                for station in stations:
                    yield station
            else:
                print("[ERROR]producer: can not get the page for %s, the genre is %s" % (genre_page_url, genre))
                break


class StationConsumer(Consumer):
    def __init__(self, queue, queue_timeout=5, consumer_id=0, output_file=None):
        Consumer.__init__(self, queue, queue_timeout)
        self.consumer_id = consumer_id
        self.output_file = output_file
        kwargs = {
            'requests_per_second': 5,
            'http_timeout': 60
        }
        self.http_client = HttpClient(**kwargs)

    def consume(self, task):
        station = self.get_station_detail(*task)
        if station:
            self.output_file.write_json(station)

    def get_station_detail(self, station_path, title):
        station_url = ROOT_URL + station_path
        html = self.http_client.get_url(station_url)
        if not html:
            print("[ERROR]consumer: can not get the page for %s" % station_url)
            return {}
        result = re.findall(STATION_DETAIL_RE, html, flags=re.DOTALL)

        if len(result) == 1:
            logo_url = ROOT_URL + result[0][0]
            desc = result[0][1].strip()
            country = result[0][2]
            genres_array = re.findall(GENRE_NAME_RE, result[0][3], flags=re.DOTALL)
            rating = result[0][4]
        else:
            print("[ERROR]consumer: can not get logo_url, desc, country, genres, or rating from the page %s"
                  % station_url)
            return {}

        iframe_path = re.findall(STATION_FRAME_PATH_RE, html, flags=re.DOTALL)
        if len(iframe_path) == 1:
            iframe_url = ROOT_URL + iframe_path[0]
        else:
            print("[ERROR]consumer: can not get the iframe url from the page %s" % station_url)
            return {}

        html = self.http_client.get_url(iframe_url)
        if not html:
            print("[ERROR]consumer: can not get the page for the iframe url %s, the station url %s"
                  % (iframe_url, station_url))
            return {}

        station_source_type = ""
        station_source_urls = re.findall(STATION_SOURCE_URL_RE, html, flags=re.DOTALL)
        if station_source_urls and len(station_source_urls) == 1:
            station_source_type = station_source_urls[0][0]
            station_source_url = station_source_urls[0][1]
        else:
            station_embeded_source_urls = re.findall(STATION_EMBEDED_SOURCE_URL_RE, html, flags=re.DOTALL)
            if station_embeded_source_urls and len(station_embeded_source_urls) == 1:
                station_source_url = station_embeded_source_urls[0]
            else:
                print("[ERROR]consumer: can not get the stream url from the page %s, the station url %s"
                      % (iframe_url, station_url))
                return {}

        return {
            "station_page_url": station_url,
            "station_source_url": station_source_url,
            "station_source_type": station_source_type,
            "logo_url": logo_url,
            "title": title,
            "desc": desc,
            "country": country,
            "genres": genres_array,
            "rating": rating,
            "generated_date": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        }


class StationStreamProducer(Producer):
    def __init__(self, queue_size=2048000):
        Producer.__init__(self, queue_size)
        self.url_black_list = {"http://Yes"}

    def produce(self):
        print("[INFO]producer: begin to get all station info from the input file")

        station_total = 0
        data_map = {}
        with open(RADIO_GUIDE_SOURCE_FILE) as input_file:
            for line in input_file:
                data = json.loads(line)
                if not data or 'station_source_url' not in data:
                    print("[WARN]producer: the data is invalid[data=%s]" % line)
                    continue

                if data['station_source_url'] in self.url_black_list:
                    print("[WARN]producer: the station source url of the data is invalid[data=%s]" % line)
                    continue

                if data['station_source_url'] in data_map:
                    continue
                else:
                    data_map[data['station_source_url']] = 1

                station_total += 1
                yield data

        print("[INFO]producer: station number: %d" % station_total)
        cur_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print("[INFO]producer: end to get all station info from the input file at %s" % cur_time)


class StationStreamConsumer(Consumer):
    def __init__(self, queue, queue_timeout=5, consumer_id=0, output_file=None):
        Consumer.__init__(self, queue, queue_timeout)
        self.consumer_id = consumer_id
        self.output_file = output_file
        kwargs = {
            'requests_per_second': 5,
            'http_timeout': 60
        }
        self.http_client = HttpClient(**kwargs)

    def consume(self, station):
        res = self.parse_source_url(station)
        if res:
            self.output_file.write_json(res)

    def parse_source_url(self, station):
        station_source_url = station['station_source_url']
        if not station_source_url or not station_source_url.strip():
            print("[WARN]consumer: the station source url is invalid[data=%s]" % station_source_url)
            return None

        station_source_url = station_source_url.strip()
        match = False
        playlist_exts = ['.m3u', '.m3u8', '.pls', '.xspf', '.xml']
        for ext in playlist_exts:
            if station_source_url.endswith(ext):
                match = True
                break

        stream_urls = []
        if not match:
            stream_url = {'url': station_source_url}
            stream_urls.append(stream_url)
        else:
            data = self.http_client.get_url(station_source_url)
            xs = parse_playlist_data(data)
            if xs:
                for x in xs:
                    if x:
                        stream_url = {'url': x}
                        stream_urls.append(stream_url)
            else:
                print("[ERROR]consumer: the station source url is invalid[data=%s]" % station_source_url)

        final_stream_urls = []
        if stream_urls and len(stream_urls) >= 1:
            for stream_url in stream_urls:
                res = self.http_client.get_url(stream_url['url'], stream=True)
                if res:
                    data = json.loads(res)
                    data.update(stream_url)
                    final_stream_urls.append(data)
            if final_stream_urls:
                station['stream_urls'] = final_stream_urls
                station['parsed_date'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

        return station


def crawl_radio_guide_source():
    begin_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print("[INFO]main: get all the stations from radioguide at %s" % begin_time)

    output_file = OutputFile(RADIO_GUIDE_SOURCE_FILE)

    thread_count = 8
    producer = StationProducer()
    producer.start()
    consumer_array = []
    for i in range(thread_count):
        consumer = StationConsumer(queue=producer.queue, queue_timeout=30, consumer_id=i, output_file=output_file)
        consumer_array.append(consumer)
        consumer.start()

    # waiting for producer and consumers finished
    producer.join()
    for consumer in consumer_array:
        consumer.join()

    output_file.destroy()
    end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print("[INFO]main: finish to get all the stations from radioguide at %s" % end_time)


def parse_radio_guide_station():
    begin_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print("[INFO]main: parse all the stations from radioguide at %s" % begin_time)

    output_file = OutputFile(RADIO_GUIDE_OUTPUT_FILE)

    thread_count = 8
    producer = StationStreamProducer()
    producer.start()
    consumer_array = []
    for i in range(thread_count):
        consumer = StationStreamConsumer(queue=producer.queue, queue_timeout=30, consumer_id=i, output_file=output_file)
        consumer_array.append(consumer)
        consumer.start()

    # waiting for producer and consumers finished
    producer.join()
    for consumer in consumer_array:
        consumer.join()

    output_file.destroy()
    end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print("[INFO]main: finish to parse all the stations from radioguide at %s" % end_time)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    args_array = ['--crawl-radioguide-source', '--parse-radioguide-station']
    for arg in args_array:
        parser.add_argument(arg, action="store_true")
    args = parser.parse_args()

    if args.crawl_radioguide_source:
        crawl_radio_guide_source()

    if args.parse_radioguide_station:
        parse_radio_guide_station()
