#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" Test Twitter developer keys.

Usage: python validate_twitter_keys.py
Input data files: ../conf/developer.key
"""

import json
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
from multiprocessing import Queue


class StdOutListener(StreamListener):
    """ A listener handles tweets that are received from the stream.
    This is a basic listener that just prints received tweets to stdout.
    """
    def __init__(self, name):
        StreamListener.__init__(self)
        self.name = name

    def on_data(self, data):
        json_data = json.loads(data)
        if 'id' in json_data:
            print('key {0} is working!'.format(self.name))
        else:
            print('key {0} is NOT working!'.format(self.name))
        return False


def start_streaming(crawler_conf):
    listener = StdOutListener(name=crawler_conf['app_name'])
    auth = OAuthHandler(crawler_conf['consumer_key'], crawler_conf['consumer_secret'])
    auth.set_access_token(crawler_conf['access_token'], crawler_conf['access_secret'])

    stream = Stream(auth, listener)
    stream.filter(track=["youtube", "youtu be"])


if __name__ == '__main__':
    with open('../conf/developer.key', 'r') as fin:
        key_dict = json.load(fin)

    num_keys = len(key_dict)

    processes = []
    crawler_queue = Queue()

    for i in range(num_keys):
        start_streaming(key_dict['key{0}'.format(i)])
