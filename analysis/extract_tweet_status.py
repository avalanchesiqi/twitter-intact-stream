#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" Entry program to extract tweet status and user status.
1. tweet_id_str, created_at, timestamp_ms, user_id_str,
   original_lang, retweeted_lang, quoted_lang,
   original_vids, retweeted_vids, quoted_vids,
   original_mentions, retweeted_mentions, quoted_mentions,
   original_hashtags, retweeted_hashtags, quoted_hashtags,
   original_geo, retweeted_geo, quoted_geo,
   original_coordinates, retweeted_coordinates, quoted_coordinates,
   original_place, retweeted_place, quoted_place,
   original_sensitive, retweeted_sensitive, quoted_sensitive,
   original_filter, retweeted_filter, quoted_filter,
   original_retweet_count, retweeted_retweet_count, quoted_retweet_count,
   original_favorite_count, retweeted_favorite_count, quoted_favorite_count,
   original_user_followers_count, retweeted_user_followers_count, quoted_user_followers_count,
   original_user_friends_count, retweeted_user_friends_count, quoted_user_friends_count,
   original_user_statuses_count, retweeted_user_statuses_count, quoted_user_statuses_count,
   original_user_favourites_count, retweeted_user_favourites_count, quoted_user_favourites_count,
   reply_tweet_id_str, retweeted_tweet_id_str, quoted_tweet_id_str,
   reply_user_id_str, retweeted_user_id_str, quoted_user_id_str,
   original_text, retweeted_text, quoted_text
2. ratemsg, timestamp_ms, track

Usage: python extract_tweet_status.py
Input data files: ../data/[app_name]/[app_name]_*/*.bz2
Output data files: ../data/[app_name]_out/[app_name]_*/tweet_stats/*.bz2, ../data/[app_name]_out/[app_name]_*/timestamp/*.txt
Time: ~20M
"""

import sys, os

sys.path.append(os.path.join(os.path.dirname(__file__), '../'))
from utils.helper import Timer
from analysis.tweet_extractor import TweetExtractor


def extract_status(input_dir, output_dir, proc_num):
    """Extract tweet status from given folder, output in output_dir."""
    extractor = TweetExtractor(input_dir, output_dir)
    extractor.set_proc_num(proc_num)
    extractor.extract()


if __name__ == '__main__':
    app_name = 'covid'

    timer = Timer()
    timer.start()

    input_dir = '../data/{0}'.format(app_name)
    output_dir = '../data/{0}_out'.format(app_name)

    proc_num = 24
    extract_status(input_dir, output_dir, proc_num)

    timer.stop()
