#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" Count the number of collected tweets, missing tweets (by rate limit message) and sampling rate in each subcrawler.

Usage: python count_ratemsg_in_subcrawlers.py
Input data files: ../data/[app_name]_out/[app_name]_*/ts_[app_name]_*.bz2, ../data/[app_name]_out/complete_ts_[app_name]_*.bz2
Time: ~20M
"""

import sys, os, bz2

sys.path.append(os.path.join(os.path.dirname(__file__), '../'))
from utils.helper import Timer, count_track


def main():
    app_name = 'covid'
    if app_name == 'cyberbullying':
        target_suffix = ['1', '2', '3', '4', '5', '6', '7', '8', 'all']
    elif app_name == 'youtube' or app_name == 'covid':
        target_suffix = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', 'all']
    else:
        target_suffix = ['1', '2', '3', '4', '5', '6', '7', '8', 'all']

    archive_dir = '../data/{0}_out'.format(app_name)

    timer = Timer()
    timer.start()

    est_num_tweet = 0

    for suffix in target_suffix:
        num_tweet = 0
        num_ratemsg = 0
        track_list = []
        with bz2.BZ2File(os.path.join(archive_dir, '{0}_{1}/ts_{0}_{1}.bz2'.format(app_name, suffix)), mode='r') as fin:
            for line in fin:
                line = line.decode('utf-8')
                if 'ratemsg' in line:
                    num_ratemsg += 1
                    track_list.append(int(line.rstrip().split(',')[2]))
                else:
                    num_tweet += 1
        num_miss = count_track(track_list)
        subcrawler_sampling_rate = num_tweet / (num_tweet + num_miss)
        print('>>> subcrawler {0}_{1: <3}, {2: >9d} retrieved tweets, {3: >7d} rate limit track, indicating {4: >9d} missing tweets, yielding {5: >6.2f}% sampling rate'
              .format(app_name, suffix, num_tweet, num_ratemsg, num_miss, 100 * subcrawler_sampling_rate))
        if suffix == 'all':
            est_num_tweet = num_tweet + num_miss

    gt_num_tweet = 0
    with bz2.BZ2File(os.path.join(archive_dir, 'complete_ts_{0}.bz2'.format(app_name)), mode='r') as fin:
        for _ in fin:
            gt_num_tweet += 1
    gt_sampling_rate = gt_num_tweet / est_num_tweet
    print('>>> complete_set {0}  , {1: >9d} retrieved tweets, {2: >7} rate limit track, estimating {3: >9d}   total tweets, yielding {4: >6.2f}% sampling rate'
          .format(app_name, gt_num_tweet, 'NaN', est_num_tweet, 100 * gt_sampling_rate))

    timer.stop()


if __name__ == '__main__':
    main()
