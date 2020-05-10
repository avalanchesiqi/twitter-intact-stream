#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" Plot the number of tweets temporally.

Usage: python plot_tweet_volume.py
Input data files: ../data/[app_name]_out/[app_name]_*/ts_[app_name]_*.bz2, ../data/[app_name]_out/complete_ts_[app_name]_*.bz2
Time: ~1M
"""

import sys, os, platform, bz2
from datetime import datetime, timedelta
import numpy as np

import matplotlib as mpl
if platform.system() == 'Linux':
    mpl.use('Agg')  # no UI backend

import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

sys.path.append(os.path.join(os.path.dirname(__file__), '../'))
from utils.helper import Timer, count_track, concise_fmt


def main():
    timer = Timer()
    timer.start()

    app_name = 'covid'
    archive_dir = '../data/{0}_out'.format(app_name)

    # to modify -- the timestamp when the crawlers are started
    # 2020-03-23 0:02:00 UTC
    init_timestamp = 1584928800

    # =============== Part1: compute hit count and miss count from a single crawler, grouped by hour ===============
    start_timestamp = init_timestamp
    current_hit = 0
    current_miss = []
    last_track = None

    hit_list = []
    miss_list = []

    sample_ts_datefile = os.path.join(archive_dir, '{0}_all/ts_{0}_all.bz2'.format(app_name))
    with bz2.BZ2File(sample_ts_datefile, mode='r') as fin:
        for line in fin:
            split_line = line.decode('utf-8').rstrip().split(',')
            if len(split_line) == 2:
                ts, tid = split_line
                ts = int(ts[:-3])
                if start_timestamp <= ts <= start_timestamp + 3600:
                    current_hit += 1
                elif ts > start_timestamp + 3600:
                    hit_list.append(current_hit)
                    miss_list.append(count_track(current_miss, start_with_rate=True))

                    current_hit = 1
                    current_miss = [last_track]
                    start_timestamp += 3600
            elif split_line[1].startswith('ratemsg'):
                ts, _, track = split_line
                track = int(track)
                ts = int(ts[:-3])
                if start_timestamp <= ts:
                    if len(current_miss) == 0:
                        if last_track is None:
                            current_miss.append(0)
                        else:
                            current_miss.append(last_track)
                    current_miss.append(track)
                last_track = track

    hit_list.append(current_hit)
    miss_list.append(count_track(current_miss, start_with_rate=True))
    total_list = [x + y for x, y in zip(hit_list, miss_list)]

    # =============== Part2: compute hit count and miss count from multiple subcrawlers, grouped by hour ===============
    start_timestamp = init_timestamp
    current_hit = 0
    gt_hit_list = []

    complete_ts_datefile = os.path.join(archive_dir, 'complete_ts_{0}.bz2'.format(app_name))
    with bz2.BZ2File(complete_ts_datefile, mode='r') as fin:
        for line in fin:
            split_line = line.decode('utf-8').rstrip().split(',')
            if len(split_line) == 2:
                ts, tid = split_line
                ts = int(ts[:-3])
                if start_timestamp <= ts <= start_timestamp + 3600:
                    current_hit += 1
                elif ts > start_timestamp + 3600:
                    gt_hit_list.append(current_hit)

                    current_hit = 1
                    start_timestamp += 3600

    gt_hit_list.append(current_hit)

    # =============== Part3: report stats ===============
    num_hit = sum(hit_list)
    num_miss = sum(miss_list)
    num_total = sum(total_list)
    num_gt_hit = sum(gt_hit_list)
    print('single crawl: {0:,} retrieved tweets, {1:,} missing tweets, {2:,} estimated total tweets, {3:.2f}% sampling rate'.format(num_hit, num_miss, num_total, 100 * num_hit / num_total))
    print('multiple subcrawls: {0:,} retrieved tweets, {1:.2f}% sampling rate'.format(num_gt_hit, 100 * num_gt_hit / num_total))

    # =============== Part4: plot hourly counts and hourly sampling rates ===============
    blue = '#6495ed'
    red = '#ff6347'
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4))

    init_dt = datetime.utcfromtimestamp(init_timestamp)

    x_axis = [init_dt + timedelta(hours=x) for x in range(len(hit_list))]

    ax1.plot_date(x_axis, hit_list, '-', color=red, marker='o', mfc='none', mec=red, ms=4, label='single: {0:,}'.format(num_hit))
    ax1.plot_date(x_axis, gt_hit_list, '-', color=blue, marker='o', mfc='none', mec=blue, ms=4, label='multiple: {0:,}'.format(num_gt_hit))
    ax1.plot_date(x_axis, total_list, '--', color='k', marker='x', ms=4, label='estimated: {0:,}'.format(num_total))
    ax1.yaxis.set_major_formatter(FuncFormatter(concise_fmt))
    ax1.set_yticks([0, 180000, 1000000, 2000000])
    ax1.set_ylim([0, 3000000])
    ax1.set_ylabel('#tweets', fontsize=12)
    ax1.legend(frameon=False, fontsize=11, ncol=1, fancybox=False, shadow=True, loc='upper left')

    ax2.plot_date(x_axis, np.array(hit_list) / np.array(total_list), '-', color=red, marker='o', mfc='none', mec=red, ms=4, label='single: {0:.4f}'.format(num_hit / num_total))
    ax2.plot_date(x_axis, np.array(gt_hit_list) / np.array(total_list), '-', color=blue, marker='o', mfc='none', mec=blue, ms=4, label='multiple: {0:.4f}'.format(num_gt_hit / num_total))
    ax2.set_ylabel('sampling rate', fontsize=12)
    ax2.set_ylim([-0.05, 1.05])
    ax2.legend(frameon=False, fontsize=11, ncol=1, fancybox=False, shadow=True, loc='center left')

    for ax in (ax1, ax2):
        ax.set_xlabel('UTC time', fontsize=12)
        ax.tick_params(axis='both', which='major', labelsize=11)
        ax.tick_params(axis='x', rotation=60)
        ax.spines['right'].set_visible(False)
        ax.spines['top'].set_visible(False)

    timer.stop()

    plt.tight_layout()
    plt.savefig('./tweet_volume.png', bbox_inches='tight')
    if not platform.system() == 'Linux':
        plt.show()


if __name__ == '__main__':
    main()
