#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" Merge all subcrawlers into one stream.
For the ground-truth stream, the lower bound is the merged tweets (as they are what we observed/what happened).
The upper bound is the number of merged tweets plus the sum of all rate limit messages,
assuming all missing tweets in each sub-crawler are disjointed, representing a upper bound of total missing tweets.

Usage: python merge_subcrawlers.py
Input data files: ../data/[app_name]_out/[app_name]_*/timestamp/*.txt
Output data files: ../data/[app_name]_out/[app_name]_*/ts_[app_name]_*.bz2, ../data/[app_name]_out/complete_ts_[app_name]_*.bz2
Time: ~1H
"""

import sys, os, bz2

sys.path.append(os.path.join(os.path.dirname(__file__), '../'))
from utils.helper import Timer, melt_snowflake, make_snowflake


def find_next_item(nextline_list):
    end_flag = all(v.rstrip() == '' for v in nextline_list)
    if end_flag:
        return None, None, True
    else:
        lst = []
        for item in nextline_list:
            if item.rstrip() == '':
                lst.append('END')
            else:
                lst.append(item)
        index_min = min(range(len(lst)), key=lst.__getitem__)
        return index_min, lst[index_min], False


def main():
    app_name = 'covid'
    if app_name == 'cyberbullying':
        target_suffix = ['1', '2', '3', '4', '5', '6', '7', '8', 'all']
    elif app_name == 'youtube' or app_name == 'covid':
        target_suffix = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', 'all']
    else:
        target_suffix = ['1', '2', '3', '4', '5', '6', '7', '8', 'all']

    archive_dir = '../data/{0}_out'.format(app_name)
    best_offset = 5000

    # merge timestamps
    timer = Timer()
    timer.start()

    for suffix_idx, suffix in enumerate(target_suffix):
        print('>>> Merging suffix {0}_{1}...'.format(app_name, suffix))
        visited_tid = set()
        ts_streaming_dict = {}
        for subdir, _, files in os.walk(os.path.join(archive_dir, '{0}_{1}'.format(app_name, suffix), 'timestamp')):
            for f in sorted(files):
                with open(os.path.join(subdir, f), 'r') as fin:
                    for line in fin:
                        split_line = line.rstrip().split(',')
                        if len(split_line) == 3:
                            ts_streaming_dict[str(make_snowflake(int(split_line[0]) - best_offset, 31, 31, suffix_idx))] = 'ratemsg{0},{1}'.format(suffix, split_line[2])
                        else:
                            tweet_id = split_line[1]
                            if tweet_id not in visited_tid:
                                ts_streaming_dict[tweet_id] = split_line[0]
                                visited_tid.add(tweet_id)

        with bz2.open(os.path.join(archive_dir, '{0}_{1}/ts_{0}_{1}.bz2'.format(app_name, suffix)), 'wt') as ts_output:
            for tid in sorted(ts_streaming_dict.keys()):
                if ts_streaming_dict[tid].startswith('ratemsg'):
                    ts = melt_snowflake(tid)[0]
                    ts_output.write('{0},{1}\n'.format(ts, ts_streaming_dict[tid]))
                else:
                    ts_output.write('{0},{1}\n'.format(ts_streaming_dict[tid], tid))
        print('>>> Finishing merging suffix {0}_{1}'.format(app_name, suffix))

    print('>>> Merging complete stream for {0}...'.format(app_name))
    inputfile_list = ['{0}_{1}/ts_{0}_{1}.bz2'.format(app_name, suffix) for suffix in target_suffix]
    inputfile_handles = [bz2.BZ2File(os.path.join(archive_dir, inputfile), mode='r') for inputfile in inputfile_list]
    visited_item_set = set()

    with bz2.open(os.path.join(archive_dir, 'complete_ts_{0}.bz2'.format(app_name)), 'wt') as ts_output:
        nextline_list = [inputfile.readline().decode('utf-8') for inputfile in inputfile_handles]

        while True:
            next_idx, next_item, end_flag = find_next_item(nextline_list)
            if end_flag:
                break
            # omit rate limit messages in the all crawler
            if 'ratemsg' not in next_item and next_item not in visited_item_set:
                ts_output.write(next_item)
                visited_item_set.add(next_item)
            nextline_list[next_idx] = inputfile_handles[next_idx].readline().decode('utf-8')

    for inputfile in inputfile_handles:
        inputfile.close()
    print('>>> Finishing merging complete stream for {0}'.format(app_name))

    timer.stop()


if __name__ == '__main__':
    main()
