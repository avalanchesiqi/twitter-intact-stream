# -*- coding: utf-8 -*-

""" Extract tweet status from full tweet.
1. tweet_id_str, created_at, timestamp_ms, user_id_str,
   original_lang, retweeted_lang, quoted_lang,
   original_vids, retweeted_vids, quoted_vids,
   original_mentions, retweeted_mentions, quoted_mentions,
   original_hashtags, retweeted_hashtags, quoted_hashtags,
   original_geoname, retweeted_geoname, quoted_geoname,
   original_countrycode, retweeted_countrycode, quoted_countrycode,
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
"""

import sys, os, bz2, re, json, logging, logging.config
from multiprocessing import Process, Queue

sys.path.append(os.path.join(os.path.dirname(__file__), '../'))
from utils.helper import strify, str2obj, obj2str


class TweetExtractor(object):
    """ Tweet Object Extractor Class.

    :param input_dir: directory that contains all tweet bz2 files
    :param output_dir: directory that composes of 2 folders -- tweet_stats and user_stats

    For a tweet, the dictionaries must include the following fields:

    id:               The integer representation of the unique identifier for this Tweet.
    ******
    entities:         Entities provide structured data from Tweets including resolved URLs, media, hashtags
                      and mentions without having to parse the text to extract that information.
                      ******
                      # We only care about urls information at this moment.
                      urls:       Optional. The URL of the video file
                                  Potential fields:
                                  * url            The t.co URL that was extracted from the Tweet text
                                  * expanded_url   The resolved URL
                                  * display_url	   Not a valid URL but a string to display instead of the URL
                                  * indices	       The character positions the URL was extracted from
                      ******
    ******
    retweeted_status: entities: urls: expanded_url
                      extended_tweet: entities: urls: expanded_url
    ******
    quoted_status:    entities: urls: expanded_url
                      extended_tweet: entities: urls: expanded_url
    """

    def __init__(self, input_dir, output_dir, proc_num=1):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.proc_num = proc_num

        os.makedirs(self.output_dir, exist_ok=True)
        self.logger = None
        self._setup_logger('tweetextractor')

    def set_proc_num(self, n):
        """Set the number of processes used in extracting."""
        self.proc_num = n

    def _setup_logger(self, logger_name):
        """Set logger from conf file."""
        log_dir = '../log/'
        os.makedirs(log_dir, exist_ok=True)
        logging.config.fileConfig('../conf/logging.conf')
        self.logger = logging.getLogger(logger_name)

    @staticmethod
    def _replace_with_nan(obj):
        if obj is None or len(obj) == 0:
            return 'N'
        else:
            return obj

    @staticmethod
    def _replace_comma_space(text):
        return re.sub(',\\s*|\s+', ' ', text)

    @staticmethod
    def _extract_vid_from_expanded_url(expanded_url):
        if 'watch?' in expanded_url and 'v=' in expanded_url:
            vid = expanded_url.split('v=')[1][:11]
        elif 'youtu.be' in expanded_url:
            vid = expanded_url.rsplit('/', 1)[-1][:11]
        else:
            return None
        # valid condition: contains only alphanumeric, dash or underline
        valid = re.match('^[\w-]+$', vid) is not None
        if valid and len(vid) == 11:
            return vid
        return None

    def _expanded_urls(self, urls):
        expanded_urls = []
        for url in urls:
            if url['expanded_url'] is not None:
                expanded_urls.append(url['expanded_url'])

        vids = set()
        for expanded_url in expanded_urls:
            vid = self._extract_vid_from_expanded_url(expanded_url)
            if vid is not None:
                vids.add(vid)
        return vids

    def _extract_vids(self, tweet):
        original_urls = []
        retweeted_urls = []
        quoted_urls = []
        try:
            original_urls.extend(tweet['entities']['urls'])
        except KeyError:
            pass
        try:
            retweeted_urls.extend(tweet['retweeted_status']['entities']['urls'])
        except KeyError:
            pass
        try:
            retweeted_urls.extend(tweet['retweeted_status']['extended_tweet']['entities']['urls'])
        except KeyError:
            pass
        try:
            quoted_urls.extend(tweet['quoted_status']['entities']['urls'])
        except KeyError:
            pass
        try:
            quoted_urls.extend(tweet['quoted_status']['extended_tweet']['entities']['urls'])
        except KeyError:
            pass

        original_vids = self._replace_with_nan(self._expanded_urls(original_urls))
        retweeted_vids = self._replace_with_nan(self._expanded_urls(retweeted_urls))
        quoted_vids = self._replace_with_nan(self._expanded_urls(quoted_urls))
        return original_vids, retweeted_vids, quoted_vids

    def _extract_hashtags(self, tweet):
        original_hashtags = set()
        retweeted_hashtags = set()
        quoted_hashtags = set()
        try:
            for hashtag in tweet['entities']['hashtags']:
                original_hashtags.add(hashtag['text'])
        except KeyError:
            pass
        try:
            for hashtag in tweet['retweeted_status']['entities']['hashtags']:
                retweeted_hashtags.add(hashtag['text'])
        except KeyError:
            pass
        try:
            for hashtag in tweet['retweeted_status']['extended_tweet']['entities']['hashtags']:
                retweeted_hashtags.add(hashtag['text'])
        except KeyError:
            pass
        try:
            for hashtag in tweet['quoted_status']['entities']['hashtags']:
                quoted_hashtags.add(hashtag['text'])
        except KeyError:
            pass
        try:
            for hashtag in tweet['quoted_status']['extended_tweet']['entities']['hashtags']:
                quoted_hashtags.add(hashtag['text'])
        except KeyError:
            pass
        original_hashtags = self._replace_with_nan(original_hashtags)
        retweeted_hashtags = self._replace_with_nan(retweeted_hashtags)
        quoted_hashtags = self._replace_with_nan(quoted_hashtags)
        return original_hashtags, retweeted_hashtags, quoted_hashtags

    def _extract_mentions(self, tweet):
        original_mentions = set()
        retweeted_mentions = set()
        quoted_mentions = set()
        try:
            for user_mention in tweet['entities']['user_mentions']:
                if user_mention['id_str'] is not None:
                    original_mentions.add(user_mention['id_str'])
        except KeyError:
            pass
        try:
            for user_mention in tweet['retweeted_status']['entities']['user_mentions']:
                if user_mention['id_str'] is not None:
                    retweeted_mentions.add(user_mention['id_str'])
        except KeyError:
            pass
        try:
            for user_mention in tweet['retweeted_status']['extended_tweet']['entities']['user_mentions']:
                if user_mention['id_str'] is not None:
                    retweeted_mentions.add(user_mention['id_str'])
        except KeyError:
            pass
        try:
            for user_mention in tweet['quoted_status']['entities']['user_mentions']:
                if user_mention['id_str'] is not None:
                    quoted_mentions.add(user_mention['id_str'])
        except KeyError:
            pass
        try:
            for user_mention in tweet['quoted_status']['extended_tweet']['entities']['user_mentions']:
                if user_mention['id_str'] is not None:
                    quoted_mentions.add(user_mention['id_str'])
        except KeyError:
            pass
        original_mentions = self._replace_with_nan(original_mentions)
        retweeted_mentions = self._replace_with_nan(retweeted_mentions)
        quoted_mentions = self._replace_with_nan(quoted_mentions)
        return original_mentions, retweeted_mentions, quoted_mentions

    def _extract_entities(self, tweet, field):
        if field in tweet:
            tweet_id_str = tweet[field]['id_str']
            user_id_str = tweet[field]['user']['id_str']
            user_location = tweet[field]['user']['location']
            if 'lang' in tweet[field]:
                lang = tweet[field]['lang']
            else:
                lang = 'N'

            if tweet[field]['place'] is not None:
                geo = self._replace_comma_space(tweet[field]['place']['full_name'])
                cc = self._replace_comma_space(tweet[field]['place']['country_code'])
            else:
                geo = 'N'
                cc = 'N'
            filter = tweet[field]['filter_level']

            retweet_count = tweet[field]['retweet_count']
            favorite_count = tweet[field]['favorite_count']

            user_followers_count = tweet[field]['user']['followers_count']
            user_friends_count = tweet[field]['user']['friends_count']
            user_statuses_count = tweet[field]['user']['statuses_count']
            user_favourites_count = tweet[field]['user']['favourites_count']

            if 'extended_tweet' in tweet[field] and 'full_text' in tweet[field]['extended_tweet']:
                text = self._replace_comma_space(tweet[field]['extended_tweet']['full_text'])
            else:
                text = self._replace_comma_space(tweet[field]['text'])
        else:
            tweet_id_str = 'N'
            user_id_str = 'N'
            user_location = 'N'
            lang = 'N'

            geo = 'N'
            cc = 'N'
            filter = 'N'

            retweet_count = 'N'
            favorite_count = 'N'

            user_followers_count = 'N'
            user_friends_count = 'N'
            user_statuses_count = 'N'
            user_favourites_count = 'N'

            text = 'N'
        return (tweet_id_str, user_id_str, user_location,
                lang, geo, cc, filter,
                retweet_count, favorite_count,
                user_followers_count, user_friends_count, user_statuses_count, user_favourites_count,
                text)

    def extract(self):
        self.logger.debug('**> Start extracting tweet status from tweet bz2 files...')

        processes = []
        filequeue = Queue()

        for root, dirs, files in os.walk(self.input_dir):
            for base_dir in dirs:
                output_tweet_dir = os.path.join(self.output_dir, base_dir, 'tweet_stats')
                os.makedirs(output_tweet_dir, exist_ok=True)
                output_ts_dir = os.path.join(self.output_dir, base_dir, 'timestamp')
                os.makedirs(output_ts_dir, exist_ok=True)

        for subdir, _, files in os.walk(self.input_dir):
            for f in sorted(files):
                filename, filetype = os.path.join(subdir, f).split('.')
                base_dir, filename = filename.split('/')[-2:]
                if filetype == 'bz2':
                    output_tweet_path = os.path.join(self.output_dir, base_dir, 'tweet_stats', '{0}.bz2'.format(filename))
                    if not os.path.exists(output_tweet_path):
                        filequeue.put(os.path.join(subdir, f))

        for w in range(self.proc_num):
            p = Process(target=self._extract_tweet, args=(filequeue,))
            p.daemon = True
            p.start()
            processes.append(p)

        for p in processes:
            p.join()

        self.logger.debug('**> Finish extracting tweet status from tweet bz2 files.')

    def _extract_tweet(self, filequeue):
        while not filequeue.empty():
            filepath = filequeue.get()
            try:
                filedata = bz2.BZ2File(filepath, mode='r')
            except:
                self.logger.warn('Exists corrupted bz2 file {0} in dataset folder'.format(filepath))
                continue
            filename, filetype = filepath.split('.')
            base_dir, filename = filename.split('/')[-2:]
            suffix = base_dir.split('_')[-1]

            tweet_output_path = os.path.join(self.output_dir, base_dir, 'tweet_stats', '{0}.bz2'.format(filename))
            tweet_output = bz2.open(tweet_output_path, 'wt')
            ts_output_path = os.path.join(self.output_dir, base_dir, 'timestamp', '{0}.txt'.format(filename))
            ts_output = open(ts_output_path, 'w')

            visited_tid = set()
            for line in filedata:
                try:
                    if line.rstrip():
                        tweet_json = json.loads(line)

                        # 2. ratemsg, timestamp_ms, track
                        if 'limit' in tweet_json:
                            # rate limit message
                            # {"limit":{"track":283540,"timestamp_ms":"1483189188944"}}
                            ratemsg_ts = tweet_json['limit']['timestamp_ms']
                            ratemsg_track = tweet_json['limit']['track']
                            tweet_output.write('{0}_{1},{2},{3}\n'.format('ratemsg', suffix, ratemsg_ts, ratemsg_track))
                            ts_output.write('{2},{0}_{1},{3}\n'.format('ratemsg', suffix, ratemsg_ts, ratemsg_track))
                            continue

                        if 'id_str' not in tweet_json:
                            continue

                        # 1. tweet_id_str, created_at, timestamp_ms, user_id_str,
                        #    original_lang, retweeted_lang, quoted_lang,
                        #    original_vids, retweeted_vids, quoted_vids,
                        #    original_mentions, retweeted_mentions, quoted_mentions,
                        #    original_hashtags, retweeted_hashtags, quoted_hashtags,
                        #    original_geoname, retweeted_geoname, quoted_geoname,
                        #    original_countrycode, retweeted_countrycode, quoted_countrycode,
                        #    original_filter, retweeted_filter, quoted_filter,
                        #    original_retweet_count, retweeted_retweet_count, quoted_retweet_count,
                        #    original_favorite_count, retweeted_favorite_count, quoted_favorite_count,
                        #    original_user_followers_count, retweeted_user_followers_count, quoted_user_followers_count,
                        #    original_user_friends_count, retweeted_user_friends_count, quoted_user_friends_count,
                        #    original_user_statuses_count, retweeted_user_statuses_count, quoted_user_statuses_count,
                        #    original_user_favourites_count, retweeted_user_favourites_count, quoted_user_favourites_count,
                        #    reply_tweet_id_str, retweeted_tweet_id_str, quoted_tweet_id_str,
                        #    reply_user_id_str, retweeted_user_id_str, quoted_user_id_str,
                        #    original_text, retweeted_text, quoted_text
                        tweet_id = tweet_json['id_str']
                        if tweet_id in visited_tid:
                            continue

                        created_at = obj2str(str2obj(tweet_json['created_at'], fmt='tweet'), fmt='youtube')
                        timestamp_ms = tweet_json['timestamp_ms']
                        user_id_str = tweet_json['user']['id_str']
                        if 'lang' in tweet_json:
                            lang = tweet_json['lang']
                        else:
                            lang = 'N'

                        original_vids, retweeted_vids, quoted_vids = self._extract_vids(tweet_json)
                        original_mentions, retweeted_mentions, quoted_mentions = self._extract_mentions(tweet_json)
                        original_hashtags, retweeted_hashtags, quoted_hashtags, = self._extract_hashtags(tweet_json)

                        if tweet_json['place'] is not None:
                            original_geo = self._replace_comma_space(tweet_json['place']['full_name'])
                            original_cc = self._replace_comma_space(tweet_json['place']['country_code'])
                        else:
                            original_geo = 'N'
                            original_cc = 'N'

                        original_filter = tweet_json['filter_level']

                        original_retweet_count = tweet_json['retweet_count']
                        original_favorite_count = tweet_json['favorite_count']

                        original_user_followers_count = tweet_json['user']['followers_count']
                        original_user_friends_count = tweet_json['user']['friends_count']
                        original_user_statuses_count = tweet_json['user']['statuses_count']
                        original_user_favourites_count = tweet_json['user']['favourites_count']

                        reply_tweet_id_str = self._replace_with_nan(tweet_json['in_reply_to_status_id_str'])
                        reply_user_id_str = self._replace_with_nan(tweet_json['in_reply_to_user_id_str'])

                        if 'extended_tweet' in tweet_json and 'full_text' in tweet_json['extended_tweet']:
                            text = self._replace_comma_space(tweet_json['extended_tweet']['full_text'])
                        elif tweet_json['text'] is not None:
                            text = self._replace_comma_space(tweet_json['text'])
                        else:
                            text = 'N'

                        retweeted_tweet_id_str, retweeted_user_id_str, retweeted_user_location,\
                        retweeted_lang, retweeted_geo, retweeted_cc, retweeted_filter, \
                        retweeted_retweet_count, retweeted_favorite_count, retweeted_user_followers_count, \
                        retweeted_user_friends_count, retweeted_user_statuses_count, retweeted_user_favourites_count, \
                        retweeted_text = self._extract_entities(tweet_json, 'retweeted_status')

                        quoted_tweet_id_str, quoted_user_id_str, quoted_user_location,\
                        quoted_lang, quoted_geo, quoted_cc, quoted_filter, \
                        quoted_retweet_count, quoted_favorite_count, quoted_user_followers_count, \
                        quoted_user_friends_count, quoted_user_statuses_count, quoted_user_favourites_count, \
                        quoted_text = self._extract_entities(tweet_json, 'quoted_status')

                        tweet_output.write('{0},{1},{2},{3},'
                                           '{4},{5},{6},'
                                           '{7},{8},{9},'
                                           '{10},{11},{12},'
                                           '{13},{14},{15},'
                                           '{16},{17},{18},'
                                           '{19},{20},{21},'
                                           '{22},{23},{24},'
                                           '{25},{26},{27},'
                                           '{28},{29},{30},'
                                           '{31},{32},{33},'
                                           '{34},{35},{36},'
                                           '{37},{38},{39},'
                                           '{40},{41},{42},'
                                           '{43},{44},{45},'
                                           '{46},{47},{48},'
                                           '{49},{50},{51}\n'
                                           .format(tweet_id, created_at, timestamp_ms, user_id_str,
                                                   lang, retweeted_lang, quoted_lang,
                                                   strify(original_vids, delimiter=';'), strify(retweeted_vids, delimiter=';'), strify(quoted_vids, delimiter=';'),
                                                   strify(original_mentions, delimiter=';'), strify(retweeted_mentions, delimiter=';'), strify(quoted_mentions, delimiter=';'),
                                                   strify(original_hashtags, delimiter=';'), strify(retweeted_hashtags, delimiter=';'), strify(quoted_hashtags, delimiter=';'),
                                                   original_geo, retweeted_geo, quoted_geo,
                                                   original_cc, retweeted_cc, quoted_cc,
                                                   original_filter, retweeted_filter, quoted_filter,
                                                   original_retweet_count, retweeted_retweet_count, quoted_retweet_count,
                                                   original_favorite_count, retweeted_favorite_count, quoted_favorite_count,
                                                   original_user_followers_count, retweeted_user_followers_count, quoted_user_followers_count,
                                                   original_user_friends_count, retweeted_user_friends_count, quoted_user_friends_count,
                                                   original_user_statuses_count, retweeted_user_statuses_count, quoted_user_statuses_count,
                                                   original_user_favourites_count, retweeted_user_favourites_count, quoted_user_favourites_count,
                                                   reply_tweet_id_str, retweeted_tweet_id_str, quoted_tweet_id_str,
                                                   reply_user_id_str, retweeted_user_id_str, quoted_user_id_str,
                                                   text, retweeted_text, quoted_text))
                        ts_output.write('{0},{1}\n'.format(timestamp_ms, tweet_id))
                        visited_tid.add(tweet_id)

                except EOFError:
                    self.logger.error('EOFError: {0} ended before the logical end-of-stream was detected,'.format(filename))

            tweet_output.close()
            ts_output.close()
            filedata.close()
            self.logger.debug('{0} done!'.format(filepath))
            print('{0} done!'.format(filepath))
