import sys, os, bz2, json, time, logging
from datetime import datetime
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
from multiprocessing import Process, Queue


class StdOutListener(StreamListener):
    """ A listener handles tweets that are received from the stream.
    This is a basic listener that just prints received tweets to stdout.
    """
    def __init__(self, name, output_dir):
        StreamListener.__init__(self)
        self.name = name
        self.output_dir = output_dir

        if not os.path.exists(output_dir):
            os.mkdir(output_dir)

        current_dt = datetime.utcfromtimestamp(int(time.time())).strftime('%Y-%m-%d')
        self.output = bz2.open(os.path.join(self.output_dir, '{0}.bz2'.format(current_dt)), 'at')
        self.next_day_ts = (datetime.strptime(current_dt, '%Y-%m-%d') - datetime(1970, 1, 1)).total_seconds() + 3600 * 24

    def on_data(self, data):
        if time.time() >= self.next_day_ts:
            self.output.close()
            current_dt = datetime.utcfromtimestamp(self.next_day_ts).strftime('%Y-%m-%d')
            self.output = bz2.open(os.path.join(self.output_dir, '{0}.bz2'.format(current_dt)), 'at')
            self.next_day_ts += 3600 * 24
        self.output.write(data)


if __name__ == '__main__':
    covid_keywords = ["coronavirus", "covid19", "covid", "covid–19", "COVIDー19", "pandemic", "covd", "ncov", "corona",
                      "corona virus", "sars-cov-2", "sarscov2", "koronavirus", "wuhancoronavirus", "wuhanvirus",
                      "wuhan virus", "chinese virus", "chinesevirus", "china", "wuhanlockdown", "wuhan", "kungflu",
                      "sinophobia", "n95", "world health organization", "cdc", "outbreak", "epidemic", "lockdown",
                      "panic buying", "panicbuying", "socialdistance", "social distance", "socialdistancing",
                      "social distancing"]
    youtube_keywords = ["youtube com", "youtu be"]

    combined_keywords = []
    for covid_keyword in covid_keywords:
        for youtube_keyword in youtube_keywords:
            combined_keywords.append(' '.join([covid_keyword, youtube_keyword]))

    with open('../conf/developer.key', 'r') as fin:
        key_dict = json.load(fin)

    crawler_conf = {'crawler_name': 'covid_youtube',
                    'output_dir': '../data/covid_youtube',
                    'key_token': key_dict['key31'],
                    "keywords": combined_keywords,
                    'languages': []
                    }

    app_name = crawler_conf['crawler_name']
    print('>>> app name: {0}'.format(app_name))
    os.makedirs('../log', exist_ok=True)
    logging.basicConfig(filename='../log/{0}_crawl.log'.format(app_name), filemode='w', format='%(asctime)s - %(message)s', level=logging.WARNING)

    os.makedirs(crawler_conf['output_dir'], exist_ok=True)
    listener = StdOutListener(name=crawler_conf['crawler_name'], output_dir=crawler_conf['output_dir'])
    auth = OAuthHandler(crawler_conf['key_token']['consumer_key'], crawler_conf['key_token']['consumer_secret'])
    auth.set_access_token(crawler_conf['key_token']['access_token'], crawler_conf['key_token']['access_secret'])
    disconnect_cnt = 0

    while True:
        try:
            stream = Stream(auth, listener)
            stream.filter(track=crawler_conf['keywords'], languages=crawler_conf['languages'])
        except Exception as e:
            disconnect_cnt += 1
            logging.error('Crawler {0} -- Disconnect cnt: {1}, msg: {2}'.format(crawler_conf['crawler_name'], disconnect_cnt, str(e)))
            continue
