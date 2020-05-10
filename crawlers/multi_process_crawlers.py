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

        current_dt = datetime.utcfromtimestamp(int(time.time())).strftime('%Y-%m-%d-%H')
        self.output = bz2.open(os.path.join(self.output_dir, '{0}.bz2'.format(current_dt)), 'at')
        self.next_hour_ts = (datetime.strptime(current_dt, '%Y-%m-%d-%H') - datetime(1970, 1, 1)).total_seconds() + 3600

    def on_data(self, data):
        if time.time() >= self.next_hour_ts:
            self.output.close()
            current_dt = datetime.utcfromtimestamp(self.next_hour_ts).strftime('%Y-%m-%d-%H')
            self.output = bz2.open(os.path.join(self.output_dir, '{0}.bz2'.format(current_dt)), 'at')
            self.next_hour_ts += 3600
        self.output.write(data)


def start_streaming(crawler_queue):
    while not crawler_queue.empty():
        crawler_conf = crawler_queue.get()

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


if __name__ == '__main__':
    with open('../conf/developer.key', 'r') as fin:
        key_dict = json.load(fin)

    with open('../conf/covid_crawler.conf', 'r') as config_file:
        configs = json.load(config_file)
    num_crawler = len(configs) - 1

    app_name = configs['app_name']
    print('>>> app name: {0}'.format(app_name))
    os.makedirs('../log', exist_ok=True)
    logging.basicConfig(filename='../log/{0}_crawl.log'.format(app_name), filemode='w', format='%(asctime)s - %(message)s', level=logging.WARNING)

    output_dir = '../data/{0}'.format(app_name)
    os.makedirs(output_dir, exist_ok=True)

    processes = []
    crawler_queue = Queue()

    for i in range(num_crawler):
        configs['crawler{0}'.format(i)]['output_dir'] = os.path.join(output_dir, configs['crawler{0}'.format(i)]['crawler_name'])
        configs['crawler{0}'.format(i)]['key_token'] = key_dict[configs['crawler{0}'.format(i)]['key_set']]
        crawler_queue.put(configs['crawler{0}'.format(i)])

    for w in range(num_crawler):
        p = Process(target=start_streaming, args=(crawler_queue,))
        p.daemon = True
        p.start()
        processes.append(p)

    for p in processes:
        p.join()
