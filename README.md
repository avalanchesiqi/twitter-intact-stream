# Twitter-intact-stream

Twitter-intact-stream is a tool for collecting (nearly) complete Twitter filtered stream, and first used to construct datasets in the following paper,
> [Siqi Wu](https://avalanchesiqi.github.io/), [Marian-Andrei Rizoiu](http://www.rizoiu.eu/), and [Lexing Xie](http://users.cecs.anu.edu.au/~xlx/). Variation across Scales: Measurement Fidelity under Twitter Data Sampling. *AAAI International Conference on Weblogs and Social Media (ICWSM)*, 2020. \[ [paper](https://avalanchesiqi.github.io/files/icwsm2020sampling.pdf) | [code](https://github.com/avalanchesiqi/twitter-sampling) \]

### Python packages version
All codes are developed and tested in Python 3.7, along with tweepy 3.7.0.

## Code usage
Before using this Twitter-intact-stream crawler, you need to [register your Twitter developer account and apps](https://developer.twitter.com/en/apps).
Rename `conf/developer.key-backup` to `conf/developer.key`, and set the registered tokens in `conf/developer.key`.

Twitter-intact-stream splits a set of filtering keywords and languages into multiple subcrawlers.
Each subcrawler can cover more tweets for the target stream, altogether they rise the overall sampling rate.

Next, you need config the monitored predicates by creating a `conf/*_crawler.conf` file.
We provide an example of COVID-19 configuration (see `conf/covid_crawler.conf`, keywords extended from [this paper](https://arxiv.org/pdf/2003.07372.pdf)):

```json
{"app_name": "covid",
 "crawler0": {"crawler_name": "covid_all",
              "key_set": "key0",
              "keywords": ["coronavirus", "covid19", "covid", "covid–19", "COVIDー19", "pandemic", "covd", "ncov", "corona", "corona virus", "sars-cov-2", "sarscov2", "koronavirus", "wuhancoronavirus", "wuhanvirus", "wuhan virus", "chinese virus", "chinesevirus", "china", "wuhanlockdown", "wuhan", "kungflu", "sinophobia", "n95", "world health organization", "cdc", "outbreak", "epidemic", "lockdown", "panic buying", "panicbuying", "socialdistance", "social distance", "socialdistancing", "social distancing"],
              "languages": []},
 "crawler1": {"crawler_name": "covid_1",
              "key_set": "key1",
              "keywords": ["wuhanlockdown", "wuhan", "kungflu", "sinophobia", "n95", "world health organization", "cdc", "outbreak", "epidemic"],
              "languages": []},
 "crawler2": {"crawler_name": "covid_2",
              "key_set": "key2",
              "keywords": ["lockdown", "panic buying", "panicbuying", "socialdistance", "social distance", "socialdistancing", "social distancing"],
              "languages": []},
 "crawler3": {"crawler_name": "covid_3",
              "key_set": "key3",
              "keywords": ["pandemic", "covd", "ncov"],
              "languages": ["en", "es"]},
 "crawler4": {"crawler_name": "covid_4",
              "key_set": "key4",
              "keywords": ["coronavirus"],
              "languages": ["en"]},
 "crawler5": {"crawler_name": "covid_5",
              "key_set": "key5",
              "keywords": ["covid"],
              "languages": ["en"]},
 "crawler6": {"crawler_name": "covid_6",
              "key_set": "key6",
              "keywords": ["covid–19", "COVIDー19", "covid19"],
              "languages": ["en"]}
}
```

The value of `key_set` should exist as the indexing key of `conf/developer.key`.
```text
`crawler0` tracks all 35 keywords in all languages.
`crawler1` tracks all 9 keywords in all languages.
`crawler2` tracks all 7 keywords in all languages.
`crawler3` tracks all 3 keywords in English or Spanish.
`crawler4` tracks all 1 keywords in English.
`crawler5` tracks all 1 keywords in English.
`crawler6` tracks all 3 keywords in English.
```

Next, you need replace the values with your desired conf file and output directory in the main script (`crawlers/multi_process_crawlers.py`):

```python
line 57: with open('../conf/covid_crawler.conf', 'r') as config_file:
line 66: output_dir = '../data/{0}'.format(app_name)
```

## Analysis
We provide analysis codes to compute the number of missing tweets and sampling rates.
The scripts should be executed in order.

1. `extract_tweet_status.py`: This script extracts the collected tweets of json format to text format. See comments for details.
2. `merge_subcrawlers.py`: This script removes duplicate tweet ids and sort them chronologically.
3. `count_ratemsg_in_subcrawlers.py`: This script counts the sampling rates in each subcrawler.
4. `plot_tweet_volume.py`: This script plots the temporal tweet counts and sampling rates at the granularity of hour.

Output:
```shell script
>>> subcrawler covid_1  ,  11725467 retrieved tweets,     216 rate limit track, indicating      6912 missing tweets, yielding  99.94% sampling rate
>>> subcrawler covid_2  ,  24415704 retrieved tweets,   75692 rate limit track, indicating   1053052 missing tweets, yielding  95.87% sampling rate
>>> subcrawler covid_3  ,  16753021 retrieved tweets,    4397 rate limit track, indicating     33454 missing tweets, yielding  99.80% sampling rate
>>> subcrawler covid_4  ,  33952506 retrieved tweets,  604400 rate limit track, indicating  24173145 missing tweets, yielding  58.41% sampling rate
>>> subcrawler covid_5  ,  27998308 retrieved tweets,  182773 rate limit track, indicating   1880251 missing tweets, yielding  93.71% sampling rate
>>> subcrawler covid_6  ,  20592431 retrieved tweets,   39503 rate limit track, indicating    336820 missing tweets, yielding  98.39% sampling rate
>>> subcrawler covid_7  ,  21653393 retrieved tweets,   24647 rate limit track, indicating    137600 missing tweets, yielding  99.37% sampling rate
>>> subcrawler covid_8  ,  18046113 retrieved tweets,    8426 rate limit track, indicating     67938 missing tweets, yielding  99.62% sampling rate
>>> subcrawler covid_9  ,  15407936 retrieved tweets,    2125 rate limit track, indicating     40456 missing tweets, yielding  99.74% sampling rate
>>> subcrawler covid_10 ,  17576365 retrieved tweets,   20193 rate limit track, indicating    187806 missing tweets, yielding  98.94% sampling rate
>>> subcrawler covid_11 ,  17894990 retrieved tweets,   11863 rate limit track, indicating    103313 missing tweets, yielding  99.43% sampling rate
>>> subcrawler covid_12 ,  18766409 retrieved tweets,    8838 rate limit track, indicating     55686 missing tweets, yielding  99.70% sampling rate
>>> subcrawler covid_all,  34545906 retrieved tweets,  691059 rate limit track, indicating 193510821 missing tweets, yielding  15.15% sampling rate
>>> complete_set covid  , 209463069 retrieved tweets,     NaN rate limit track, estimating 228056727   total tweets, yielding  91.85% sampling rate
>>> Elapsed time: 0:34:35.998
```

From which we can see, the sampling rate increases from 15.15% with one single crawl to 91.85% with 12 subcrawlers (209M tweets in 8 days).
This is significantly more than current Twitter threshold (4.32M per day).
The following image plots temporal tweet counts and sampling rates for a dataset collected from 2020-03-23 to 2020-03-31.
![Temporally tweet counts and sampling rates](analysis/tweet_volume.png)
