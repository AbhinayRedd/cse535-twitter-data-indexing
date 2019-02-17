from TwitterSearch import *
import json
import time

# Configuration Starts here
type_name = "indian_news"
keywords = ["india"]
max_rt = 75
lang = {
    "hi": 1000,
    "fr": 1000,
    "th": 1000,
    "sp": 1000,
    "en": 1000
}

current_rt = 0
sleep_time = 6

def go_to_sleep(current_ts_instance):
  time.sleep(sleep_time)


for ln in lang:
    print(" working for language {0}".format(ln))
    result = []
    max_count = int(lang[ln])
    current_count = 0
    ts = TwitterSearch(consumer_key='key', consumer_secret='key', access_token='key', access_token_secret='key')
    tso = TwitterSearchOrder()
    tso.set_keywords(keywords, or_operator = True)
    tso.set_language(ln)
    tso.set_include_entities(True)
    try:
        for tweet in ts.search_tweets_iterable(tso, callback = go_to_sleep):
            if "RT @" in tweet['text'] and current_rt <= max_rt:
              current_count = current_count + 1
              current_rt = current_rt + 1
              result.append(tweet)
            elif "RT @" not in tweet['text']:
              current_count = current_count + 1
              result.append(tweet)
            if current_count >= max_count:
              break
    except TwitterSearchException as e:
        print(e)
    file = open("data/{0}_{1}.json".format(type_name, ln), "w")
    file.write(json.dumps(result))
