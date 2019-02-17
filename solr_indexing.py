import json
import os
import pysolr
solr = pysolr.Solr('http://localhost:8983/solr/gettingstarted/', timeout=10)

cities = {"th": "bangkok", "hi": "delhi", "fr": "France", "en": "nyc", "es": "mexico city"}
our_topics = ["infrastructure", "Social Unrest", "Politics", "Crime", "Environment"]
topics_mapping = {
  "Environment":"environment",
  "Crime": "crime",
  "Politics": "politics",
  "infrastructure": "infra",
  "Social Unrest": "social unrest"
  }

# https://gist.github.com/naotokui/ecce71bcc889e1dc42d20fade74b61e2
def get_emoji(text, re):
  emoji_pattern = re.compile(
    u"(\ud83d[\ude00-\ude4f])|"  # emoticons
    u"(\ud83c[\udf00-\uffff])|"  # symbols & pictographs (1 of 2)
    u"(\ud83d[\u0000-\uddff])|"  # symbols & pictographs (2 of 2)
    u"(\ud83d[\ude80-\udeff])|"  # transport & map symbols
    u"(\ud83c[\udde0-\uddff])"  # flags (iOS)
    "+", flags=re.UNICODE)
  return {
    u'tweet_emoticons': map(lambda y: filter(lambda x: len(x)> 0, y)[0], emoji_pattern.findall(text)),
    u'text': re.sub(emoji_pattern, "", text)
  }

def process_tweet(tweet, topic):
  data = {}
  # data should be in {topic: , city: , tweet_text: , tweet_lang: , text_xx: , hashtags: , tweet_date: , tweet_loc:} format
  if "lang" in tweet and "text" in tweet and "created_at" in tweet:
    import re
    from datetime import datetime
    data["city"] = cities[tweet["lang"]]
    data["topic"] = topic
    data["tweet_lang"] = tweet["lang"]
    data["tweet_{0}".format(tweet["lang"])] = tweet["text"]
    data["tweet_date"] = tweet["created_at"]
    data["tweet_date"] = datetime.strptime(re.sub("\+0000", "", tweet["created_at"]), "%a %b %d %H:%M:%S  %Y").strftime("%Y-%m-%dT%H:00:00Z")
    emoticon_data = get_emoji(tweet["text"], re)
    data["tweet_emoticons"] = emoticon_data["tweet_emoticons"]
    data["tweet_text"] = emoticon_data["text"]
    if "coordinates" in tweet:
      data["tweet_loc"] = tweet["coordinates"]

    data_tweet_mapping = {
      "urls": {"main_key": "tweet_urls", "sub_key": "url"},
      "hashtags": {"main_key": "hashtags", "sub_key": "text"},
      "user_mentions": {"main_key": "mentions", "sub_key": "screen_name"}
    }

    if "entities" in tweet:
      for key in ["hashtags", "user_mentions", "urls"]:
        if key in tweet["entities"]:
          data[data_tweet_mapping[key]["main_key"]] = []
          for sub_data in tweet["entities"][key]:
            data[data_tweet_mapping[key]["main_key"]].append(sub_data[data_tweet_mapping[key]["sub_key"]])
  return data

file_lists = os.listdir("./data")

for file_name in file_lists:
  file = open("./data/{0}".format(file_name))
  tweets = json.loads(file.read())
  current_topic = list(set(file_name.split("_")) & set(our_topics))[0]
  for tweet in tweets:
    final_tweet = process_tweet(tweet, topics_mapping[current_topic])
    if len(final_tweet.keys()) > 0:
      print(final_tweet)
    break
    solr.add([final_tweet])
  file.close()
