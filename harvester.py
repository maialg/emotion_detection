# Import necessary modules
from time import sleep
import tweepy
from kafka import KafkaProducer
from twitterpipe_globals import *
import json

class TweetHarvester():
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER)
        bearer_token = open('./keys/bearer_token').read().strip()
        self.streaming_client = tweepy.StreamingClient(bearer_token)
        self.streaming_client.on_data = self.on_data

    # The main method that starts the tweet harvesting process. It adds the rules for filtering tweets and handles any
    # KeyboardInterrupt exception that might occur.
    def start(self):
        try:
            # Add the rules for filtering tweets.
            # Not including retweets, because one viral tweet can skew the data, but it also decreases number of tweets
            # All tweets should be in english
            self.streaming_client.add_rules(tweepy.StreamRule('#istandwithukraine lang:en -is:retweet ', tag='standwith'))
            self.streaming_client.add_rules(tweepy.StreamRule('#slavaukraini lang:en -is:retweet ', tag='slava'))
            self.streaming_client.add_rules(tweepy.StreamRule('#russiaisaterroriststate lang:en -is:retweet ', tag='terrotist_state'))
            self.streaming_client.add_rules(tweepy.StreamRule('#standwithputin lang:en -is:retweet ', tag='standputin'))
            self.streaming_client.add_rules(tweepy.StreamRule('#standwithrussia lang:en -is:retweet ', tag='standrussia'))
            self.streaming_client.add_rules(tweepy.StreamRule('#zelenskywarcriminal lang:en -is:retweet ', tag='warCriminal'))
            self.streaming_client.filter()
            
            
       
        except KeyboardInterrupt:
            print('Stopping...')
            # Print the rules that the Twitter API is filtering on.
            print('RULES!!!!!: ', self.streaming_client.get_rules())
            self.stop()

    # A method that stops the tweet harvesting process.
    def stop(self):
        # A method that stops the tweet harvesting process.
        self.on_finish()

    def on_data(self, json_data):
        # A method that is called whenever new data (tweets) are received. It filters the tweets based on their tag and sends
        # them to the appropriate Kafka topic.

        # Load the received data as a JSON object.
        matching_rules = json.loads(json_data)

        # Iterate over the matching rules and get the tag of each rule.
        for lines in matching_rules["matching_rules"]:
            tag = lines['tag']
            tag = str(tag)

        # Check the tag of the tweet and send it to the appropriate Kafka topic.
        #Pro-ukraine hashtags
        if tag == "terrotist_state":
            self.producer.send(TWEET_TOPIC, json_data)
            print(f'Received tweet', json_data.decode().strip())
        elif tag == 'slava':
            self.producer.send(TWEET_TOPIC, json_data)
            print(f'Received tweet', json_data.decode().strip())
        elif tag == 'invade':
            self.producer.send(TWEET_TOPIC, json_data)
            print(f'Received tweet', json_data.decode().strip())

        # Check the tag of the tweet and send it to the appropriate Kafka topic.
        #Pro-Russia hashtags
        elif tag == 'fucknato':
            self.producer.send(TWEET_TOPIC2, json_data)
            print(f'Received tweet2', json_data.decode().strip())
        elif tag == 'standputin':
            self.producer.send(TWEET_TOPIC2, json_data)
            print(f'Received tweet2', json_data.decode().strip())
        elif tag == 'standrussia':
            self.producer.send(TWEET_TOPIC2, json_data)
            print(f'Received tweet2', json_data.decode().strip())
        elif tag == 'warCriminal':
            self.producer.send(TWEET_TOPIC2, json_data)
            print(f'Received tweet2', json_data.decode().strip())

    def on_finish(self):
        # A method that is called when the tweet harvesting process is finished. It disconnects the StreamingClient and
        # prints a message to indicate that the harvester has stopped.
        self.streaming_client.disconnect()
        print('Stopping harvester...')
        sleep(5)
        print('Twitter harvester stopped!')

# Create an instance of the TweetHarvester class and start the tweet harvesting process.
tweet_harvester = TweetHarvester()
tweet_harvester.start()


