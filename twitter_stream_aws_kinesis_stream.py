# If you're collecting tweets with very popular keywords (e.g., bigdata, iop, trump, applebee), use the following script.

#from tweepy.streaming import StreamListener
import boto3
#import random
import time
import json
import tweepy

#This is a basic listener that just prints received tweets to stdout.
class StdOutListener(tweepy.Stream):

    def on_data(self, data):
        if isinstance(data, dict):
            # An attempt to cut down on non English tweets.
            if data['user']['lang'] != 'en':
                return

        print(data)
        client.put_record(StreamName=streamname, Data=data, PartitionKey='id')

        return True

    def on_error(self, status):
        print(status)

if __name__ == '__main__':
    
    #This handles Twitter authentification and the connection to Twitter Streaming API
    listener = StdOutListener('consumer_key', 'consumer_secret', 'access_token', 'access_token_secret')

    #tweets = Table('tweets_ft',connection=conn)
    client = boto3.client('kinesis', 
                          region_name='',
                          aws_access_key_id='',
                          aws_secret_access_key='' 
                          )

    streamname = ''
    #This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby'
    #stream.filter(track=['trump'], stall_warnings=True)
    while True:
        try:
            print('Twitter streaming...')
            listener.filter(track=['Blacko', 'Black Sherif', 'blacko', 'black sherif', '#blacko'], languages=['fr','en'], stall_warnings=True)
        except Exception as e:
            print(e)
            print('Disconnected...')
            time.sleep(5)
            continue


