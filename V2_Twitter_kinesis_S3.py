# -*- coding: utf-8 -*-
"""
Created on Tue Mar 14 21:35:41 2023

@author: bernard.tieku
"""
#from secrets import access_key, secret_access_key
import sys
import boto3
import os, glob
from  pathlib import Path
import json

import tweepy
from tweepy import StreamingClient, StreamRule
import time

print ("Start : %s" % time.ctime())
timeout = 5
timeout_start = time.time()

class MyStream(tweepy.StreamingClient):
    # This function gets called when the stream is working
    def on_connect(self):
        print("Connected")
        
        

    def on_tweet(self, tweet):
        if time.time() < timeout_start + timeout:
            print(tweet.data)
            tweet = json.dumps(tweet.data)
            
            #sending data to s3
            client.put_record(StreamName='test_stream', Data=tweet, PartitionKey='id')
        else:
            sys.exit("Script Done")

        return True
   
   
if __name__ == '__main__':

    stream = MyStream("bearer_token")
    
    #tweets = Table('tweets_ft',connection=conn)
    client = boto3.client('kinesis', region_name='us-east-2', aws_access_key_id='access_key',aws_secret_access_key='secret_key' )
    
    while True:
            try:
                print('Twitter streaming...')
                #stream.add_rules(tweepy.StreamRule("#Bitcoin")) #adding the rules
                stream.add_rules(tweepy.StreamRule(["#Bitcoin", "bitcoin"])) #adding the rules
                stream.filter() #runs the stream
    
            except Exception as e:
                print(e)
                print('Disconnected...')
                time.sleep(5)
                continue
