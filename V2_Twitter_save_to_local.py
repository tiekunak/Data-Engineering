# -*- coding: utf-8 -*-
"""
Created on Sun Mar 19 12:42:04 2023

@author: Nana Tieku
"""



#import boto3
import sys
import json
import tweepy
from tweepy import StreamingClient, StreamRule
import time

API_TOKEN = "bearer_token"
print ("Start : %s" % time.ctime())
save_file = open('last.json', 'a')
timeout = 5
timeout_start = time.time()

class MyStream(StreamingClient):
    
 
    
    # This function gets called when the stream is working
    def on_connect(self):
        print("Connected")
        
    def on_tweet(self, tweet):
        if time.time() < timeout_start + timeout:
            print(tweet.data)
            tweet = json.dumps(tweet.data)
            
        else:
            sys.exit("Script Done")
            
         
        
                
        save_file.write(str(tweet.data))
        return True
    
    
if __name__ == '__main__':
    stream = MyStream(API_TOKEN)
    
    while True:
        
        try:
            print('Twitter streaming...')
            #stream.add_rules(tweepy.StreamRule("#Bitcoin")) #adding the rules
            stream.add_rules(StreamRule(["#Bitcoin", "bitcoin"]))
            stream.filter()
        except Exception as e:
            print(e)
            print('Disconnected...')
            time.sleep(5)
            continue
    

