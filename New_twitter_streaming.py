# Bernard Tieku
# If you're collecting tweets with very popular keywords (e.g., bigdata, iop, trump, applebee), use the following script.

import tweepy

# Saving data as
save_file = open('test.json', 'a')

#This is a basic listener that just prints received tweets to stdout.
class StdOutListener(tweepy.Stream):

    def on_data(self, data):
        if isinstance(data, dict):
            # An attempt to cut down on non English tweets.
            if data['user']['lang'] != 'en':
                return


        print(data)
        save_file.write(str(data))
        return True

    def on_error(self, status):
        print(status)

if __name__ == '__main__':

    #This handles Twitter authetification and the connection to Twitter Streaming API
    stream = StdOutListener('consumer_key', 'consumer_secret', 'access_token', 'access_token_secret')

    #This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby', '#bigdata'
    stream.filter(track=['GUDA', 'Akuffo Addo', 'kirani ayat'])
