# http://adilmoujahid.com/posts/2014/07/twitter-analytics/
# If you're collecting tweets with very popular keywords (e.g., bigdata, iop, trump, applebee), use the following script.

#from tweepy.streaming import StreamListener
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
    stream = StdOutListener('CWEwmsn9rZ0hAcw4yflugnFl8', '5HkhR51YmNcJnO7PHmol7iEOBNlMmMmNv5QIhLhm8g8KiclEqP', '878936672-KjFi2bgEyFDu6Wo9QT7fzcRQ2jfqxe4E1ZCVsdfY', 'CyTkbXDmX12pTTgoR8xaLQ7OdjelKxTbO182Nc1WtpGda')

while True:
	try:
		#stream = Stream(auth, l)
		#This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby', '#bigdata'
		stream.filter(track=['GUDA', 'Akuffo Addo', 'kirani ayat'])
	except: 
		continue