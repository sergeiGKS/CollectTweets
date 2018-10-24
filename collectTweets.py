
# coding: utf-8

# # This sript: collect all old tweets


#libraries
from tweepy import OAuthHandler 
import os
import pandas as pd
import geograpy
import got



class TwitterClient(object):
	''' 
    Generic Twitter Class. 
	'''
	
	def __init__(self): 
		''' 
        Class constructor or initialization method.
		
		'''
		
        #keys and tokens from the Twitter Dev Console 
		consumer_key = ""
		consumer_secret =""
		access_token_key =""
		access_token_secret=""
		
		# attempt authentication 
		try: 
			# create OAuthHandler object 
			self.auth = OAuthHandler(consumer_key, consumer_secret) 
			# set access token and secret 
			self.auth.set_access_token(access_token_key, access_token_secret) 
            # create tweepy API object to fetch tweets 
			self.api = tweepy.API(self.auth,wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
		
		except Exception, e: 
			print("Error: Authentication Failed") 
			print e
			

	#reading local file to get key and token 
	def read_base_file(self, data_folder, base_file):
		""" 
		Utility function to read files
		"""
	
		try:
			return pd.read_csv(data_folder+base_file, na_values=["", " ", "-"])
		except NameError:
			print "File Not Found.", 
		except Exception, e:
			print "Error in reading", base_file
			print e
		return pd.DataFrame()

	





	def collectTweets(self, querry,since=None,until=None):
		"""
		Method to collect tweets from querry
		"""
		
		
		#Initialisation of DataFrame
		tweet_df = pd.DataFrame(columns=["id_tweet", "username","user_location","tweet_location","retweets","date","text"
									 ,"mentions","favorites","hashtags","permalinks",
									"user_geo_enabled","user_country","language"])
		
		
		
		#Querry to search tweets in different language
		tweetCriteria = got.manager.TweetCriteria().setQuerySearch(querry).setSince(since).setUntil(until)
		tweets = got.manager.TweetManager.getTweets(tweetCriteria)

		for tweet in tweets:
			id_tweet = tweet.id
			retweets = tweet.retweets
			username = tweet.username
			date = tweet.date
			text = tweet.text
			location = tweet.geo
			mentions = tweet.mentions
			favorites = tweet.favorites
			hashtags = tweet.hashtags
			permalinks = tweet.permalink
			if id_tweet!= 0:
				try:
					tweet_status = self.api.get_status(id_tweet)
					user_location = tweet_status.user.location
					user_geo_enabled = tweet_status.user.geo_enabled
					language = tweet_status.lang
				except tweepy.error.TweepError:
					user_location=""
					user_geo_enabled=False
					language = ""

				# add tweet to dataframe
				tweet_df = tweet_df.append({
					"id_tweet":id_tweet,
					"username": username,
					"user_location": user_location,
					"user_country": self.getCountryFromText(user_location),
					"tweet_location": location,
					"user_geo_enabled": user_geo_enabled,
					"retweets": retweets,
					"date": date,
					"text": text,
					"mentions": mentions,
					"favorites": favorites,
					"hashtags":hashtags,
					"permalinks":permalinks,
					"language":language
				  },ignore_index=True)
					
					

		
		
		#delete duplicate tweets	
		tweet_df = tweet_df.drop_duplicates(subset="id_tweet", keep='first', inplace=False)
		
		return tweet_df
		



	def getCountryFromText(self,text):
		"""
		parse text location to get country name
		"""
		if text!="":
			place = geograpy.get_place_context(text=text)
			country = place.countries
			if country!=[]:
				return country[0]
			else:
				return text




				
def main():

	api = TwitterClient() 

	# collect AUF tweets
	tweets_df_a = api.collectTweets("Kagame","2018-09-27","2018-09-30")


	
  
if __name__ == "__main__": 
    # calling main function 
    main() 

