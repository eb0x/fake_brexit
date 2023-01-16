#This app collects tweets into a SQLITE DB. It is single user and so needs twitter Consumer and Access keys. 
# (Never save keys in a public place so these are kept in separate file)
#This tweepy Twitter app is called fake brexit as it is designed to collect tweets around the brexit 
#topic looking for fake news. 
#In fact the twitter retrieval is entirely generic. The only brexit specific item is the word in the search term.
#


from pathlib import Path
import re
import sys
import collections
import pickle
import math
import time
from datetime import datetime
import random
import logging
#import pandas as pd
import time
import tweepy
import json
import base64 

import keys as k                # Don't upload these to a public forum such as github
import fake_db2 as Db_m         # Db_m  = DB Module

import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=FutureWarning)


#Based on the Tweepy stream tutorial
#
class MyStreamListener(tweepy.StreamListener):

##Set the DB file
    def set_DB(self, file):
        self.DB = Db_m.Db(file)
        self
        
#Call back from the stream. status is a Tweet object
    def on_status(self, status):
#        filename = 'fetched_tweets.txt' + datetime.now().strftime("%d-%m-%Y_%H_%M_%S")
        user_dict = self.get_user_data(status.user)
        status_dict = self.get_status_data(status)
        self.DB.add_userStatusID(user_dict, status_dict)
        self.DB.addAllData(user_dict['id'], user_dict)
        self.DB.addAllData(status_dict['id'], status_dict)
        self.DB.committ()

        
#Error callback
    def on_error(self, status):
        print("Error Code : " + status)

#Look at the twitter user object, and extract as many fields as possible into a dictionary
#
    def get_user_data(self, user):
        user_data = {}
        descr = ""
        nam = ""
        try:
            if isinstance(user.description ,str):
                descr = user.description.replace('"', r'\'')
            if isinstance(user.name ,str):
                descr = user.name.replace('"', r'\'') 
            user_data = {"id": user.id,\
             "id_str": user.id_str,\
             "retrieved": datetime.now().strftime("%d-%m-%Y_%H_%M_%S"),\
             "screen_name": user.screen_name,\
             "default_profile": user.default_profile,\
             "profile_background_tile": user.profile_background_tile,\
             "following": user.following,\
             "description": descr,\
             "name": nam,\
             "profile_sidebar_border_color": user.profile_sidebar_border_color,\
             "utc_offset": user.utc_offset,\
             "statuses_count": user.statuses_count,\
             "notifications": user.notifications,\
             "verified": user.verified,\
             "profile_background_image_url_https": user.profile_background_image_url_https,\
             "profile_image_url": user.profile_image_url,\
             "default_profile_image": user.default_profile_image,\
             "profile_image_url_https": user.profile_image_url_https,\
             "geo_enabled": user.geo_enabled,\
             "follow_request_sent": user.follow_request_sent,\
#             "is_translation_enabled": user.is_translation_enabled,\
             "profile_use_background_image": user.profile_use_background_image,\
             "protected": user.protected,\
             "favourites_count": user.favourites_count,\
             "url": user.url,\
             "followers_count": user.followers_count,\
             "profile_background_image_url": user.profile_background_image_url,\
             "profile_link_color": user.profile_link_color,\
             "profile_text_color": user.profile_text_color,\
#             "profile_banner_url": user.profile_banner_url,\
#             "has_extended_profile": user.has_extended_profile,\
#             "is_translator": user.is_translator,\
             "profile_sidebar_fill_color": user.profile_sidebar_fill_color,\
             "created_at": user.created_at.strftime("%d-%m-%Y_%H_%M_%S"),\
             "contributors_enabled": user.contributors_enabled,\
             "friends_count": user.friends_count,\
             "profile_background_color": user.profile_background_color,\
             "location": user.location,\
             "time_zone": user.time_zone,\
             "listed_count": user.listed_count,\
             "lang": user.lang}
        except Exception as e:
            print("get_user_id: {}".format(e))
            pass
        return user_data

#In twitter a tweet is called a status. Here we look at the status object and put all the data
#into a dictionary. Not all the tweets contain all the fields so check if fields are present or use defaults.
#Retweets and mentions embed a status object, so call this reflexively.
#entities are a json object, so this needs serializing and turned into a string (using base64 encoding) for later use
#
    def get_status_data(self, status):
        status_data = {}
        try:
 #           print("RTS {}".format(status.retweeted_status))
 #           print(type(status.retweeted_status))
            if isinstance(status.retweeted_status,(tweepy.models.Status, dict)):
                rval = self.get_status_data(status.retweeted_status)
                rtstatus = rval['id']
        except Exception as e:
#            print("get_status_data: {}".format(e))
            rtstatus = 0
        try:
            if isinstance(status.quoted_status,(tweepy.models.Status, dict)):
                qval = self.get_status_data(status.quoted_status)
                qtstatus = qval['id']
        except Exception as e:
#            print("get_status_data: {}".format(e))
            qtstatus = 0
        try:
            if status.entities:
                edstatus = base64.b64encode(bytes(json.dumps(status.entities), "utf-8"))
        except:
            edstatus = ""
        try:
            if status.timestamp_ms:
                ts =  status.timestamp_ms
        except:
            ts = 0
        try:
            status_data = {
                "id": status.id,\
                "text": status.text,\
                "created_at": status.created_at.strftime("%d-%m-%Y_%H_%M_%S"),\
                "in_reply_to_status_id": status.in_reply_to_status_id,\
                "id_str": status.id_str,\
                "entities": edstatus,\
                "quote_count": status.quote_count,\
                "quoted_status": qtstatus,\
#               "possibly_sensitive": status.possibly_sensitive,\
                "in_reply_to_user_id_str": status.in_reply_to_user_id_str,\
                "coordinates": status.coordinates,\
                "timestamp_ms": ts,\
                "retweet_count": status.retweet_count,\
                "retweeted": status.retweeted,\
                "retweeted_status": rtstatus,\
                "contributors": status.contributors,\
                "favorite_count": status.favorite_count,\
                "favorited": status.favorited,\
                "in_reply_to_status_id_str": status.in_reply_to_status_id_str,\
                "source": status.source,\
                "in_reply_to_user_id": status.in_reply_to_user_id,\
                "user": status.user.id,\
#                "place": status.place,\
                "geo": status.geo,\
                "truncated": status.truncated,\
                "in_reply_to_screen_name": status.in_reply_to_screen_name,\
                "is_quote_status": status.is_quote_status,\
                "lang": status.lang}
        except Exception as e:
            print("get_status_data: {}".format(e))
            pass
        return status_data

#Not used
#
def test_rate_limit(api, wait=True, buffer=.1):
    """
    Tests whether the rate limit of the last request has been reached.
    :param api: The `tweepy` api instance.
    :param wait: A flag indicating whether to wait for the rate limit reset
                if the rate limit has been reached.
    :param buffer: A buffer time in seconds that is added on to the waiting
                time as an extra safety margin.
    :return: True if it is ok to proceed with the next request. False otherwise.
    """
    #Get the number of remaining requests
    remaining = int(api.last_response.getheader('x-rate-limit-remaining'))
    #Check if we have reached the limit
    if remaining == 0:
        limit = int(api.last_response.getheader('x-rate-limit-limit'))
        reset = int(api.last_response.getheader('x-rate-limit-reset'))
    #Parse the UTC time
        reset = datetime.fromtimestamp(reset)
        print("0 of {} requests remaining until {}".format(limit, reset))
        if wait:
            delay = (reset - datetime.now()).total_seconds() + buffer
            print( "Sleeping for {}s...".format(delay))
            time.sleep(delay)
            return True
        return False 
    #We have not reached the rate limit
    return True

#This code runs the application if started
#
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger()
#    DB.initialize() # reset DB
    myauth=tweepy.OAuthHandler(k.CONSUMER_KEY, k.CONSUMER_SECRET)
    myauth.set_access_token(k.ACCESS_TOKEN, k.ACCESS_TOKEN_SECRET)
    api = tweepy.API(myauth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    try:
        api.verify_credentials()
    except Exception as e:
        logger.error("error creating API", exc_info=True)
        raise e
    iter_count = 1
    start = datetime.now()
    while(True):
        try:
            logger.info("API created {}. Iteration:{}, ".format( start.strftime("%d-%m-%Y_%H_%M_%S"), iter_count))
            tweets_listener = MyStreamListener(api)
            tweets_listener.set_DB("fake_db.sqlite")
            myStream = tweepy.Stream(auth = api.auth, listener=tweets_listener , tweet_mode='extended')
            myStream.filter(track=['brexit'])
        except KeyboardInterrupt:                                       #Use ^c to stop at console
            print("Stopped")
            break
        except Exception as e:
            end = datetime.now()
            logger.info("fake_brexit Stream Error: {} at: {}".format(e, end.strftime("%d-%m-%Y_%H_%M_%S")))
            logger.info("Ran for {}".format(end - start))
            if iter_count > 10:
                logger.info("Stopping at: {}, iteration {}".format( end.strftime("%d-%m-%Y_%H_%M_%S"), iter_count))
                break
            else:
                iter_count += 1
                time.sleep(random.randint(300*iter_count,900*iter_count))           #in case hit rate limit, stop at least five minute then restart
                                                                                    #progressively step back to avoid being sanctioned. Wait on rate limits should stop this
                                                                                    # but it is not reliable
            
        
