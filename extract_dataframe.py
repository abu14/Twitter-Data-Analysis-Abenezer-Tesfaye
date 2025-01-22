import json
import pandas as pd
from textblob import TextBlob


def read_json(json_file: str)->list:
    """
    json file reader to open and read json files into a list
    Args:
    -----
    json_file: str - path of a json file
    
    Returns
    -------
    length of the json file and a list of json
    """
    
    tweets_data = []
    for tweets in open(json_file,'r'):
        tweets_data.append(json.loads(tweets))
    
    return len(tweets_data), tweets_data

class TweetDfExtractor:
    """
    this function will parse tweets json into a pandas dataframe
    Return
    ------
    dataframe
    """
    def __init__(self, tweets_list):
        
        self.tweets_list = tweets_list
   
    def find_statuses_count(self)->list:
        """
        Get the status count for the tweets
        """
        statuses_count = []
        for items in self.tweets_list:
            statuses_count.append(items.get('statuses_count',None))
        return statuses_count  



    def find_full_text(self)->list:
        """
        This funciton gets the entire tweet/text from the tweet strings
        """
        text = []
        for items in self.tweets_list:
            text.append(items.get('full_text',None))
        return text 
       
    
    def find_sentiments(self, text)->list:
        """
        extract the sentiments of the tweets using TextBlob
        """
        polarity = []
        self.subjectivity = []
        
        for items in text:
            self.subjectivity.append(TextBlob(items).sentiment.subjectivity)
            polarity.append(TextBlob(items).sentiment.subjectivity)
        # polarity = TextBlob(text).sentiment.polarity
        # subjectivity = TextBlob(text).sentiment.subjectivity
        return polarity, self.subjectivity



    def find_created_time(self)->list:
        """
        The created time gets the tweeted createtion specific time
        """
        created_at = []
        for items in self.tweets_list:
            created_at.append(items.get('created_at',None))
       
        return created_at
    


    def find_source(self)->list:
        """
        Source of the tweet or Retweet. 
            eg. mobile, web
        """
        source = []
        for items in self.tweets_list:
            source.append(items.get('source',None))
        return source



    def find_screen_name(self)->list:
        """
        Get the screen name from the column user
        Return: screen name item from the dictionary
        """
        screen_name = []
        for items in self.tweets_list:
            screen_name.append(items['user']['screen_name'])
        return screen_name



    def find_followers_count(self)->list:
        """
        extracts the number of followers a user / tweet account has
        Returns: a count of followers
        """
        followers_count = []
        for items in self.tweets_list:
            followers_count.append(items['user']['followers_count'])

        return followers_count
    

    def find_friends_count(self)->list:
        friends_count = []
        for items in self.tweets_list:
            friends_count.append(items['user']['friends_count'])
        return friends_count


    def is_sensitive(self)->list:
        try:
            is_sensitive = [x['possibly_sensitive'] for x in self.tweets_list]
        except KeyError:
            is_sensitive = None
        return is_sensitive
    

    def find_favourite_count(self)->list:
        favourite_count = []
        for items in self.tweets_list:
            favourite_count.append(items['user']['favourites_count'])
        return favourite_count
        
    
    def find_retweet_count(self)->list:
        retweet_count = []
        for items in self.tweets_list:
            retweet_count.append(items.get('retweet_count',None))


    def find_hashtags(self)->list:
        hashtags = []
        for items in self.tweets_list:
            hashtags.append(items['user']['hashtags'])
        return hashtags
    

    def find_mentions(self)->list:
        mentions = []
        for items in self.tweets_list:
            mentions.append(items['user']['mentions'])
        return mentions


    def find_location(self)->list:
        try:
            location = self.tweets_list['user']['location']
        except TypeError:
            location = ''
        
        return location

    
        
        
    def get_tweet_df(self, save=False)->pd.DataFrame:
        """required column to be generated you should be creative and add more features"""
        
        columns = ['created_at', 'source', 'original_text','polarity','subjectivity', 'lang', 'favorite_count', 'retweet_count', 
            'original_author', 'followers_count','friends_count','possibly_sensitive', 'hashtags', 'user_mentions', 'place']
        
        created_at = self.find_created_time()
        source = self.find_source()
        text = self.find_full_text()
        polarity, subjectivity = self.find_sentiments(text)
        lang = self.find_lang()
        fav_count = self.find_favourite_count()
        retweet_count = self.find_retweet_count()
        screen_name = self.find_screen_name()
        follower_count = self.find_followers_count()
        friends_count = self.find_friends_count()
        sensitivity = self.is_sensitive()
        hashtags = self.find_hashtags()
        mentions = self.find_mentions()
        location = self.find_location()
        data = zip(created_at, source, text, polarity, subjectivity, lang, fav_count, retweet_count, screen_name, follower_count, friends_count, sensitivity, hashtags, mentions, location)
        df = pd.DataFrame(data=data, columns=columns)

        if save:
            df.to_csv('processed_tweet_data.csv', index=False)
            print('File Successfully Saved.!!!')
        
        return df

                
if __name__ == "__main__":
    # required column to be generated you should be creative and add more features
    columns = ['created_at', 'source', 'original_text','clean_text', 'sentiment','polarity','subjectivity', 'lang', 'favorite_count', 'retweet_count', 
    'original_author', 'screen_count', 'followers_count','friends_count','possibly_sensitive', 'hashtags', 'user_mentions', 'place', 'place_coord_boundaries']
    _, tweet_list = read_json("../covid19.json")
    tweet = TweetDfExtractor(tweet_list)
    tweet_df = tweet.get_tweet_df() 

    # use all defined functions to generate a dataframe with the specified columns above