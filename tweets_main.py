import pandas as pd
import sqlalchemy
import tweepy as tw

import re
import time
from deep_translator import GoogleTranslator
from textblob import TextBlob
from unicodedata import normalize

# Access tokens
consumer_key = **********
consumer_secret = **********
access_token = **********
access_token_secret = **********

# Autentication
auth = tw.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tw.API(auth, wait_on_rate_limit=True)

#Database URI and engine creation
DATABASE_URI = 'postgresql+psycopg2://postgres:postgres@localhost:5432/candidatos'
engine = sqlalchemy.create_engine(DATABASE_URI)

#Users list to search
users_list = ['@jdoriajr', '@wilsonwitzel', '@claudiocastroRJ', '@RomeuZema', '@Casagrande_ES',  # SE
                  '@ratinho_jr', '@CarlosMoises', '@EduardoLeite_',  # S
                  '@ronaldocaiado', '@Reinaldo45psdb', '@MauroMendes40', '@IbaneisOficial', # CO
                  '@RenanFilho_', '@costa_rui', '@CamiloSantanaCE', '@FlavioDino', '@joaoazevedolins',
                  '@PauloCamara40', '@wdiaspi', '@fatimabezerra', '@belivaldochagas',  # NE
                  '@GladsonCameli', '@waldezoficial', '@wilsonlimaAM', '@helderbarbalho', '@celmarcosrocha',
                  '@antoniodenarium', '@maurocarlesse']  # N


def tweets_scrape_to_dataframe(username, count, last_id):
    # TWITTER SCRAPE
    tweets_df = pd.DataFrame(columns=['tweet_text', 'tweet_datetime', 'tweet_id', 'username', 'retweets', 'favorites'])
    print("### Starting scrape for @" + username)
    try:
        # Creation of query method using parameters
        tweets = tw.Cursor(api.user_timeline, id=username, tweet_mode='extended', since_id=last_id).items(count)

        # Pulling information from tweets iterable object
        tweets_list = [
            [tweet.full_text, tweet.created_at, tweet.id_str, tweet.user.screen_name, tweet.retweet_count,
             tweet.favorite_count] for tweet in tweets]

        # Creation of dataframe from tweets_list
        tweets_df = pd.DataFrame(tweets_list, columns=['tweet_text', 'tweet_datetime', 'tweet_id', 'username', 'retweets', 'favorites'])

        print(str(len(tweets_df)) + " tweets identified")

    except BaseException as e:
        print('failed on_status,', str(e))
        time.sleep(3)

    return tweets_df

def dataframe_to_database(df):

    # Saving dataframe on database
    for id in df['tweet_id']:
        query = "SELECT * FROM posts WHERE tweet_id = '" + id + "'"
        df_id = pd.read_sql_query(query, engine)

        if not df_id.empty:
            df = df[df.tweet_id != id]

    df.to_sql('posts', con=engine, if_exists='append', index=False)
    print(str(len(df)) + " tweets added to the database")



# Check if the user has any record on the database
new_users_list = [s.strip('@') for s in users_list]
for user in new_users_list:

    query = "SELECT * FROM posts WHERE username = '" + user + "'"
    df = pd.read_sql_query(query, engine)

    if df.empty:
        print("\nNo records for @" + user + " on database")
        df = tweets_scrape_to_dataframe(user, 20000, 1)
        dataframe_to_database(df)

    else:
        print("\nFound records for @" + user + " on database")

        #Get last tweet ID recorded for the user
        query = "SELECT max(tweet_id) FROM posts WHERE username = '" + user +"'"
        df = pd.read_sql_query(query, engine)
        last_id = (int(df.iloc[0]['max']))

        df = tweets_scrape_to_dataframe(user, 20000, last_id)
        dataframe_to_database(df)

