import pandas as pd
import sqlalchemy
import tweepy as tw
import re
import time
from deep_translator import GoogleTranslator
from textblob import TextBlob
from unicodedata import normalize

def translate_tweet(tweet_pt):
    translating = 1
    text_en = 'error'
    while translating:
        try:
            text_en = GoogleTranslator(source='auto', target='en').translate(tweet_pt)
            translating = 0
        except BaseException as e:
            print(tweet_pt)
            print('Failed on_status,', str(e))
            translating = 0
    return text_en

def calculate_polarity(text_en):
    sentiment = TextBlob(text_en)
    return sentiment.polarity



def remove_stop_words(tweet_raw_text):
    stop_words = ['de', 'a', 'o', 'que', 'e', 'do', 'da', 'em', 'um', 'para', 'é', 'com', 'não', 'nao', 'uma', 'os', 'no', 'se', 'na', 'por', 'mais', 'as', 'dos', 'como', 'mas', 'foi', 'ao', 'ele', 'das', 'tem', 'à', 'seu', 'sua', 'ou', 'ser', 'quando', 'muito', 'há', 'nos', 'já', 'está', 'eu', 'também', 'só', 'pelo', 'pela', 'até', 'isso', 'ela', 'entre', 'era', 'depois', 'sem', 'mesmo', 'aos', 'ter', 'seus', 'quem', 'nas', 'me', 'esse', 'eles', 'estão', 'você', 'tinha', 'foram', 'essa', 'num', 'nem', 'suas', 'meu', 'às', 'minha', 'têm', 'numa', 'pelos', 'elas', 'havia', 'seja', 'qual', 'será', 'nós', 'tenho', 'lhe', 'deles', 'essas', 'esses', 'pelas', 'este', 'fosse', 'dele', 'tu', 'te', 'vocês', 'vos', 'lhes', 'meus', 'minhas', 'teu', 'tua', 'teus', 'tuas', 'nosso', 'nossa', 'nossos', 'nossas', 'dela', 'delas', 'esta', 'estes', 'estas', 'aquele', 'aquela', 'aqueles', 'aquelas', 'isto', 'aquilo', 'estou', 'está', 'estamos', 'estão', 'estive', 'esteve', 'estivemos', 'estiveram', 'estava', 'estávamos', 'estavam', 'estivera', 'estivéramos', 'esteja', 'estejamos', 'estejam', 'estivesse', 'estivéssemos', 'estivessem', 'estiver', 'estivermos', 'estiverem', 'hei', 'há', 'havemos', 'hão', 'houve', 'houvemos', 'houveram', 'houvera', 'houvéramos', 'haja', 'hajamos', 'hajam', 'houvesse', 'houvéssemos', 'houvessem', 'houver', 'houvermos', 'houverem', 'houverei', 'houverá', 'houveremos', 'houverão', 'houveria', 'houveríamos', 'houveriam', 'sou', 'somos', 'são', 'era', 'éramos', 'eram', 'fui', 'foi', 'fomos', 'foram', 'fora', 'fôramos', 'seja', 'sejamos', 'sejam', 'fosse', 'fôssemos', 'fossem', 'for', 'formos', 'forem', 'serei', 'será', 'seremos', 'serão', 'seria', 'seríamos', 'seriam', 'tenho', 'tem', 'temos', 'tém', 'tinha', 'tínhamos', 'tinham', 'tive', 'teve', 'tivemos', 'tiveram', 'tivera', 'tivéramos', 'tenha', 'tenhamos', 'tenham', 'tivesse', 'tivéssemos', 'tivessem', 'tiver', 'tivermos', 'tiverem', 'terei', 'terá', 'teremos', 'terão', 'teria', 'teríamos', 'teriam']
    #ADD BELLOW NEW STOPWORDS
    new_stop_words = ['rt', 'sao', 'r']
    stop_words.extend(new_stop_words)

    tweets_nsw = []
    for word in tweet_raw_text.split():
      if not (word in stop_words):
          tweets_nsw.append(word)

    return (tweets_nsw)

#Database URI and engine creation
DATABASE_URI = 'postgresql+psycopg2://postgres:postgres@localhost:5432/candidatos'
engine = sqlalchemy.create_engine(DATABASE_URI)

df_posts = pd.read_sql_table('posts', engine)
df_posts.drop(['tweet_datetime', 'username', 'retweets', 'favorites'], inplace=True, axis=1,errors='ignore')

records_counter = 0
for i in range(len(df_posts)):
    query = "SELECT * FROM analysis WHERE tweet_id = " + str(int(df_posts.iloc[ i , 1 ]))
    df_query = pd.read_sql_query(query, engine)

    if df_query.empty:

        tweet_id = int(df_posts.iloc[ i , 1 ])
        tweet_english = translate_tweet(df_posts.iloc[i, 0])
        polarity = calculate_polarity(tweet_english)
        raw_tweet = prepare_raw_text(df_posts.iloc[i, 0])
        words_list = remove_stop_words(raw_tweet)

        #df_analysis = pd.DataFrame(columns=['tweet_id', 'tweet_english', 'polarity', 'raw_tweet', 'words_list'])
        data = {'tweet_id':         [tweet_id],
                'tweet_english':    [tweet_english],
                'polarity':         [polarity],
                'raw_tweet':        [raw_tweet],
                'words_list':       [words_list]}

        df_analysis = pd.DataFrame(data)
        df_analysis.to_sql('analysis', con=engine, if_exists='append', index=False)
        records_counter +=1

        if (records_counter % 100) == 0 and records_counter>0:
            print(str(records_counter) + " rows added to the database.")

