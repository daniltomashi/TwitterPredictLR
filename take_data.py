import tweepy as tw
import pandas as pd
import configparser
import numpy as np
import time
from datetime import datetime
from datetime import timedelta
import os


# read configs
config = configparser.ConfigParser()
config.read('../config/config.ini')

api_key = config['twitter']['api_key']
api_key_secret = config['twitter']['api_key_secret']
bearer_token = config['twitter']['bearer_token']
access_token = config['twitter']['access_token']
access_token_secret = config['twitter']['access_token_secret']


# authentication
auth = tw.OAuthHandler(api_key, api_key_secret)
auth.set_access_token(access_token, access_token_secret)

api = tw.API(auth)


# # find users
# i = 0
# max_count_of_users = 40000
# used_id = list(pd.read_csv('datasets/users.csv')['id'])
# max_id = 99999999+1


# while True:
#     users = pd.DataFrame(columns=['id', 'screen_name', 'followers_count', 'created_at', 'protected',\
#                         'verified', 'statuses_count', 'friends_count'])
#     if i != 0:
#         time.sleep(680)
#     # seven minutes to try again this loop
#     while i < max_count_of_users:
#         start_time = time.time()
#         user_id = np.random.randint(0,max_id)
#         if user_id in used_id:
#             continue

#         try:
#             user_data = api.get_user(user_id=user_id)
#             users.loc[i] = [user_data.id,user_data.screen_name, user_data.followers_count, user_data.created_at,\
#                         user_data.protected,user_data.verified, user_data.statuses_count, \
#                             user_data.friends_count]
#             i += 1
#             used_id.append(user_id)
#         except (tw.errors.NotFound, tw.errors.Forbidden):             
#             continue
#         except (tw.errors.TooManyRequests):
#             users.to_csv('datasets/users.csv', index=False, header=False, mode='a')
#             print(len(users), '\t', datetime.now(), '\t', time.time()-start_time)
#             break
        







# take tweets
client = tw.Client(bearer_token=bearer_token, consumer_key=api_key, consumer_secret=api_key_secret, \
    access_token=access_token, access_token_secret=access_token_secret)


was_exception = True
users = pd.read_csv('datasets/users.csv')
sleep = False
while was_exception:
    if sleep == True:
        # before 600
        time.sleep(480)
    else:
        sleep = True
    with open('start_with.txt', 'r') as file:
        ind = int(file.readline().strip())
    was_exception = False
    try:
        print(datetime.now(), end='\t')
        
        if os.path.exists(os.getcwd()+'/datasets/tweets.csv'):
            already_tweets = pd.read_csv('datasets/tweets.csv')
        else:
            already_tweets = pd.DataFrame(columns=['id', 'screen_name', 'text_id', 'text', 'retweet_count', \
                                    'reply_count', 'like_count', 'quote_count', 'lang', 'type', 'created_at'])
        while ind <= len(users)-1:
            tweets = pd.DataFrame(columns=['id', 'screen_name', 'text_id', 'text', 'retweet_count', \
                                    'reply_count', 'like_count', 'quote_count', 'lang', 'type', 'created_at'])
            j = 0
            user = users.loc[ind]
            user_tweets = already_tweets[already_tweets['id'] == user['id']]
            tweets_and_id = client.get_users_tweets(id=user['id'], exclude=['replies', 'retweets'], max_results=40,\
                                                    tweet_fields=['public_metrics', 'lang', 'referenced_tweets',\
                                                    'created_at'], expansions='author_id')

            if tweets_and_id.data != None:
                drop_or_no = 0
                for i in tweets_and_id.data:
                    if i.public_metrics['like_count'] < 10:
                        continue
                    if i.id not in user_tweets['text_id']:
                        tweets.loc[j, 'id'] = user['id']
                        tweets.loc[j, 'screen_name'] = user['screen_name']
                        to_append_text = str(i)
                        for repl in ['\n', '  ', '\t']:
                            to_append_text = to_append_text.replace(repl, ' ')
                        tweets.loc[j, 'text'] = to_append_text
                        tweets.loc[j, 'text_id'] = i.id
                        tweets.loc[j, 'created_at'] = i.created_at

                        for metr in i.public_metrics.items():
                            tweets.loc[j, metr[0]] = metr[1]

                        if i.referenced_tweets != None:
                            tweets.loc[j, 'type'] = i.referenced_tweets[0].type
                        else:
                            tweets.loc[j, 'type'] = 'tweet'
                        
                        if i.lang == 'en' or i.lang == 'eng':
                            tweets.loc[j, 'lang'] = i.lang
                            drop_or_no += 1
                        elif i.lang == 'zxx':
                            tweets.loc[j, 'lang'] = 'link'
                            drop_or_no += 1
                        else:
                            tweets.drop(j, axis=0)
                            j -= 1

                        j += 1

                if (drop_or_no / len(tweets_and_id.data)) < 0.75:
                    tweets = tweets[tweets['id'] != user.id]
                    with open('without_values_or_another_language.txt', 'a') as without:
                        without.write(f'{users.loc[ind]["id"]},another language\n')
                else:
                    if os.path.exists(os.getcwd()+'/datasets/tweets.csv'):
                        tweets.to_csv('datasets/tweets.csv', index=False, header=False, mode='a')
                    else:
                        tweets.to_csv('datasets/tweets.csv', index=False)
            else:
                with open('without_values_or_another_language.txt', 'a') as without:
                    without.write(f'{users.loc[ind]["id"]},no tweets\n')

            ind += 1

    except (tw.errors.TooManyRequests, tw.errors.TwitterServerError):
        print(datetime.now(), '\t')

        was_exception = True
        with open('start_with.txt', 'w') as file:
            file.write(str(ind))

