import pymongo
import tweepy as twp


def get_mongo_db():
    connection = pymongo.MongoClient('ds159574.mlab.com', 59574)
    db = connection['4plus_twitter_scrapper']
    db.authenticate('chain', 'chain-mongo-123')

    return db


def get_tweepy_auth():
    consumer_key = "0FXPrGRN7kUzoJQ8LRqZ5dvvp"
    consumer_secret = "Lf9LtdM0JeUD4RiegGLnDzYeA3tnYZGSEYoGbxTwsOf4jpLQwL"
    access_token = "1189699308-7SEfiKt3E5GOOyHkE2sBIzxImHC6j0ELpBPuY0Q"
    access_token_secret = "3XlsGtXzOd4tFnQFzNkMNy2lD1fupqKH7RkIgLRpNUX34"

    auth = twp.OAuthHandler(consumer_key,consumer_secret)
    auth.set_access_token(access_token,access_token_secret)

    return auth
