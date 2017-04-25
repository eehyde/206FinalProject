import unittest
import requests
import json
import tweepy
import sqlite3
import twitter_info
from collections import Counter
from pprint import pprint

##########
#Write functions to:
###Get and cache data based on a twitter search term
###Get and cache data about a twitter user
##########
CACHE_FNAME = '206_final_project_cache.json'

try:
		cache_file = open(CACHE_FNAME, 'r')
		cache_contents = cache_file.read()
		CACHE_DICTION = json.loads(cache_contents)
		cache_file.close()
except:
		CACHE_DICTION = {}

consumer_key = twitter_info.consumer_key 
consumer_secret = twitter_info.consumer_secret
access_token = twitter_info.access_token
access_token_secret = twitter_info.access_token_secret
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth, parser=tweepy.parsers.JSONParser())

def get_user_tweets(handle):
	unique_identifier = "twitter_{}".format(handle) 
	if unique_identifier in CACHE_DICTION: 
		print('using cached data to search for', handle)
		twitter_results = CACHE_DICTION[unique_identifier] 
		return twitter_results
	else:
		print('getting data from internet to search for', handle)
		results = api.user_timeline(handle) 
		CACHE_DICTION[unique_identifier] = results 
		f = open(CACHE_FNAME,'w')
		f.write(json.dumps(CACHE_DICTION))
		f.close() 
		return results

def search_twitter(search):
	unique_identifier = "twitter_{}".format(search) 
	if unique_identifier in CACHE_DICTION: 
		print('using cached data to search for', search)
		twitter_results = CACHE_DICTION[unique_identifier] 
		return twitter_results
	else:
		print('getting data from internet to search for', search)
		results = api.search(q=search) 
		CACHE_DICTION[unique_identifier] = results 
		f = open(CACHE_FNAME,'w')
		f.write(json.dumps(CACHE_DICTION))
		f.close() 
		return results

##########
#Write a function to get and cache data from the OMDB API
##########

def getMovieWithOMDB(title):
		url = 'http://www.omdbapi.com/?t={}'.format(title)
		if url in CACHE_DICTION:
			response_text = json.loads(CACHE_DICTION[url])
		else:
			response = requests.get(url)
			CACHE_DICTION[url] = response.text
			response_text = json.loads(response.text)
			cache_file = open(CACHE_FNAME, 'w')
			cache_file.write(json.dumps(CACHE_DICTION))
			cache_file.close()
		return response_text

##########
#Define a class Movie
###It should accept a dictionary that represents a movie, have at least 3 instance variables, and
###3 methods besides the constructor
##########

class Movie(object):

	def __init__(self, dictionary_input):
		self.movie_ID = dictionary_input["imdbID"]
		self.title = dictionary_input['Title']
		self.director = dictionary_input['Director']
		self.rating = dictionary_input['imdbRating']
		self.actor_string = dictionary_input["Actors"]
		self.languages = dictionary_input['Language']

	def get_list_of_actors(self):
		list_of_actors = self.actor_string.split(",")
		return list_of_actors

	def get_num_languages(self):
		list_of_languages = self.languages.split(",")
		return len(list_of_languages)


	def __str__(self):
		return "{} is directed by {} and has an IMDB rating of {}".format(self.title,self.director,self.rating)


# for x in movie_instances:
# 	print(x.get_list_of_actors())

###########
#[Optional] Define a class to handle the twitter data
#For example a class Tweet and/or class TwitterUser
##########

##########
#Pick 3 movie titles search terms and put them as strings in a list
##########

movie_list = ["Mulan","Inception","The Imitation Game"]

##########
#Make a requst to OMDB one each of the terms in the list above using the function you wrote
#in the begining of the program. Accumulate the results into a list.
##########

movie_data_from_OMDB =[getMovieWithOMDB(x) for x in movie_list]
#pprint(movie_data_from_OMDB)

##########
#Using the results from above, create a list of class instances
##########

movie_instances = []
for x in movie_data_from_OMDB:
	movie_instances.append(Movie(x))

##########
#Use the twitter search functions to search for either
###each of the directors of the three movies
### OR
###one star actor in each of the movies
### OR
###each of the titles of the movies
###Save a list of instances of a class, or a list of tweet dictionaries, or 
#a list of tweet tuples into a variable for later
##########

#this is a list of tuples in the form (movie, tweet dictionary)
twitter_movie_search = []

for x in movie_list:
	data = search_twitter(x)
	a = data["statuses"]
	for b in a:
		twitter_movie_search.append((x,b))

##########
#Create a database with three tables:
###Tweets:
#- Tweet Text
#- Tweet ID (PRIMARY KEY)
#- User
#- Movie Search
#- Num Favs
#- Num Retweets
###Users:
#- User ID (PRIMARY KEY)
#- User screen name
#- Total num favs
#- Description
###Movies:
#- ID (PRIMARY KEY)
#- Title
#- Director
#- Num languages
#- IMDB rating
#- Top billed actor
##########

conn = sqlite3.connect('finalproject_tweets.db')
cur = conn.cursor()

statement = 'DROP TABLE IF EXISTS Tweets'
cur.execute(statement)
statement = 'DROP TABLE IF EXISTS Users'
cur.execute(statement)
statement = 'DROP TABLE IF EXISTS Movies'
cur.execute(statement)

table_spec = 'CREATE TABLE IF NOT EXISTS '
table_spec += 'Tweets (tweet_id INTEGER PRIMARY KEY, '
table_spec += 'tweet_text TEXT, user_id TEXT, movie_search TEXT, num_favs INTEGER, num_retweets INTEGER)' #saying what type the categories should be in 
cur.execute(table_spec)

table_spec = 'CREATE TABLE IF NOT EXISTS '
table_spec += 'Users (user_id INTEGER PRIMARY KEY, '
table_spec += 'user_screen_name TEXT, total_num_favs INTEGER, description TEXT)'
cur.execute(table_spec)

table_spec = 'CREATE TABLE IF NOT EXISTS '
table_spec += 'Movies (id TEXT PRIMARY KEY, '
table_spec += 'title TEXT, director TEXT, num_languages INTEGER, IMDB_rating INTEGER, top_billed_actor TEXT)'
cur.execute(table_spec)

##### TO POPULATE THE TWEETS TABLE #####

tweet_id = []
tweet_text = []
user_id =[]
movie_search = []
num_favs = []
num_retweets = []
hashtags = [] #not in the database, a list of tuples (movie search, hashtag) 
#becasue not sure if it will be meaningful (or possible) to have a cell in a database with multiple values - 
#for now leaving it out of the database, but keeping it as a list

for x in twitter_movie_search:
	movie_search.append(x[0])
	tweet_id.append(x[1]['id'])
	tweet_text.append(x[1]['text'])
	user_id.append(x[1]['user']['id'])
	num_favs.append(x[1]['user']['favourites_count'])
	num_retweets.append(x[1]['retweet_count'])
	for hashtag in x[1]['entities']['hashtags']:
		hashtags.append((x[0],hashtag['text']))
tweet_tuples = zip(tweet_id,tweet_text,user_id,movie_search,num_favs,num_retweets)

statement = 'INSERT INTO Tweets VALUES (?, ?, ?, ?, ?, ?)'
for x in tweet_tuples:
	cur.execute(statement, x)
conn.commit()

##### TO POPULATE THE USERS TABLE #####
list_of_users_possible_repeats = []

for a_mention in twitter_movie_search:
	for y in a_mention[1]['entities']['user_mentions']:
		list_of_users_possible_repeats.append(y['screen_name'])
for a_user in twitter_movie_search:
	list_of_users_possible_repeats.append(a_user[1]['user']['screen_name'])

list_of_users = []

for x in list_of_users_possible_repeats:
	if x not in list_of_users:
		list_of_users.append(x)

#print(list_of_users)

user_user_id =[]
user_screen_name = []
user_num_favs = []
user_description = []
for x in list_of_users:
	user_info = get_user_tweets(x)
	for y in user_info:
		user_user_id.append(y['user']['id'])
		user_screen_name.append(y['user']['screen_name'])
		user_num_favs.append(y['user']['favourites_count'])
		user_description.append(y['user']['description'])
users_tweet_tuples = zip(user_user_id,user_screen_name,user_num_favs,user_description)

statement = 'INSERT OR IGNORE INTO Users VALUES (?, ?, ?, ?)'
for x in users_tweet_tuples:
    cur.execute(statement, x)
conn.commit()

##### TO POPULATE THE MOVIES TABLE #####
movie_ID = []
title = []
director = []
num_languages = []
rating= []
top_actor = []

for x in movie_instances:
	movie_ID.append(x.movie_ID)
	title.append(x.title)
	director.append(x.director)
	num_languages.append(x.get_num_languages())
	rating.append(x.rating)
	top_actor.append(x.get_list_of_actors()[0])
movie_tweet_tuples = zip(movie_ID,title,director,num_languages,rating,top_actor)

statement = 'INSERT OR IGNORE INTO Movies VALUES (?, ?, ?, ?, ?, ?)'
for x in movie_tweet_tuples:
    cur.execute(statement, x)
conn.commit()

##########
#Load the data into the database
##########

##########
###Process the data:
#Make at lease 3 queries to grab intersections of data (one must use join)
##########


##########
###Process the data:
#Using the data from the queries, use at least four of the processing mechanisms 
#(ex: list comprehension & counter from collections) to find something interesting/cool/weird
##########

##########
#Create an output file that is kind of like a "summary stats" page with a clear title and 
#with the data on different lines
##########




conn.close()
##########
#Write at least two test methods for each function or class method that is defined
##########
Casablanca = getMovieWithOMDB("Casablanca")
Casablanca_instance = Movie(Casablanca)
a = str(Casablanca_instance)
Casablanca_list_of_actors = Casablanca_instance.get_list_of_actors()
Casablanca_num_languages = Casablanca_instance.get_num_languages()
class Tests(unittest.TestCase):
	def test_movie_info(self):
		self.assertEqual(type(movie_data_from_OMDB),type(["list",1,2,3]),"Testing that the json information I got about each movie using the requests module is recoginized as a list")
	def test_movie_list(self):
		self.assertEqual(type(movie_list[0]),type("dghsgdhs"), "Testing that my list of movies contains strings")
	def test_movie_list2(self):
		self.assertTrue(len(movie_list)==3,"Testing that movie list has three movies in it")
	def test_movie_caching(self):
		cache_file = open("206_final_project_cache.json","r")
		file = cache_file.read()
		cache_file.close()
		self.assertTrue(movie_list[1] in file)
	def test_getMovieWithOMDB(self):
		self.assertEqual(type(Casablanca),type({})),"Testing that getMovieWthOMDB returns a dictionary"
	# def test_getMovieWithOMDB2(self):
	# 	self.assertEqual
	def test_strmethod(self):
		self.assertEqual(a,"Casablanca is directed by Michael Curtiz and has an IMDB rating of 8.5", "Testing that the __str__ method returns the correct information")
	# def test_strmethod2(self):
	# 	self.assertEqual(type(a))
	def test_getListofActors(self):
		self.assertEqual(type(Casablanca_list_of_actors),type([]),"Testing that get_list_of_actors returns a list")
	def test_getListofActors2(self):
		self.assertEqual(type(Casablanca_list_of_actors[0]),type("djhsjfhj"),"Testing that get_list_of_actors returns a list of strings")
	def test_getnumlanguages(self):
		self.assertEqual(type(Casablanca_num_languages),type(3),"Testing that get_num_languages returns an integer")
	def test_People_and_Hashtags(self):
		self.assertEqual(type(People_and_Hashtags),type({}),"Testing that People_and_Hashtags is a dictionary")
	def test_People_and_Hashtags2(self):
		self.assertEqual(type(People_and_Hashtags["FILL IN PERSON HERE ONCE I KNOW MY MOVIES"]),type("fhjdfh"),"Testing that People_and_Hashtags has at least one key with a string value")

if __name__ == "__main__":
	unittest.main(verbosity=2)