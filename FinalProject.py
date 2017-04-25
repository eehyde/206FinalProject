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
		try:
			results = api.user_timeline(handle) 
			CACHE_DICTION[unique_identifier] = results 
			f = open(CACHE_FNAME,'w')
			f.write(json.dumps(CACHE_DICTION))
			f.close() 
			return results
		except:
			pass

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


##########
#Pick 3 movie titles search terms and put them as strings in a list
##########

movie_list = ["The Big Short","Mulan","The Imitation Game"]

##########
#Make a requst to OMDB one each of the terms in the list above using the function you wrote
#in the begining of the program. Accumulate the results into a list.
##########

movie_data_from_OMDB =[getMovieWithOMDB(x) for x in movie_list]
#pprint(movie_data_from_OMDB[0])

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
#pprint(twitter_movie_search[0])

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
#- Number of followers
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
table_spec += 'tweet_text TEXT, user_id INTEGER, movie_search TEXT, num_favs INTEGER, num_retweets INTEGER)' #saying what type the categories should be in 
cur.execute(table_spec)

table_spec = 'CREATE TABLE IF NOT EXISTS '
table_spec += 'Users (user_id INTEGER PRIMARY KEY, '
table_spec += 'user_screen_name TEXT, total_num_favs INTEGER, description TEXT, number_of_followers INTEGER)'
cur.execute(table_spec)

table_spec = 'CREATE TABLE IF NOT EXISTS '
table_spec += 'Movies (id TEXT PRIMARY KEY, '
table_spec += 'title TEXT, director TEXT, num_languages INTEGER, IMDB_rating INTEGER, top_billed_actor TEXT)'
cur.execute(table_spec)

##########
#Load the data into the database
##########
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
num_followers = []
for x in list_of_users:
	user_info = get_user_tweets(x)
	for y in user_info:
		user_user_id.append(y['user']['id'])
		user_screen_name.append(y['user']['screen_name'])
		user_num_favs.append(y['user']['favourites_count'])
		user_description.append(y['user']['description'])
		num_followers.append(y['user']['followers_count'])
users_tweet_tuples = zip(user_user_id,user_screen_name,user_num_favs,user_description,num_followers)

statement = 'INSERT OR IGNORE INTO Users VALUES (?, ?, ?, ?, ?)'
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
###Process the data:
#Make at lease 3 queries to grab intersections of data (one must use join)
##########

query = 'SELECT Tweets.num_favs,Users.user_screen_name,Tweets.tweet_text FROM Tweets INNER JOIN Users ON Tweets.user_id=Users.user_id WHERE Tweets.num_favs>500'
cur.execute(query)
text_with_more_than_500_favs = cur.fetchall()
#print(text_with_more_than_500_favs)
query = 'SELECT Tweets.movie_search,Users.user_screen_name,Tweets.tweet_text FROM Tweets INNER JOIN Users ON Tweets.user_id=Users.user_id WHERE Users.number_of_followers>500'
cur.execute(query)
text_with_more_than_500_followers = cur.fetchall()
#print(text_with_more_than_500_followers)
query = 'SELECT movie_search FROM Tweets WHERE num_retweets > 1'
cur.execute(query)
searched_movies = cur.fetchall()
#print(searched_movies)
##########
###Process the data:
#Using the data from the queries, use at least four of the processing mechanisms 
#(ex: list comprehension & counter from collections) to find something interesting/cool/weird
##########

### USING CONTAINER FROM COLLECTIONS LIBRARY ###
### LIST COMPREHENSION ###
#The movie search that had the most tweets with retreats greater than 1:
searched_movies_as_a_list = [title for atuple in searched_movies for title in atuple]
counter_obj_of_movies = Counter(searched_movies_as_a_list).most_common()
#print(counter_obj_of_movies)

### SORTING WITH A KEY PARAMETER ###
sorted_tuples = sorted(text_with_more_than_500_favs, key = lambda x: x[0],reverse=True)

### DICTIONARY ACCUMULATION ###
movie_dict = {}
for (movie, user, text) in text_with_more_than_500_followers:
	if movie not in movie_dict:
		movie_dict[movie] = 1
	else:
		movie_dict[movie] += 1
sorted_movie_dict = sorted(movie_dict, key = lambda x: movie_dict[x], reverse=True)


##########
#Create an output file that is kind of like a "summary stats" page with a clear title and 
#with the data on different lines
##########
summary_file = open('summary_file.txt',"w")
summary_file.write("Summary File for SI206 Final Project" + "\n")
summary_file.write("The movie search that had the most tweets associated with it, with at least 1 retweet: "+ str(counter_obj_of_movies[0][0])+ "\n")
summary_file.write("The Tweet: " + str(sorted_tuples[0][2]) + " had " + str(sorted_tuples[0][0]) + " favorites, and this is the highest number of favorites of any of the searched tweets."+ "\n")
summary_file.write("The movie " + sorted_movie_dict[0] + " had the most tweets from accounts with more than 500 followers."+ "\n" )
summary_file.close()

conn.close()
##########
#Write at least two test cases for each function or class method that is defined
##########

Casablanca = getMovieWithOMDB("Casablanca")
Casablanca_instance = Movie(Casablanca)
a = str(Casablanca_instance)
Casablanca_list_of_actors = Casablanca_instance.get_list_of_actors()
Casablanca_num_languages = Casablanca_instance.get_num_languages()
twitter_test = get_user_tweets('eehyde19')
twitter_test2 = search_twitter('umich')
pprint(twitter_test2)
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
	def test_getusertweets(self):
		self.assertEqual(type(twitter_test),type([]),"Testing that get_user_tweets returns a list")
	def test_getusertweets2(self):
		self.assertEqual(type(twitter_test[0]),type({}),"Testing that the elements in the list that is returned from get_user_tweets are dictionaries")
	def test_searchtwitter(self):
		self.assertEqual(type(twitter_test2),type({}),"Testing that search_twitter returns a dictionary")
	def test_searchtwitter2(self):
		self.assertEqual(type(twitter_test2['statuses']), type([1,2]),"Testing that search_twitter has a key 'statuses' with tweets in a list as the value")
	def test_getMovieWithOMDB(self):
		self.assertEqual(type(Casablanca),type({})),"Testing that getMovieWthOMDB returns a dictionary"
	def test_getMovieWithOMDB2(self):
		cache_file2 = open("206_final_project_cache.json","r")
		file2 = cache_file2.read()
		cache_file2.close()
		self.assertTrue('http://www.omdbapi.com/?t=Casablanca' in file2)
	def test_strmethod(self):
		self.assertEqual(a,"Casablanca is directed by Michael Curtiz and has an IMDB rating of 8.5", "Testing that the __str__ method returns the correct information")
	def test_strmethod2(self):
		self.assertEqual(type(a), type("djsdjs"),"Testing that the __str__ method does actually return a string")
	def test_getListofActors(self):
		self.assertEqual(type(Casablanca_list_of_actors),type([]),"Testing that get_list_of_actors returns a list")
	def test_getListofActors2(self):
		self.assertEqual(type(Casablanca_list_of_actors[0]),type("djhsjfhj"),"Testing that get_list_of_actors returns a list of strings")
	def test_getnumlanguages(self):
		self.assertEqual(type(Casablanca_num_languages),type(3),"Testing that get_num_languages returns an integer")
	def test_getnumlangauges2(self):
		self.assertEqual(len(str(Casablanca_num_languages)),len([1]),"Testing that get_num_languages only returns one number")

if __name__ == "__main__":
	unittest.main(verbosity=2)