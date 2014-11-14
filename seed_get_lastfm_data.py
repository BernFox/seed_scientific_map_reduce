#!/usr/bin/env python
import pylast
import json

#These are the things we'll need to set up a connection with the last.fm api
API_KEY = "4e1bb9a3fead3bb1975ddbe2e0b3b7c4"
API_SECRET = "678c5ab3251cf8b40b978c731f6bbb03"

username = "abernkopf"
password_hash = pylast.md5("I've removed my real password")

network = pylast.LastFMNetwork(api_key = API_KEY, api_secret = 
    API_SECRET, username = username, password_hash = password_hash)

#Below is the code we'll use to extract data from last.fm

print "Getting top artists..."
#This gets a list of 1000 of the top artists from the last.fm API
top_artists = network.get_top_artists(limit=1000)
#This list comprehension extracts the actual artist object from the above call
top_artists = [artist[0] for artist in top_artists]

print "Getting artist tags, this may take a few minutes..."
#This gets the top tags for each artist in the top_artists list
tags = [art.get_top_tags() for art in top_artists]

print "Formatting tags..."
#This list comprehension turns each list of tag objects in a list of tag dicts, this makes it easier to handle.
#I'm not getting rid of the original artist and tag objects becuase I might still need them. 
tags1 = [ [{'tag_name':item[0].name, 'weight':int(item[1])} for item in t] for t in tags]

print "Combining artist and tag data..."
#This list comp combines the artist name with theior tags, at this point we can just use the data
#and leave behind the rest the artist and tag objects, we just extract the data that we need
dict_list = [{'name':artist.name, 'tags':tags} for artist,tags in zip(top_artists,tags1)]

print "Turning data into json objects..."
#Lets turn this data into json so we can write it to a file
json_list = [json.dumps(item) for item in dict_list]

print "Writing json data to file..."
#Now lets write the data to a file so we can work with it later, we'll use the 'with' syntax, and map & lambda to get this done
with open("artist_data.txt","w") as f:
	map(lambda x: f.write( x + '\n' ), json_list)

print "Done!" 
