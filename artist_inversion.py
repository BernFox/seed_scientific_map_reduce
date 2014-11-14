#!/usr/bin/env python
import json
import itertools
import numpy as np
from operator import itemgetter


#Now we're going to invert the data set so that we can do inter-tag similarity.
#we'll do this by creating tag objects that have a list of artists associated with them, instead of artists 
#that have tags associated with them. Then we'll use the Jaccard similarity index on the artists associated 
#with each tag to compute a similarity value

with open("artist_data.txt","r") as f:
	print "Reading data..."
	data = f.readlines()
	data = [json.loads(item) for item in data]

	print "Gathering tags..."
	#Lets put all the tags in a set so we can gather all unique tags, we'll use a couple list comps 
	#and a set comprehension to achieve this
	tags = [item['tags'] for item in data]

	print "Getting tag lists as strings..."
	#In this intermediate step we are creating a list of strings, each string is actually a list cast 
	#to a string that contains all of the tags for each artist. We'll use this to reduce strings to 
	#be compared in a later step. This will help us speed up the data inversion in a later step.
	tag_lists = [str([tag['tag_name'] for tag in artist]) for artist in tags]
	
	print "Finishing tag accumulation..."
	#Here we continue the gathering of the tags from line 18
	tags = list(itertools.chain(*tags))
	tags = {tag['tag_name'] for tag in tags}

	print "FYI, there are {} unique tags in the data set".format(len(tags))

	#Lets turn that set back into a numpy list, this makes it easier to deal with and faster to iterate through
	tags = np.array(tags)
	tags = tags.tolist()

	print "Inverting data set..."
	#This step we are creating a list of dicts, each dict contains the tag name and a list that contains all the artists 
	#associated with that tag. We do this by using a dict comprehension inside of a list comp. The list comp iterates
	#Through all the tags, while the nested dict comp uses that tag to iterate through the tag_list data. The tag_list 
	#captures the tags for each artist in a string so we can use the 'in' operator, 
	#if the tag string is in the artist string, we add the artist to the list. 
	tags = [{'tag_name':tag, 'artists':[data[i]['name'] for i,artist in enumerate(tag_lists) if tag in artist]} for tag in tags]

	print "Sorting..."
	#We'll sort that tags array, just to be nice.
	tags = sorted(tags, key=itemgetter('tag_name'))

	print "Writing data to file..."
	#Now that we have successfully inverted the data set, we'll write it all as json objects to a file. 
	with open("inverted_artist_data.txt", "w") as g:
		map(lambda x: g.write( json.dumps(x) + '\n' ), tags)

print "Done!"
    


