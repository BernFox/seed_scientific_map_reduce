#!/usr/bin/env python
import json 
import numpy as np
import pandas as pd



#Here we'll write our own Jaccard Similarity Index funciton, just so we know exactly how it behaves
def JaccardIndex(seta, setb):
	setA = set(seta)
	setB = set(setb)

	intersect = setA & setB
	union = setA | setB

	if len(union) == 0:
		return 0

	sim_index = len(intersect)/float(len(union))

	return sim_index


def get_sims(f):
	print "Retrieving data..."
	data = f.readlines()
	print "Cleaning data..."
	data = [json.loads(item) for item in data]

	#Let's turn the data into a pandas series, and take advantage of pandas fast source code
	data = pd.Series(data)
	tags = pd.Series([tag['tag_name'].encode('ascii', 'ignore') for tag in data])

	print "Calculating similarity matrix. WARNING THIS IS SLOW.............."
 	sims = [[JaccardIndex(tag1['artists'],tag2['artists']) for tag2 in data] for tag1 in data]
 	sims = pd.DataFrame(sims)
 	sims.columns = tags
 	sim.index = tags

 	print "Writing out data. This also pretty slow, it has to write out over a gb."
 	sims.to_csv('tag_similarity.csv')

 	print "Done!"

if __name__ == '__main__':
	with open("inverted_artist_data.txt", "r") as f:
		sims = get_sims(f)