#!/usr/bin/env python
import json 
import sys
import collections
from mrjob.job import MRJob
import pandas as pd
from mr_diag import MRTagSimMatrix
import pylast

API_KEY = "4e1bb9a3fead3bb1975ddbe2e0b3b7c4"
API_SECRET = "678c5ab3251cf8b40b978c731f6bbb03"

username = "abernkopf"
password_hash = pylast.md5("I've removed my real password")

network = pylast.LastFMNetwork(api_key = API_KEY, api_secret = 
    API_SECRET, username = username, password_hash = password_hash)


def sim_diag():
	#You can run the calc to get the sim triangle programatically, but you have to go in and monkey wth the mr_diag.py code
	#and refactor it to take 'artist_data.txt' as its input. I recommend just running sim_matrix() it doens't take very long at all.
	#I could rewrite that input and configuration as a parameterized funciton, but we'll do that another time.
	mr_job = MRTagSimMatrix(args=['-r', 'local', 'artist_data.txt'])
	
	with mr_job.make_runner() as runner:
		runner.run()


def sim_matrix(write=True):
	with open('artist_data.txt', 'r') as f:
		print "Reading data..."
		data = f.readlines()
		data = [json.loads(item) for item in data]
		data = pd.Series(data)

		print 'Calcing similarity matrix, this doesnt take too long the matrix is only 1000^2 items'
		sims = [[round(JaccardIndex([item.items()[0][1] for item in tag1['tags']],[item.items()[0][1] for item in tag2['tags']]), 2) for tag2 in data] for tag1 in data]

		print "Putting sims in dataframe..."
		sims = pd.DataFrame(sims)
		names = [item['name'].encode('ascii', 'ignore') for item in data]
		sims.columns = names
		sims.index = names

		if write:
			print "writing data out to file..."
			sims.to_csv('artist_sims.csv')

		return sims

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


def see_sim_diff(sim):
	#This function gets top artist data from last.fm and prints out a comparison with our similarity measure, and theirs. 

	print "Getting top artists..."
	#This gets a list of 1000 of the top artists from the last.fm API
	top_artists = network.get_top_artists(limit=1000)
	#This list comprehension extracts the actual artist object from the above call
	top_artists = [artist[0] for artist in top_artists]

	cold_sims = [item for item in top_artists[0].get_similar()]
	similars = [(item[0].name,item[1]) for item in cold_sims]

	results = []
	print "Coldplay comparison with last.fm"
	for item in similars:
	    try:
	        #print item[0], "Jaccard calc: {}".format(sim['Coldplay'][item[0]])
	        #print item
	        results.append((item[0], item[1], sim['Coldplay'][item[0]]))
	    except Exception:
	        #print "Error {}".format(str(item))
	        results.append((item[0], round(item[1],2), "Error"))

	results = pd.DataFrame(results, columns = ['artist', 'Last.FM', 'JacMap'])
	print results
	results.index = results['artist']
	results = results[['Last.FM', 'JacMap']]

	#Remove points where we don't have the same data as last.fm
	results = results.loc[results['JacMap'] != "Error"]

	diffs = results['Last.FM'] - results['JacMap']
	error = round(diffs.apply(lambda x: x**2).mean(),2)
	print "Error in our matric: {}".format(error)

	return results.sort('JacMap')

	