#!/usr/bin/env python
import json 
import sys
import collections
from mrjob.job import MRJob
import pandas as pd

"""
This class contains the MapReduce runner that contains the mapper and reducer that will calculate the similarity data.
It only calculates a traigular split of the similarity matrix, thus reducing the time complexity. 

This code could run on a Hadoop cluster. It uses the Jaccard similarity index as its metric for similarity,
and ouputs a couter number and a series of json objects. It takes about 10 min to run, far reduced from the O^2 of the square version.
"""

class MRTagSimMatrix(MRJob):

	# Here we initialize items in memory that all the runners will share.
	counter = 0
	sims_diag = {}
	data = []

	#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ MAP STAGE ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	def mapper_init(self):
		sys.stderr.write("\n"*10)
		sys.stderr.write("MAPPING...\n")
		sys.stderr.write("\n"*3)
		sys.stderr.flush()



	def mapper(self, _, tag):
		self.increment_counter('Mapper', 'Parser', 1)

		
		data = json.loads(tag)

		#Uncomment this code to runner for inverted artist data, run with 'inverted_artist_data.txt'
		tag = data['tag_name'].encode('ascii', 'ignore')
		artists = [artist.encode('ascii', 'ignore') for artist in data['artists']]
		yield tag, artists
		
		#This is code below parses out the data for the 'artist_data.txt' file
		#artist = data['name'].encode('ascii', 'ignore')
		#tags = [artist.items()[0][1].encode('ascii', 'ignore') for artist in data['tags']]
		#yield artist, tags

	def reducer_init(self):
		sys.stderr.write("\n"*10)
		sys.stderr.write("REDUCING...\n")
		sys.stderr.write("\n"*10)
		sys.stderr.flush()		


	def reducer(self, tag, artists):
		self.increment_counter('Meducer', 'Jaccard', 1)
		artists = list(artists)

		#uncomment it running inverted_artist_data.txt
		self.data.append((tag, artists))

		#self.data.append((artists, tag))
		result = {}

		tagA = tag
		#sys.stderr.write("Reducer A status: {} \n".format(str(artists)))
		#Iterate through the data in memory so far, this we take the triangular portion of the similarity matrix.
		#Because we don't iterate through the whole data set for each piece of data, we cut down on the O^2 complexity.
		for jac in self.data:

			#uncomment if running on inverted_artist_data.txt
			tagB = jac[0]
			#tagB = jac[0].items()[0][1]	

			sim_index = self.Jaccard(artists, jac[1][0], tagA, tagB)
			#sys.stderr.write("Reducer status: {} \n".format(str(tagB)))
			result.update({tagB:sim_index})

		result = {tagA:result}
		results = json.dumps(result)
		yield tagA, result
			

	def Jaccard(self, seta, setb, tagA, tagB):
			#Clean the results before they get ingested into the metric
			if seta == []:
				seta = []
			elif type(seta) == list and type(seta[0]) == list:
				seta = seta[0]
			
			if setb == []:
				setb = []
			elif type(setb) == list and type(setb[0]) == list:
				setb = setb[0]

			#Get sets for calc
			setA = set(seta)	
			setB = set(setb)
			intersect = setA & setB
			union = setA | setB

			#Jaccard conditionals. We decide that if the two setsa are empty, we'll say they're totally different because
			#we have no evidence (data) to claim they are similar.
			if tagA == tagB:
				sim_index = 1

			elif len(union) == 0:	
				sim_index = 0

			else:
				sim_index = len(intersect)/float(len(union))
				sim_index = round(sim_index,2)

			return sim_index


if __name__ == '__main__':
		
		mrjob = MRTagSimMatrix()
		mrjob.run()