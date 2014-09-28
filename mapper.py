#!/usr/bin/env python2

from itertools import chain, combinations
import sys
import urllib2
import subprocess
import os

#Function to produce combinations of genes
def powerset(iterable):
	combList = list(iterable)
	return chain.from_iterable(combinations(combList,n) for n in range(len(combList)+1))

def main():
	gens = sys.stdin.readlines()
	gens = [item.strip() for item in gens]
	gens = list(powerset(gens))

	#For Biogrid REST service we need to separate gens like "gene1|gene2|gene3"
	for i in range(0, len(gens)) :
		gens[i] = "|".join(str(x) for x in gens[i])

	#REST calls
	for i in range(1, len(gens)):
		url = 'http://webservice.thebiogrid.org/interactions/?searchNames=true&geneList=' + str(gens[i]) + '&taxId=all&accesskey=99380a201fcc2245c18f6e4d1fec9035'
		result = urllib2.urlopen(url).readlines()

	#Merging of gene names and REST answers
		for j in range(0, len(result)):
			resultStr = "".join(str(gens[i]) + '\t' + result[j])
			sys.stdout.write(resultStr)
	
if __name__ == "__main__":
	main()
