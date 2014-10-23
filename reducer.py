#!/usr/bin/env python2

import sys

def main():
	#Replacing tabs to commas
	for line in sys.stdin:
		line = line.replace('\t', ',')
		sys.stdout.write(line)

if __name__ == "__main__":
	main()
