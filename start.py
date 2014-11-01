#!/usr/bin/env python2

import os
import getpass
import subprocess
from easygui import *

def main():
	#Introduction
	image="hadoop.gif"
	msgbox(msg="Welcome to the BioGrid Hadoop tool!\n\nPlease follow the steps below to use this tool:\n\n  1. Choose your input file\n\tPath: ?\n  2. Choose your output file destination \n\tPath: ?\n  3. Your default Hadoop folder where bin/hadoop runs \n\tPath: ?\n", title='BioGrid Hadoop Console', ok_button='Continue', image=image)

	#Get information about input/output paths and hadoop
	inputFile = fileopenbox(msg=None, title="1. Please select your input file", default='*', filetypes=None)

	if inputFile == None:
		msgbox(msg="You didn't choose input file. Closing program...", title='Error!', ok_button='OK', image=None)
		print "You didn't choose input file. Closing program..."
		return 1

	#Choose output directory
	msgbox(msg="Please follow the steps below to use this tool:\n\n  1. Choose your input file\n\tPath: " + inputFile +"\n  2. Choose your output file destination \n\tPath: ?\n  3. Your default Hadoop folder where bin/hadoop runs \n\tPath: ?\n", title='BioGrid Hadoop Console - Step 2', ok_button='Continue', image=image)

	outputFileDir = diropenbox(msg=None, title="2. Please select your output folder", default='*')

	if outputFileDir == None:
		msgbox(msg="You didn't choose output file. Closing program...", title='Error!', ok_button='OK', image=None)
		print "You didn't choose output file. Closing program..."
		return 1

	#Choose Hadoop directory
	msgbox(msg="Please follow the steps below to use this tool:\n\n  1. Choose your input file\n\tPath: " + inputFile +"\n  2. Choose your output file destination \n\tPath: " + outputFileDir + "\n  3. Your default Hadoop folder where bin/hadoop runs \n\tPath: ?\n", title='BioGrid Hadoop Console - Step 3', ok_button='Continue', image=image)

	hadoopDefaultDir = diropenbox(msg="where bin/hadoop runs", title="3. Default hadoop location", default="/home")
	
	if hadoopDefaultDir == None:
		msgbox(msg="You didn't specify your Hadoop folder. Closing program...", title='Error!', ok_button='OK', image=None)
		print "You didn't specify your Hadoop folder. Closing program..."
		return 1

	#Final check
	choice = buttonbox(msg="Final check:\n\n  1. Choose your input file\n\tPath: " + inputFile +"\n  2. Choose your output file destination \n\tPath: " + outputFileDir + "\n  3. Your default Hadoop folder where bin/hadoop runs \n\tPath: " + hadoopDefaultDir +"\n\nWhen you Click \"Run\" please wait until the current job is finished processing, because the job will run in the background.", title='BioGrid Hadoop Console', choices=["Run", "Abort"])

	if choice == "Abort":
		return 1

	#Reach bin/hadoop without $HADOOP environmental variable
	copyInputFile = hadoopDefaultDir + "/bin/hadoop"

	#Copy input file to the HDFS
	print "Copying input file to HDFS..."
	subprocess.call([copyInputFile, "dfs", "-copyFromLocal", inputFile, "input/input.txt"])

	#Initialization of parameters
	mapper = os.getcwd() + "/mapper.py"
	reducer = os.getcwd() + "/reducer.py"
	customJar = os.getcwd() + "/custom.jar"
	inputPath = "/user/" + getpass.getuser() + "/*"
	outputPath = "/user/" + getpass.getuser() + "/output/"
	os.chdir(hadoopDefaultDir)

	##################
	#Hadoop streaming#
	##################
	print "\nParameters has been initialized. Starting MR job...\n"
	subprocess.call(["bin/hadoop", "jar", "contrib/streaming/hadoop-streaming-1.2.1.jar", "-libjars", customJar, "-file", mapper, "-mapper", mapper, "-file", reducer, "-reducer", reducer, "-outputformat", "com.custom.CustomCSVFormat", "-input", inputPath, "-output", outputPath])

	#Copy CSV output to local
	print "Copying CSV to" + outputFileDir + '\n'
	outputPath = "/user/" + getpass.getuser() + "/output/*.csv"
	error = subprocess.call(["bin/hadoop", "dfs", "-get", outputPath, outputFileDir])

	if error == 0:
		print "Copied!\n"
	else:
		print "ERROR in command get...\n"

	#Delete HDFS
	print "Delete HDFS folders...\n"
	error = subprocess.call(["bin/hadoop", "dfs", "-rmr", "/user/"])
	if error != 0:
		print "ERROR!\n"

	print "\nDone."

	#Exit message
	msgbox(msg="Hadoop MR Job has been executed successfully. Gene interactions have been downloaded to " + outputFileDir + " folder.", title='BioGrid Hadoop Console', ok_button='Exit')

	return 0

if __name__ == "__main__":
	main()
