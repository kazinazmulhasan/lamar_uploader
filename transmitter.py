'''
The program will parse the config file and for each house/building mentioned
in the config file, system will create a new thread and run program to collect
data from the server for specified house/building. Since system will create
a new thread for each house/building, one's failure won't affect another's
operation.

@author Kazi, Nazmul (Naz)
'''

from threading import Thread
from copy import deepcopy as clone
import datetime
import re
import requests
from time import *

config_filename = "config.dat"

# This page will receive all the data we pull from EDS server,
# will decide what to do with the data and will send a reply
# based on what this system will decide what to do next
remoteserver = "http://oxiago.com/lamar/receiver.php"

# address of EDS server
localserver = "http://192.168.101.113/services/user/records.xml"

# update interval is how often EDS server records the data
# this is not how often we should pull the data
# wrong value for update interval can result in receiving
# no data or less data from EDS server.
update_interval = 30 # in minutes
# TTL stands for Time to Live, if a thread is still running
# after ttl, the thread will be terminated forcefully by the
# parent thread. start time doesn't depend on child threads.
# as soon as the parent thread will be executed the clock will
# start ticking.
# thread_TTL must be not greater than update_interval - 5
# this way all threads will be terminated 5 mins before the
# program will be executed again.
# However, average run time of this program is 4 seconds
thread_TTL = 25 # in minutes

class Logger:
	def __init__(self, filename, grouping=True):
		self.file = open("log/"+filename+".log", "a")
		self.grouping = grouping

	def log(self, text):
		self.file.write("%s: %s\n" % (datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"), text))

	def close(self):
		if self.grouping:
			self.file.write("\n")
		self.file.close()

class Config:
	def __init__(self, data):
		data = data.split("\n")
		head = data.pop(0)
		head = re.split("\s*->\s*", head)
		self.source = head[0]
		self.destination = head[1]
		self.targets = {}
		self.dependents = {}
		for line in data:
			line = re.split("\s*->\s*", line)
			line[0] = re.split("\s+", line[0])
			self.targets[line[1]] = line[0]
			for each in line[0]:
				self.dependents[each] = ""

class Transmitter(Thread):
	def __init__(self, config):
		Thread.__init__(self)
		# parse configuration
		self.config = Config(config)
		# open logger for logging
		self.logger = Logger(self.config.destination)
		self.stop = False
	
	def run(self):
		self.logger.log("transmitter on")
		# get last datetime from remote server, then collect data, then upload data
		self.get_last_datetime() and self.collect_and_upload_data()
		self.logger.log("transmitter off")
		# close logger
		self.logger.close()
		print(datetime.datetime.now())

	def get_last_datetime(self):
		# build the query
		query = "%s?req=last_datetime&table=%s" % (remoteserver, self.config.destination)
		# make the request
		response = requests.get(query).text
		
		# analyse response from remote server
		if response[0:4] == "true":
			# set start date
			self.start_datetime = datetime.datetime.strptime(response[5:], "%Y-%m-%d %H:%M:%S")
			# the last datetime we receive already exist in the database
			# so we need to add the update interval time to avoid this record
			self.start_datetime += datetime.timedelta(0, update_interval*60)
			# set end datetime to current time
			self.end_datetime = datetime.datetime.now()
			self.logger.log("last datetime received: "+response[5:])
			# return true, otherwise next operation won't run
			return True
		else:
			# didnt receive any datetime from remote server
			self.logger.log("failed to get last datetime!!")
			# abort
			return False

	def collect_and_upload_data(self):
		# build the query/url
		query = "%s?begin=%s&end=%s&period=%d" % (localserver, self.start_datetime.strftime("%d%m%Y%H%M%S"), self.end_datetime.strftime("%d%m%Y%H%M%S"), update_interval*60)
		# add all the variable names, we need to calculate our target variables
		for each in self.config.dependents.keys():
			query += "&var=%s.%s" % (self.config.source, each)
		# make the request
		response = requests.get(query)
		# log
		self.logger.log("data collected")
		
		# parse the response data to records
		# each record has different timestamp than others
		records = re.split("(?:</record>)*(?:<record>)", response.text)
		# remove the first one from the stack
		# the first one contains no record but xml info.
		records.pop(0)
		
		# process each record
		for record in records:
			if self.stop:
				break
			# make a copy of target and dependents
			targets = clone(self.config.targets)
			dependents = clone(self.config.dependents)
			# extract datetime from record
			rid = re.search(r"<dateTime>(\d+)", record).group(1)
			# format datetime to match server/database format
			rid = datetime.datetime.strptime(rid, "%d%m%Y%H%M%S").strftime("%Y-%m-%d %H:%M:%S")
			# find all the values we requested
			matches = re.findall(r"(\w+)</id><value>([^<]+)", record)
			# convert the values to number and index/save them
			for match in matches:
				dependents[match[0]] = round(float(match[1]), 6)
			
			# calculate target variables using values from dependents variables
			for target in targets:
				for i in range(len(targets[target])):
					targets[target][i] = dependents[targets[target][i]]
				targets[target] = sum(targets[target])
			# upload data
			# if server didnt get the data, abort
			# aborting is imprtant. this way we wont 
			# have missing data in the middle
			# rest of the data will be uploaded in
			# the beginning of next transmission
			ack = self.upload_data(rid, targets)
			if not ack:
				break

	def upload_data(self, rid, values):
		# set required parameters
		values["req"] = "transmission"
		values["table"] = self.config.destination
		values["id"] = rid # rid stands for record id and it is datetime of the record 
		# make the request
		response = requests.post(remoteserver, params=values).text
		if response == "true": # data has been ackowledged (ACK)
			self.logger.log("%s ACK" % rid)
			return True
		else:
			self.logger.log("%s NACK\nAborting" % rid) # NACK stands for Not ACKnowledged
			return False

if __name__ == "__main__":
	print(datetime.datetime.now())
	# open main logger
	logger = Logger("main", False)
	try:
		# list of transmitters
		transmitter_L = []
		# read config file and load data
		config_data = ""
		file = open(config_filename, "r")
		ignore_newline = False
		while True:
			line = file.readline()
			if not line:
				break
			if line[0] == "#" or (line == "\n" and ignore_newline):
				ignore_newline = True
				continue
			config_data += line
			ignore_newline = False
		# trim any extra newlines at the end
		config_data = config_data.rstrip()
		# split data into groups by double or more newlines
		config_data = re.split("\n{2,}", config_data)
		# create a thread for each group
		for group in config_data:
			transmitter_L.append(Transmitter(group))
		# start/run all the threads/transmitters
		for transmitter in transmitter_L:
			transmitter.start()
		# check if all threads are dead or not
		# if not, check again after 1 min
		# if yes, join all the threads with parent thread and exit
		while thread_TTL > 1:
			# check if all threads are dead
			all_dead = True
			for transmitter in transmitter_L:
				if Transmitter.isAlive(): # still running
					all_dead = False
			# no thread is running, kill/join all with parent
			if all_dead:
				for transmitter in transmitter_L:
					transmitter.join()
			# wait another minute, check again
			else:
				thread_TTL -= 1
				sleep(1)
		# system had enough time to finish it's task but we can't allow
		# anymore time. We need to start killing all the thread before
		# next program runs.
		if thread_TTL <= 1:
			# request to stop processing and return to parent
			for transmitter in transmitter_L:
				transmitter.stop = True
			# join all with parent
			for transmitter in transmitter_L:
				transmitter.join()
			
		# we successfully ran/triggered all the transmitters
		logger.log("transmission successful")
	except Exception as e:
		# something unexpected happened
		logger.log("transmission failed!!")
	logger.close()