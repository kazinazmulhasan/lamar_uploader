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

config_filename = "config.dat"
remoteserver = "http://oxiago.com/lamar/receiver.php"
localserver = "http://192.168.101.113/services/user/records.xml"
update_interval = 30 # in minutes

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
		for group in config_data:
			# run/trigger transmitter
			Transmitter(group).start()
		# we successfully ran/triggered all the transmitters
		logger.log("transmission successful")
	except Exception as e:
		# something unexpected happened
		logger.log("transmission failed!!")
	logger.close()