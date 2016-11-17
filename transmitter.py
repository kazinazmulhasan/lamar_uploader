from threading import Thread
from copy import deepcopy as clone
import datetime
import re
import requests

config_filename = "config.dat"
webserver_hostname = "oxiago.com"
webserver_receiver_addr = "/lamar/receiver.php"
localserver_hostname = "192.168.101.113"
localserver_database_addr = "/services/user/records.xml"
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
		self.config = Config(config)
		self.logger = Logger(self.config.destination)
	
	def run(self):
		self.logger.log("transmitter on")
		self.get_last_datetime() and self.collect_data() and self.upload_data()
		self.logger.log("transmitter off")
		self.logger.close()

	def get_last_datetime(self):
		query = "http://%s%s?req=last_datetime&table=%s" % (webserver_hostname, webserver_receiver_addr, self.config.destination)
		response = requests.get(query).text
		print(response)
		
		if response[0:4] == "true":
			self.start_datetime = datetime.datetime.strptime(response[5:], "%Y-%m-%d %H:%M:%S")
			self.start_datetime += datetime.timedelta(0, update_interval*60)
			self.end_datetime = datetime.datetime.now()
			self.logger.log("last datetime received: "+response[5:])
			return True
		else:
			self.logger.log("failed to get last datetime!!")
			return False

	def collect_data(self):
		print(self.config.dependents)
		print(self.config.targets)
		
		# build the query/url
		query = "http://%s%s?begin=%s&end=%s&period=%d" % (localserver_hostname, localserver_database_addr, self.start_datetime.strftime("%d%m%Y%H%M%S"), self.end_datetime.strftime("%d%m%Y%H%M%S"), update_interval*60)
		# add all the variable names, we need to calculate our target variables
		for each in self.config.dependents.keys():
			query += "&var=%s.%s" % (self.config.source, each)
		# make the request
		response = requests.get(query)
		
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
			matches = re.findall(r"([A-Z]+)</id><value>([^<]+)", record)
			# convert the values to number and index/save them
			for match in matches:
				dependents[match[0]] = float(match[1])
			
			# calculate target variables using values from dependents variables
			for target in targets:
				for i in range(len(targets[target])):
					targets[target][i] = dependents[targets[target][i]]
				print(targets)
				targets[target] = sum(targets[target])
			# print(self.config.dependents)
			print(rid, end=" ")
			print(targets)
		
		self.logger.log("data collected")
		return True

	def upload_data(self):
		self.logger.log("data has been uploaded")
		return True

if __name__ == "__main__":
	logger = Logger("main", False)
	print("main logger opened")
	try:
		# read config file and load data
		config_data = ""
		file = open(config_filename, "r")
		print("config file opened")
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
		print("config data read")
		for group in config_data:
			# run/trigger transmitter
			Transmitter(group).start()
			break;
		print("all thread are running")
		# we successfully ran/triggered all the transmitters
		logger.log("transmission successful")
	except Exception as e:
		print("ooops!!")
		# something unexpected happened
		logger.log("transmission failed!!")
	logger.close()
	print("logger closed. exiting...")