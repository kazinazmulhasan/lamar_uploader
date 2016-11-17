import http.client as network
import urllib.parse as url
import datetime
from threading import Thread
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
		self.get_last_datetime2()
		self.collect_data()
		# self.upload_data()
		self.logger.log("transmitter off")
		self.logger.close()

	def get_last_datetime(self):
		connection = network.HTTPSConnection(webserver_hostname)
		params = url.urlencode({"req":"last_datetime", "table":self.config.destination})
		connection.request("GET", webserver_receiver_addr+"?"+params)
		response = connection.getresponse().read().decode('utf-8')
		connection.close()
		
		if response[0:4] == "true":
			self.start_datetime = datetime.datetime.strptime(response[5:], "%Y-%m-%d %H:%M:%S")
			self.start_datetime += datetime.timedelta(0, update_interval*60)
			self.end_datetime = datetime.datetime.now()
			self.logger.log("last datetime received: "+response[5:])
			return True
		else:
			self.logger.log("failed to get last datetime!!")
			return False
	
	def get_last_datetime2(self):
		self.start_datetime = datetime.datetime.strptime("2016-11-14 00:00:00", "%Y-%m-%d %H:%M:%S")
		self.end_datetime = datetime.datetime.strptime("2016-11-14 01:00:00", "%Y-%m-%d %H:%M:%S")
		return True

	def collect_data(self):
		# connection = network.HTTPSConnection(localserver_hostname)
		# query = "%s?begin=%s&end=%s&period=%d" % (localserver_database_addr, self.start_datetime.strftime("%d%m%Y%H%M%S"), self.end_datetime.strftime("%d%m%Y%H%M%S"), update_interval*60)
		# for each in self.config.dependents.keys():
		# 	query += "&var=%s.%s" % (self.config.source, each)
		# connection.request("GET", query)
		# response = connection.getresponse().read().decode('utf-8')
		# connection.close()
		print("collecting data")
		query = "%s?begin=%s&end=%s&period=%d" % (localserver_database_addr, self.start_datetime.strftime("%d%m%Y%H%M%S"), self.end_datetime.strftime("%d%m%Y%H%M%S"), update_interval*60)
		for each in self.config.dependents.keys():
			query += "&var=%s.%s" % (self.config.source, each)
		print("making request")
		res = requests.get(query)
		print(res.text)
		
		records = re.split("(?:</record>)*(?:<record>)", response)
		records.pop(0)
		for record in records:
			rid = re.search("<dateTime>(\d+)", record).group(1)
			rid = datetime.datetime.strptime(rid, "%d%m%Y%H%M%S").strftime("%Y-%m-%d %H:%M:%S")
			matches = re.findall(r"([A-Z]+)</id><value>([^<]+)", record)
			for match in matches:
				self.dependents[match[0]] = float(match[1])
		
		for target in self.targets:
			for i in range(len(self.targets[target])):
				self.targets[target][i] = self.dependents[self.targets[target][i]]
			self.targets[target] = sum(self.targets[target])
		print(self.targets)
		
		self.logger.log("data collected")
		return True

	def upload_data(self):
		self.logger.log("data has been uploaded")
		return True

if __name__ == "__main__":
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
			break;
		# we successfully ran/triggered all the transmitters
		logger.log("transmission successful")
	except Exception as e:
		# something unexpected happened
		logger.log("transmission failed!!")
	logger.close()