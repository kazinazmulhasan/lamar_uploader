'''
This program will collect weather data from specific weather website,
filter/parse specific data from the page that we are interested in and
will upload the data to our server.

@author Kazi, Nazmul (Naz)
'''

import re
import requests
import datetime
from transmitter import Logger

source_address = "http://mesowest.utah.edu/cgi-bin/droman/meso_table_mesodyn.cgi?stn=LMAW4&unit=0&time=&hours=25&hour1=00&day1=0&month1=&year1=&radius=25&past=0&order=1"
remoteserver = "http://oxiago.com/lamar/receiver.php"

# required variables
weather_data = []
logger = Logger("weather")
date = ""

'''
this function will collect data from website
'''
def collect_data():
	global date
	# get raw data/source code of the webpage
	data = requests.get(source_address).text
	# parse date and time
	date = re.findall("Most Recent Weather Conditions at:\s*(\d+)/(\d+)/(\d+)\s*(\d+):(\d+)", data)[0]
	# format date into mysql accepted format
	date = "%s-%s-%s %s:%s:%s" % (date[2],date[0],date[1],date[3],date[4],"00")
	# convert it into a date class
	date = datetime.datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
	# for debugging
	# print(date)
	
	# parse latest weather data from raw data
	# replace all the newlines and multiple spaces into single space
	# for single line searching and ease of parsing data
	data = re.sub(r"\s+", " ", data)
	# search the table that contains the weather data
	# the target table is the third/last table in the page
	data = re.findall("<table.*?(?=</table)", data)[2]
	
	# retrive the rows from the table
	data = re.findall("<tr.*?(?=</tr)", data)
	# remove the first row that contains title of the columns
	data.pop(0)
	# parse weather data from next 4 rows
	for i in range(4):
		weather_data.append([])
		parse_weather_data(i, data[i])

def parse_weather_data(index, text):
	# find all the values
	values = re.findall("([\d.:]*)</td>", text)
	# update the date
	weather_data[index].append(get_date()) # date
	# save the weather data
	weather_data[index].append(values[1]) # temperature
	weather_data[index].append(values[4]) # humidity
	weather_data[index].append(values[5]) # wind speed
	weather_data[index].append(values[8]) # solar radiation
	logger.log("data has been collected for %s" % values[0].zfill(5))

def get_date():
	global date
	# store current date value to return
	old_date = str(date)
	# update date
	date -= datetime.timedelta(minutes=15)
	# return old date value
	return old_date

# this function will upload the collected data in the server
def upload_data(data):
	# build the url/query
	values = {}
	# include metadata
	values["req"] = "transmission"
	values["table"] = "weather"
	# include collected data
	values["id"] = data[0]
	values["temperature"] = data[1]
	values["humidity"] = data[2]
	values["wind_speed"] = data[3]
	values["solar_radiation"] = data[4]
	# upload data
	response = requests.post(remoteserver, params=values).text
	# response = "true"
	# log
	logger.log("data has been uploaded for %s" % data[0][11:16] if response == "true" else "failed to upload data")

if __name__ == "__main__":
	# for debugging
	# print(datetime.datetime.now())
	logger.log("task begin")
	try:
		# try to collect weather data
		collect_data()
		# try to upload collected data
		for i in range(4):
			upload_data(weather_data[i])
			# print(weather_data[i])
	except Exception as e:
		# something unexpected happened
		# log the error for record and debugging help
		logger.log(e)
	logger.log("task end")
	# close logger
	logger.close()
	# for debugging
	# print(datetime.datetime.now())