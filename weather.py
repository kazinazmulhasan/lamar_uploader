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

'''
this function will collect data from website
'''
def collect_data():
	# get raw data
	data = requests.get(source_address).text
	# parse date and time
	date = re.findall("Most Recent Weather Conditions at:\s*(\d+)/(\d+)/(\d+)\s*(\d+):(\d+)", data)[0]
	# format date into mysql accepted format
	date = "%s-%s-%s %s:%s:00" % (date[2],date[0],date[1],date[3],date[4])
	# store the date
	weather_data.append(date)
	# for debugging
	# print(date)
	
	# parse latest weather data from raw data
	# replace all the newlines and multiple spaces into single space
	# for single line searching and ease of parsing data
	data = re.sub(r"\s+", " ", data)
	# search the table that contains the weather data
	data = re.search("<table.*?(?=</table)", data).group(0)
	# retrive the rows from the table
	data = re.findall("<tr.*?(?=</tr)", data)
	
	# start collecting data
	# get temperature
	weather_data.append(re.findall("<b>[^\d.-]*([\d.-]*)[^>]*?(?=</b)", data[1])[0])
	# get humidity
	weather_data.append(re.findall("<b>[^\d.-]*([\d.-]*)[^>]*?(?=</b)", data[4])[0])
	# get wind speed
	weather_data.append(re.findall("<b>[^\d.-]*([\d.-]*)[^>]*?(?=</b)", data[5])[0])
	# get humidity
	weather_data.append(re.findall("<b>[^\d.-]*([\d.-]*)[^>]*?(?=</b)", data[8])[0])
	logger.log("data has been collected")
	# for debugging
	# print(weather_data)

# this function will upload the collected data in the server
def upload_data():
	# build the url/query
	values = {}
	# include metadata
	values["req"] = "transmission"
	values["table"] = "weather"
	# include collected data
	values["id"] = weather_data[0]
	values["temperature"] = weather_data[1]
	values["humidity"] = weather_data[2]
	values["wind_speed"] = weather_data[3]
	values["solar_radiation"] = weather_data[4]
	# upload data
	response = requests.post(remoteserver, params=values).text
	# log
	logger.log("data has been uploaded" if response == "true" else "failed to upload data")

if __name__ == "__main__":
	# for debugging
	# print(datetime.datetime.now())
	logger.log("task begin")
	try:
		# try to collect weather data
		collect_data()
		# try to upload collected data
		upload_data()
	except Exception as e:
		# something unexpected happened
		# log the error for record and debugging help
		logger.log(e)
	logger.log("task end")
	# close logger
	logger.close()
	# for debugging
	# print(datetime.datetime.now())