import pandas as pd
import requests as requests
from requests.auth import HTTPBasicAuth
import json
import datetime as dt
from dateutil.relativedelta import *
import time

def format(json_result, date_range):
	data_obj = {}
	
	for key in json_result["results"]["details"][0]:
			data_obj[key] = []

	for result in json_result["results"]["details"]:
		for key, value in result.items():
			data_obj[key].append(value)
	
	result_frame = pd.DataFrame(data_obj)
	
	result_frame["date"] = date_range
	
	return result_frame

# range calculators
def month_ranges(pd, cd):
	#cd = dt.datetime.strptime(cd, "%Y-%m-%d")
	#pd = dt.datetime.strptime(pd, "%Y-%m-%d")
	print(cd, dt)
	cd_diff = ((cd.year - pd.year) * 12) + (cd.month - pd.month)
	
	days_diff = cd.day - pd.day
	
	if days_diff >= 0:
		adjust=0
		
	else:
		adjust= -1
	cd_diff += adjust
	
	return cd_diff
	
def string_date(date_name):
	return date_name.strftime("%Y-%m-%d")

class moat_requests():
	
	def __init__(self, username, password):
		self.username = username
		self.password = password
		self.query = {
		"metrics": [],
		"level": [],
		"dates" : []
		}
		self.granularity_list = ["day", "months", "week"]
		self.levels = ["level1", "level2", "level3", "level4"]
	

	def metric(self, *metric_list):
		# appends list of metrics to request
		for val in metric_list:
			self.query["metrics"].append(val)
	
	
	def date_range(self, start_date, end_date, granularity=None):
		if granularity:
			if granularity in self.granularity_list:
				print(granularity)
				if granularity == "days":
					for i in range(0, date_range):
						self.query["dates"].append((start_date + relativedelta(days=i), start_date + relativedelta(days=i)))
				elif granularity == "months":
					start_date = dt.datetime.strptime(start_date, "%Y-%m-%d")
					end_date = dt.datetime.strptime(end_date, "%Y-%m-%d")
					periods = month_ranges(start_date, end_date) 
					print(periods)
					for i in range(0, periods):
						self.query["dates"].append((string_date(start_date + relativedelta(months=i)), string_date(start_date+relativedelta(months=i+1, days=-1))))
				elif granularity == "weeks":
					for i in range(0, date_range):
						self.query["dates"].append((start_date + relativedelta(i*7-6), start_date + relativedelta(days=i*7)))
		else:
			self.query["dates"].append((start_date, end_date))
	
	def build(self):
		# builds api request
		seperator = ","
		self.request_queue = {}
		#print(self.request_queue)
		for dates in self.query["dates"]:
			url = "https://api.moat.com/1/stats.json?start={}&end={}&columns={},{}".format(dates[0], dates[1], self.query["level"], seperator.join(self.query["metrics"]))
			self.request_queue[dates[0]] = url
	
	def send_query(self):
		# sends query
		self.responses = []
		for date, request_url in self.request_queue.items():
			print("Retrieving metrics for {}".format(date))
			for attempt in range(10):
				try:
					result = requests.get(request_url, auth=HTTPBasicAuth(self.username, self.password))
					result_json = json.loads(result.text)
					# time out section to be nice with the api
					print("Sleeping....")
					time.sleep(5)
					#print(result_json)
					self.responses.append(format(result_json, date))
					break
				except:
					print("Error")
					time.sleep(10)
					continue
		
		formatted_results = pd.concat(self.responses, ignore_index=True)
		return formatted_results
	
	"""
	def moat_filter_req(dates, levelid, metrics=["impressions_analyzed"], level="level2", filter_label="level1"):
		# filter down through a list of items
		seperator = ","
		result = req.get("https://api.moat.com/1/stats.json?start={}&end={}&{}={}&columns={},{}" \
                     .format(dates[0], dates[1], filter_label, levelid ,level, seperator.join(metrics)),
                     auth=HTTPBasicAuth(self.username, self.password))
    result_json = json.loads(result.text)
    return result_json
	"""
	
	"""
	def filter_format(json_res, date_value, levelid, label):
		data_obj = {}

		#  Retrieves the key values for data_obj
		for key in json_res["results"]["details"][0]:
			data_obj[key] = []

		for result in json_res["results"]["details"]:
			for key, value in result.items():
				data_obj[key].append(value)

		# insert into a dataframe
		moat_frame = pd.DataFrame(data_obj)

		print(moat_frame)

		# date added
		moat_frame["date_val"] = date_value

		# id added
		moat_frame["level1_id"] = levelid

		# label added
		moat_frame["level1_label"] = label

		return moat_frame
	"""