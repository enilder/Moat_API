import pandas as pd
import requests as requests
from requests.auth import HTTPBasicAuth
import json
import datetime as dt
from dateutil.relativedelta import *
import time

import pprint as pp

def format(json_result, date_range):
	data_obj = {}
	
	for key in json_result["results"]["details"][0]:
			data_obj[key] = []

	for result in json_result["results"]["details"]:
		for key, value in result.items():
			data_obj[key].append(value)
	
	result_frame = pd.DataFrame(data_obj)
	return result_frame

def query_format(responses):
	formatted_results = pd.concat(responses, ignore_index=True) # Move to a different function
	return formatted_results
	
	
# range calculators
def month_ranges(pd, cd):
	cd_diff = ((cd.year - pd.year) * 12) + (cd.month - pd.month)
	
	days_diff = cd.day - pd.day
	
	if days_diff >= 0:
		adjust=0
		
	else:
		adjust= -1
	cd_diff += adjust
	
	return cd_diff
	
def day_ranges(pd, cd):
	print(cd, pd)
	days_diff = cd - pd
	
	return days_diff.days
	
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
		self.granularity_list = ["days", "months", "week"]
		self.available_levels = ["level1", "level2", "level3", "level4"]
	

	def metric(self, *metric_list):
		# appends list of metrics to request
		
		#clear previous list
		self.query["metrics"] = []
		for val in metric_list:
			self.query["metrics"].append(val)
			
	def levels(self, *level_list):
		
		# clear previous list
		self.query["level"] = []
		
		for level in level_list:
			if level in self.available_levels:
				self.query["level"].append(level)
	
	
	def date_range(self, start_date, end_date, granularity=None):
		start_date = dt.datetime.strptime(start_date, "%Y-%m-%d")
		end_date = dt.datetime.strptime(end_date, "%Y-%m-%d")
		
		# clear dates
		self.query["dates"] = []
		
		if granularity:
			if granularity in self.granularity_list:
				print(granularity)
				if granularity == "days":
					self.query["dates"] = {"start_date": start_date, "end_date": end_date, "granularity": "date"}
				elif granularity == "months":
					self.query["dates"] = {"start_date": start_date, "end_date": end_date, "granularity": "date", "date_grouping": "&date_grouping=month"}
				elif granularity == "weeks":
					self.query["dates"] = {"start_date": start_date, "end_date": end_date, "granularity": "date", "date_grouping": "&date_grouping=week"}
		else:
			self.query["dates"].append((start_date, end_date))			
	
	def build(self, **options):
		
		option_dict = dict(**options)
		
		seperator = ","
		self.request_queue = {}
		
		url = "https://api.moat.com/1/stats.json?start={start_date}&end={end_date}&{filter}={filter_label}{group_date}&columns={date},{level_label},{metric_labels}".format(
		start_date= string_date(self.query["dates"].get("start_date", "")), 
		end_date= string_date(self.query["dates"].get("end_date", "")), 
		filter = option_dict.get("filter",""),
		filter_label = option_dict.get("filter_label",""),
		group_date = self.query["dates"].get("date_grouping", ""),
		date = self.query["dates"].get("granularity", ""),
		level_label=seperator.join(self.query["level"]), 
		metric_labels=seperator.join(self.query["metrics"]))
		print(url)
		self.request_queue[self.query["dates"]["start_date"]] = url
			
	
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
					print("Request ok. Sleeping....")
					time.sleep(5)
					#pp.pprint(result_json)
					self.responses.append(format(result_json, date))
					break
				except Exception as e:
					print("Error: {}.... Sleeping for 10".format(e))
					time.sleep(10)
					continue
		
		df_results = query_format(self.responses)
		df_results = self.datetimeConvert(df_results)
		df_results = self.metricConvert(df_results)
		return df_results
		
		
	def datetimeConvert(self, df_results):
		if "date" in df_results.columns:
			df_results["date"] = pd.to_datetime(df_results["date"])
		return df_results
		
	def metricConvert(self, df_results):
		# Convert any metric values using the datetime function
		
		metric_columns = [col for col in df_results if "date" not in col and "level" not in col]
		
		for metric in metric_columns:
			df_results[metric] = pd.to_numeric(df_results[metric], errors="coerce")
			if "perc" in metric:
				df_results[metric] /= 100
		return df_results