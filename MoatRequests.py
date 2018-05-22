import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
import json
import datetime as dt
from dateutil.relativedelta import *
import time

def _prettifyDate(date_name):
	return date_name.strftime("%Y-%m-%d")
	

class MoatApi():

	def __init__(self, username, password):
	
		self.username = username
		self.password = password
		self.request_queue = {}
		
	def _moatFormat(self, json_result):
		
		"""
		Formats api request results and
		returns results as a dataframe
		"""
		data_obj = {}
		
		for key in json_result["results"]["details"][0]:
				data_obj[key] = []

		for result in json_result["results"]["details"]:
			for key, value in result.items():
				data_obj[key].append(value)
		
		result_frame = pd.DataFrame(data_obj)
		return result_frame
		
	def sendQueries(self):
		
		"""
		Sends all queries in request queue.
		Returns results in dataframe
		"""
		self.responses = []
		for request_number, query_item in self.request_queue.items():
			print("Retrieving metrics for query #{}".format(request_number))
			for attempt in range(10):
				try:
					query_url = query_item.query_params["url"]
					result = requests.get(query_url, auth=HTTPBasicAuth(self.username, self.password))
					result_json = json.loads(result.text)
					print("Request ok. Sleeping....")
					print(result_json)
					time.sleep(5) # time out section to be nice with the api
					
					results_dataframe = self._moatFormat(result_json)
					
					#add date formating for different granularities
					
					self.responses.append(results_dataframe)
					break
				except Exception as e:
					print("Error: {}.... Sleeping for 10".format(e))
					time.sleep(10)
					continue
		
		#df_results = self._moatFormat(self.responses)
		df_results = pd.concat(self.responses, axis=0)
		df_results = self._datetimeConvert(df_results)
		df_results = self._metricConvert(df_results)
		return df_results
		
	def queueQuery(self, moat_query=None, **query_options):
		
		"""
		queues a Query in request queue.
		Can be used with a preexisting query
		Query paramaters can be passed with parameters
		"""
		request_number = len(self.request_queue.keys())
		
		if moat_query:
			self.request_queue[request_number] = self._urlGenerator(moat_query)
		
		elif query_options:
			print("Creating query based on parameters")
			dict(query_options) # converts query options to a dict
			
			query_instance = Query(query_options)
			self.request_queue[request_number] = self._urlGenerator(query_instance)
			pass
			
		else:
			print("No query or query parameters passed..")
			pass
		
	def _urlGenerator(self, moat_query):
		
		"""
		Generates request url
		"""
		seperator = ","
		
		query_params = moat_query.query_params # shortcut for query parameters
		
		request_url = "https://api.moat.com/1/stats.json?start={start_date}&end={end_date}&{filter}={filter_label}{group_date}&columns={date_breakdown},{level_label},{metric_labels}".format(
		start_date = _prettifyDate(query_params["dates"].get("start_date", "")), 
		end_date = _prettifyDate(query_params["dates"].get("end_date", "")), 
		filter = query_params.get("filter",""),
		filter_label = query_params.get("filter_label",""),
		group_date = query_params["dates"].get("date_grouping", ""),
		date_breakdown = query_params["dates"].get("granularity", ""),
		level_label=seperator.join(query_params.get("levels", [])), 
		metric_labels=seperator.join(query_params.get("metrics", [])))
		print(request_url)
		
		# appends query url to original query
		moat_query.query_params["url"] = request_url
		return moat_query
		
	def _datetimeConvert(self, df_results):
		if "date" in df_results.columns:
			df_results["date"] = pd.to_datetime(df_results["date"])
		return df_results
		
	def _metricConvert(self, df_results):
		# Convert any metric values using the datetime function
		
		metric_columns = [col for col in df_results if "date" not in col and "level" not in col and "label" not in col]
		
		for metric in metric_columns:
			df_results[metric] = pd.to_numeric(df_results[metric], errors="coerce")
			if "perc" in metric:
				df_results[metric] /= 100
		return df_results

		

class MoatQuery():

	"""
	Query Class
	"""


	def __init__(self, start_date, end_date=None, granularity=None, date_grouping=None, **query_options):
	
		self.GRANULARITY_LIST = ["day", "week", "month", "quarter"] # List of valid date aggregates
		self.FILTER_LIST = ["level1", "%d_%d", "os_broswer"]         # List of valid filters
		self.LEVEL_LIST = ["level1", "level2", "level3", "level4"]   # List of valid levels
		
		self.query_params = {"dates": {}
		}
		
		self.dateRange(start_date, end_date=end_date, granularity=granularity)
		
		"""
		self.query_params["dates"] = {"start_date": start_date,
		"end_date": end_date, "granularity": granularity}
		"""
	
	
	
	def _dateCheck(self, date_val):
		
		"""
		Validates date formats and inserts
		dates formats
		"""
		try:
			pd.to_datetime(date_val)
		except ValueError as e:
			print("{} is not a valid date string: {}".format(date_val, e))
			
		#return date_val
	
	def metrics(self, *metrics):
		
		"""
		Inserts metrics in query params
		"""
		# Check to see if metrics exists
		if self.query_params.get("metrics", False) == False:
			print("loading metrics")
			self.query_params["metrics"] = metrics
		else:
		# Inserts metrics if metric not already in Query Parameters
			metrics = [metric_label for metric_label in metrics if metric_label not in self.query_params["metrics"]]
			self.query_params["metrics"] += metrics
	
	
	def dateRange(self, start_date, end_date=None, granularity=None):
		
		"""
		Returns a date range object for the query
		"""
		# date checks
		self._dateCheck(start_date)
		self.query_params["dates"]["start_date"]  = start_date
		
		# Conditional check. If no end_date provided, will default to yesterday
		if end_date:
			self.query_params["dates"]["end_date"] = end_date
		else:
			end_date = dt.datetime.today() - dt.timedelta(days=1)
			print("No end date stated providing {} as end date".format(_prettifyDate(end_date)))
			self.query_params["dates"]["end_date"] = end_date
		
	
		# Convert datetime format to a "YYYY-mm-dd" format
		start_date = start_date.strftime("%Y-%m-%d")
		end_date = end_date.strftime("%Y-%m-%d")
		
		# If granularity specified, formats request url accordingly
	
		if granularity:
			if granularity in self.GRANULARITY_LIST:
				
				print("Granularity = {}".format(granularity))
				self.query_params["dates"]["granularity"] = "date"
				
				if granularity == "day":
					pass
				elif granularity == "month":
					self.query_params["dates"]["date_grouping"] = "&date_grouping=month"
				elif granularity == "week":
					self.query_params["dates"]["date_grouping"] = "&date_grouping=week"
				elif granularity == "quarter":
					self.query_params["dates"]["date_grouping"] = "&date_grouping=quarter"
			else:
				print("Not a valid date breakdown. All queries can be day, week, month or quarter")
				pass