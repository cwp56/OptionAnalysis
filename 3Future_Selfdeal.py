import os
import sys
from datetime import datetime, timedelta
from influxdb import InfluxDBClient

dataPath = os.getcwd()
if __name__ == '__main__':
	# Parse the input parameters
	try:
		paras = sys.argv
		year = int(paras[1])  # The year of the query date
		month = int(paras[2])  # The month of the query date
		day = int(paras[3])  # The day of the query date
		duration = int(paras[4])  # Query duration (seconds)
	except:
		print('Parameter error')
		exit(1)
	
	# query date and duration
	queryDate = datetime(year, month, day)
	durationSecs = duration
	print(queryDate.strftime('%Y-%m-%dT%H:%M:%SZ'))
	
	# All distinct trade IDs
	client = InfluxDBClient('localhost', 8086, 'root', 'root', 'test')
	queryStr = "select distinct internal_order_id as internal_order_id from zce_future_trade where '%s' <= time and time < '%s';" % (queryDate.strftime('%Y-%m-%dT%H:%M:%SZ'), (queryDate + timedelta(seconds=durationSecs)).strftime('%Y-%m-%dT%H:%M:%SZ'))
	result = client.query(queryStr)
	client.close()
	tradeIds = {}  # Trade IDs
	for p in result.get_points():
		internal_order_id = p['internal_order_id']
		if internal_order_id in tradeIds:
			raise Exception
		tradeIds[internal_order_id] = 0
	
	# quote & quote
	client = InfluxDBClient('localhost', 8086, 'root', 'root', 'test')
	queryStr = "select distinct internal_quote_id as internal_quote_id from zce_option_quote where note = 'QS准备下单' and '%s' <= time and time < '%s';" % (queryDate.strftime('%Y-%m-%dT%H:%M:%SZ'), (queryDate + timedelta(seconds=durationSecs)).strftime('%Y-%m-%dT%H:%M:%SZ'))
	result = client.query(queryStr)
	client.close()
	quoteIds = {}
	for p in result.get_points():
		quoteIds[p['internal_quote_id']] = 0
	quoteSelfIds = []
	# quote_id, ask_id, bid_id
	for id in tradeIds:
		askId = id + 1
		bidId = id + 2
		if id in quoteIds and askId in quoteIds and bidId in quoteIds:
			quoteSelfIds.append(id)
	print(len(quoteSelfIds), quoteSelfIds)
	
	# order & order
	client = InfluxDBClient('localhost', 8086, 'root', 'root', 'test')
	queryStr = "select internal_order_id from zce_option_order where note = 'OS准备下单' and match_condition = 0 and '%s' <= time and time < '%s';" % (queryDate.strftime('%Y-%m-%dT%H:%M:%SZ'), (queryDate + timedelta(seconds=durationSecs)).strftime('%Y-%m-%dT%H:%M:%SZ'))
	result = client.query(queryStr)
	client.close()
	orderIDs = {}
	for p in result.get_points():
		internal_order_id = p['internal_order_id']
		if internal_order_id not in orderIDs:
			orderIDs[internal_order_id] = 0
		orderIDs[internal_order_id] += 1
	orderSelfIds = []
	for id in tradeIds:
		if id in orderIDs:
			orderSelfIds.append(id)
	print(len(orderSelfIds), orderSelfIds)
	
	# quote & order
	quote_orderSelfIds = []
	for id in tradeIds:
		if id in quoteIds and id in orderIDs:
			quote_orderSelfIds.append(id)
	print(len(quote_orderSelfIds), quote_orderSelfIds)