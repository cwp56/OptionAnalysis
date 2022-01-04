import matplotlib.pyplot as plt
import numpy as np
from influxdb import InfluxDBClient
from datetime import datetime, timedelta
import sys

color1 = '#4751b0'
color2 = '#fd8d3c'
color3 = '#60a4ec'

def DrawDelayTimes(array, label, color):
	xs = [1]
	for i in range(10, 100, 10):
		xs.append(i)
	xs.append(99)
	xs = np.array(xs)
	ys = np.percentile(array, xs / 100)
	plt.axhline(ys[5], c=color, ls='dotted')
	plt.axhline(ys[-1], c=color, ls='dotted')
	plt.plot(xs, ys, marker='o', label=label, c=color)

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
		
	queryDate = datetime(year, month, day)
	durationSecs = duration
	
	# quote send time delay
	client = InfluxDBClient('localhost', 8086, 'root', 'root', 'test')
	queryStr = "select internal_quote_id, time_marketdata_send, time_send from shfe_option_quote where note='QS准备下单' and '%s' <= time and time < '%s' and time_marketdata_send != 0 and time_send != 0 and time_marketdata_send <= time_send;" % (queryDate.strftime('%Y-%m-%dT%H:%M:%SZ'), (queryDate + timedelta(seconds=durationSecs)).strftime('%Y-%m-%dT%H:%M:%SZ'))
	result = client.query(queryStr)
	client.close()
	
	quote_send_delays = {}  # {internal_quote_id: time_marketdata_send: min(time_send)}
	for p in result.get_points():
		internal_quote_id = p['internal_quote_id']
		time_marketdata_send = p['time_marketdata_send']
		time_send = p['time_send']
		if internal_quote_id not in quote_send_delays:
			quote_send_delays[internal_quote_id] = {}
		if time_marketdata_send in quote_send_delays[internal_quote_id]:
			if time_send < quote_send_delays[internal_quote_id][time_marketdata_send]:
				quote_send_delays[internal_quote_id][time_marketdata_send] = time_send
		else:
			quote_send_delays[internal_quote_id][time_marketdata_send] = time_send
	send_delays = {}  # {internal_quote_id: time_send_delay}
	for id in quote_send_delays:
		if len(quote_send_delays[id]) != 1:
			print('Error', id)
		for markert_time in quote_send_delays[id]:
			send_delays[id] = quote_send_delays[id][markert_time] - markert_time
	send_delays = dict(sorted(send_delays.items(), key=lambda x: x[1], reverse=True))
	# Send time delay array
	time_send_delays = np.array(list(send_delays.values())) * 1e-9
	
	# quote feed time delay
	client = InfluxDBClient('localhost', 8086, 'root', 'root', 'test')
	queryStr = "select internal_quote_id, time_send, time_feed from shfe_option_quote where note='QS_final_status' and '%s' <= time and time < '%s' and time_send != 0 and time_feed != 0 and time_send <= time_feed;" % (queryDate.strftime('%Y-%m-%dT%H:%M:%SZ'), (queryDate + timedelta(seconds=durationSecs)).strftime('%Y-%m-%dT%H:%M:%SZ'))
	result = client.query(queryStr)
	client.close()
	
	quote_feed_delays = {}  # {internal_quote_id: time_send: min(time_feed)}
	for p in result.get_points():
		internal_quote_id = p['internal_quote_id']
		time_send = p['time_send']
		time_feed = p['time_feed']
		if internal_quote_id not in quote_feed_delays:
			quote_feed_delays[internal_quote_id] = {}
		if time_send in quote_feed_delays[internal_quote_id]:
			if time_feed < quote_feed_delays[internal_quote_id][time_send]:
				quote_feed_delays[internal_quote_id][time_send] = time_feed
		else:
			quote_feed_delays[internal_quote_id][time_send] = time_feed
	
	feed_delays = {} # {internal_quote_id: time_feed_delay}
	for id in quote_feed_delays:
		if len(quote_feed_delays[id]) != 1:
			print('Error', id)
		for send_time in quote_feed_delays[id]:
			feed_delays[id] = quote_feed_delays[id][send_time] - send_time
	feed_delays = dict(sorted(feed_delays.items(), key=lambda x: x[1], reverse=True))
	# Feed time delay array
	time_feed_delays = np.array(list(feed_delays.values())) * 1e-9
	
	# Mean and Percentile
	print(time_send_delays.mean(), np.percentile(time_send_delays, [0.5, 0.75, 0.99]))
	print(time_feed_delays.mean(), np.percentile(time_feed_delays, [0.5, 0.75, 0.99]))
	
	# Draw time delay
	DrawDelayTimes(time_send_delays, 'Send', color1)
	DrawDelayTimes(time_feed_delays, 'Feed', color3)
	plt.xlabel('Percentile (%)')
	plt.ylabel('Delay time (s)')
	plt.legend()
	plt.title('Quote')
	plt.tight_layout()
	plt.show()