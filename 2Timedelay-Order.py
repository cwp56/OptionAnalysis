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
	
	# All internal_order_id
	client = InfluxDBClient('localhost', 8086, 'root', 'root', 'test')
	queryStr = "select internal_order_id from shfe_option_order where note='OS准备下单' and '%s' <= time and time < '%s' and time_marketdata_send != 0 and time_send != 0;" % (queryDate.strftime('%Y-%m-%dT%H:%M:%SZ'), (queryDate + timedelta(seconds=durationSecs)).strftime('%Y-%m-%dT%H:%M:%SZ'))
	result = client.query(queryStr)
	client.close()
	qs_ready_order = {}
	for p in result.get_points():
		internal_order_id = p['internal_order_id']
		if internal_order_id in qs_ready_order:
			raise Exception
		qs_ready_order[internal_order_id] = 0
		
	# Send time delay & Feed time delay
	client = InfluxDBClient('localhost', 8086, 'root', 'root', 'test')
	queryStr = "select internal_order_id, time_send - time_marketdata_send as time_send_delay, time_feed - time_send as time_feed_delay from shfe_option_order where note='OS_final_status' and '%s' <= time and time < '%s' and time_marketdata_send != 0 and time_send != 0 and time_feed != 0 and time_marketdata_send <= time_send and time_send <= time_feed;" % (queryDate.strftime('%Y-%m-%dT%H:%M:%SZ'), (queryDate + timedelta(seconds=durationSecs)).strftime('%Y-%m-%dT%H:%M:%SZ'))
	result = client.query(queryStr)
	client.close()
	
	order_send_delays = {}  # {internal_order_id: time_marketdata_send: time_send}
	order_feed_delays = {}  # {internal_order_id: time_send: min(time_feed)}
	for p in result.get_points():
		internal_order_id = p['internal_order_id']
		time_send_delay = p['time_send_delay']
		time_feed_delay = p['time_feed_delay']
		if internal_order_id not in qs_ready_order:
			continue
		if internal_order_id in order_send_delays:
			if time_send_delay != order_send_delays[internal_order_id]:
				raise Exception
		else:
			order_send_delays[internal_order_id] = time_send_delay
		if internal_order_id in order_feed_delays:
			if time_feed_delay < order_feed_delays[internal_order_id]:
				order_feed_delays[internal_order_id] = time_feed_delay
		else:
			order_feed_delays[internal_order_id] = time_feed_delay
	time_send_delays = np.array(list(order_send_delays.values())) * 1e-9
	time_feed_delays = np.array(list(order_feed_delays.values())) * 1e-9
	
	client = InfluxDBClient('localhost', 8086, 'root', 'root', 'test')
	queryStr = "select internal_order_id, time_delete-time_marketdata_delete as time_delay from shfe_option_order where note='OS准备撤单' and time >= '%s' AND time < '%s' and time_delete != 0 and time_marketdata_delete != 0 and time_marketdata_delete <= time_delete;" % (queryDate.strftime('%Y-%m-%dT%H:%M:%SZ'), (queryDate + timedelta(seconds=durationSecs)).strftime('%Y-%m-%dT%H:%M:%SZ'))
	result = client.query(queryStr)
	client.close()
	order_delay_times = {}  # {internal_order_id: time_delete_delay}
	for p in result.get_points():
		internal_order_id = p['internal_order_id']
		time_delay = p['time_delay']
		if internal_order_id in order_delay_times:
			raise Exception
		order_delay_times[internal_order_id] = time_delay
	time_delete_delays = np.array(list(order_delay_times.values())) * 1e-9
	
	# Mean and Percentile (Send, Feed, Delete)
	print(time_send_delays.mean(), np.percentile(time_send_delays, [0.5, 0.75, 0.99]))
	print(time_feed_delays.mean(), np.percentile(time_feed_delays, [0.5, 0.75, 0.99]))
	print(time_delete_delays.mean(), np.percentile(time_delete_delays, [0.5, 0.75, 0.99]))
	
	# Draw time delay
	DrawDelayTimes(time_send_delays, 'Send', color1)
	DrawDelayTimes(time_feed_delays, 'Feed', color2)
	DrawDelayTimes(time_delete_delays, 'Delete', color3)
	plt.xlabel('Percentile (%)')
	plt.ylabel('Delay time (s)')
	plt.legend()
	plt.title('Order')
	plt.tight_layout()
	plt.show()