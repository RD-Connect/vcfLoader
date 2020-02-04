import json
import requests
from rdconnect import index

def update_data_last_index(host, port, num_shards, num_replicas, user, pwd, data_project, data_index): 
	if not index.index_exists():
		print('[INFO]: Tracking index does not exists. Its creation proceeds.')
		data="""{
			"settings": { 
				"index": { 
					"number_of_shards": {}, 
					"number_of_replicas": {}
				} 
			}, 
			"mappings":{ "1.0.0" :{
				"properties": {
					"platform" : { "type" : "text", "index": "true" },
					"index" : { "type" : "text" }
				}
			}}}""".format(num_shards, num_replicas)
		index.create_index(host, port, 'data_tracking', data, user, pwd)
	else:
		print('[INFO]: Tracking index does exists. No need to create it.')

	# Get last entry
	url = "http://{}:{}/data_tracking/1.0.0/_search".format(host, port)
	headers = {'Content-Type': 'application/json'}
	data = "{ \"query\": { \"term\": { \"platform\": \"" + data_project + "\" } } }"
	response = requests.get(url, data = data, headers = headers) #, auth = (user,pwd))
	cnt = json.loads(response.text)['hits']

	# If no last entry
	if response.status_code == 200 and cnt['total'] == 0:
		url = "http://{}:{}/data_tracking/1.0.0/".format(host, port)
		data = "{ \"platform\": \"" + data_project + "\", \"index\": \"" + data_index + "\" }"
		response = requests.post(url, data = data, headers = headers) #, auth = (user,pwd))
		if response.status_code != 201:
			raise Exception('Obtained status code "{}" when inserting content.'.format(response.status_code))	
		# If last entry
	elif response.status_code == 200 and cnt['total'] == 1:
		url = "http://{}:{}/data_tracking/1.0.0/{}".format(host, port, cnt['hits'][0]['_id'])
		data = "{ \"platform\": \"" + data_project + "\", \"index\": \"" + data_index + "\" }"
		response = requests.post(url, data = data, headers = headers) #, auth = (user,pwd))
		if response.status_code != 200:
			raise Exception('Obtained status code "{}" when updating content.'.format(response.status_code))	
	# If strange situations happens
	elif response.status_code == 200 and cnt['total'] > 1:
		raise Exception('Duplicated entries in tracking index are not allowed! Clean the index to proceed.')
	else:
		raise Exception('Obtained status code "{}"'.format(response.status_code))

