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
					"number_of_replicas": {}, 
					"refresh_interval": "-1" 
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

