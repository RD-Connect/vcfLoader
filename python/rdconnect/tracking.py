import json
import requests
import hail as hl
from rdconnect import index


def update_data_last_index(host, port, num_shards, num_replicas, user, pwd, data_project, data_index): 
	if not index.index_exists(host, port, "data_tracking", user, pwd):
		print('[INFO]:   . Tracking index does not exists. Its creation proceeds.')
		data = "{\"settings\": { \"index\": { \"number_of_shards\": \"" + num_shards + "\", \"number_of_replicas\": \"" + num_replicas + "\" } }, \"mappings\":{ \"1.0.0\" :{ \"properties\": { \"platform\" : { \"type\" : \"text\", \"index\": \"true\" }, \"index\" : { \"type\" : \"text\" } } } } }"
		index.create_index(host, port, 'data_tracking', data, user, pwd)
	else:
		print('[INFO]:   . Tracking index does exists. No need to create it.')

	# Get last entry
	url = "http://{}:{}/data_tracking/1.0.0/_search".format(host, port)
	headers = {'Content-Type': 'application/json'}
	data = "{ \"query\": { \"term\": { \"platform\": \"" + data_project + "\" } } }"
	response = requests.get(url, data = data, headers = headers, auth = (user,pwd))
	cnt = json.loads(response.text)['hits']

	# If no last entry
	if response.status_code == 200 and cnt['total'] == 0:
		url = "http://{}:{}/data_tracking/1.0.0/".format(host, port)
		data = "{ \"platform\": \"" + data_project + "\", \"index\": \"" + data_index + "\" }"
		response = requests.post(url, data = data, headers = headers, auth = (user,pwd))
		if response.status_code != 201:
			raise Exception('Obtained status code "{}" when inserting content.'.format(response.status_code))
	# If last entry
	elif response.status_code == 200 and cnt['total'] == 1:
		url = "http://{}:{}/data_tracking/1.0.0/{}".format(host, port, cnt['hits'][0]['_id'])
		data = "{ \"platform\": \"" + data_project + "\", \"index\": \"" + data_index2 + "\" }"
		response = requests.post(url, data = data, headers = headers, auth = (user,pwd))
		if response.status_code != 200:
			raise Exception('Obtained status code "{}" when updating content.'.format(response.status_code))	
	# If strange situations happens
	elif response.status_code == 200 and cnt['total'] > 1:
		raise Exception('Duplicated entries in tracking index are not allowed! Clean the index to proceed.')
	else:
		raise Exception('Obtained status code "{}"'.format(response.status_code))

def update_samples_data_management(initial_vcf, data_url, data_token):
	#url = "https://platform.rd-connect.eu/datamanagement/api/statusbyexperiment/?experiment="
	headers = { 'accept': 'application/json', 'Content-Type': 'application/json', 'Authorization': 'Token ' + data_token }

	vcf = hl.split_multi_hts(hl.import_vcf(str(initial_vcf), array_elements_required = False, force_bgz = True, min_partitions = 2))
	full_samples = [y.get('s') for y in vcf.col.collect()]

	print('[INFO]:   . Experiments in loaded VCF: {}'.format(len(full_samples)))
	print('[INFO]:   . First and last sample: {} // {}'.format(full_samples[0], full_samples[len(full_samples) - 1]))
	print('[INFO]:   . Provided URL for data-management: {}'.format(data_url))
	print('[INFO]:   . Provided token for data-management: {}'.format(data_token))

	for sam in full_samples:
		response = requests.post(data_url + sam, headers = headers)
		if response.status_code != 200:
			print(response.status_code)
			print(response.text)
			raise Exception('[ERROR]   . Information for sample "{}" could not be updated.'.format(sam))
