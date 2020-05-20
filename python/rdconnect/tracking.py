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


def update_dm_index(initial_vcf, index_name, data_ip, data_url, data_token):
	#url = "https://platform.rd-connect.eu/datamanagement/api/statusbyexperiment/?experiment="
	uri = "/datamanagement/api/statusbyexperiment/?experiment="
	url = "https://" + data_ip + uri
	headers = { 'accept': 'application/json', 'Content-Type': 'application/json', 'Authorization': 'Token ' + data_token, "Host": data_url }
	data = "{\"dataset\":\"" + index_name + "\"}"
	
	vcf = hl.split_multi_hts(hl.import_vcf(str(initial_vcf), array_elements_required = False, force_bgz = True, min_partitions = 2))
	full_samples = [y.get('s') for y in vcf.col.collect()]

	print('[INFO]:   . Experiments in loaded VCF: {}'.format(len(full_samples)))
	print('[INFO]:   . First and last sample: {} // {}'.format(full_samples[0], full_samples[len(full_samples) - 1]))
	print('[INFO]:   . Provided IP for data-management: {}'.format(data_ip))
	print('[INFO]:   . Provided URL for data-management: {}'.format(data_url))
	print('[INFO]:   . Provided token for data-management: {}'.format(data_token))
	print('[INFO]:   . Provided update content: "{}"'.format(str(data)))
	print('[INFO]:   . Created query URL for data-management: {}'.format(url))

	for sam in full_samples:
		q_url = url + sam
		response = requests.post(q_url, data = data, headers = headers, verify = False)
		if response.status_code != 200:
			raise Exception('[ERROR]   . Information for sample "{}" could not be updated.\n{}'.format(sam, response.text))


def update_dm(initial_vcf, index_name, data_ip, data_url, data_token, field):
	if not field in ("genomicsdb", "hdfs", "es", "in_platform"):
		raise Exception("[ERROR]: (update_dm + {}) Invalid field to be updated in data data-management.".format(field))

	#url = "https://platform.rd-connect.eu/datamanagement/api/statusbyexperiment/?experiment="
	uri = "/datamanagement/api/statusbyexperiment/?experiment="
	url = "https://" + data_ip + uri
	headers = { 'accept': 'application/json', 'Content-Type': 'application/json', 'Authorization': 'Token ' + data_token, "Host": data_url }
	data = "{\"" + field + "\":\"pass\"}"

	vcf = hl.split_multi_hts(hl.import_vcf(str(initial_vcf), array_elements_required = False, force_bgz = True, min_partitions = 2))
	full_samples = [ y.get('s') for y in vcf.col.collect() ]

	print('[INFO]:   . Experiments in loaded VCF: {}'.format(len(full_samples)))
	print('[INFO]:   . First and last sample: {} // {}'.format(full_samples[0], full_samples[len(full_samples) - 1]))
	print('[INFO]:   . Provided IP for data-management: {}'.format(data_ip))
	print('[INFO]:   . Provided URL for data-management: {}'.format(data_url))
	print('[INFO]:   . Provided token for data-management: {}'.format(data_token))
	print('[INFO]:   . Provided update content: "{}"'.format(str(data)))
	print('[INFO]:   . Provided field to update in data-management: {}'.format(field))
	print('[INFO]:   . Created query URL for data-management: {}'.format(url))

	for sam in full_samples:
		q_url = url + sam
		response = requests.post(q_url, data = data, headers = headers, verify = False)
		if response.status_code != 200:
			raise Exception('[ERROR]   . Information for sample "{}" could not be updated using "{}".'.format(sam, q_url))

def update_dm_densematrix(initial_vcf, index_name, data_ip, data_url, data_token, value):
	#url = "https://platform.rd-connect.eu/datamanagement/api/statusbyexperiment/?experiment="
	uri = "/datamanagement/api/sparsematrix"
	url = "https://" + data_ip + uri
	headers = { 'accept': 'application/json', 'Content-Type': 'application/json', 'Authorization': 'Token ' + data_token, "Host": data_url }

	vcf = hl.split_multi_hts(hl.import_vcf(str(initial_vcf), array_elements_required = False, force_bgz = True, min_partitions = 2))
	full_samples = [ y.get('s') for y in vcf.col.collect() ]

	print('[INFO]:   . Experiments in loaded VCF: {}'.format(len(full_samples)))
	print('[INFO]:   . First and last sample: {} // {}'.format(full_samples[0], full_samples[len(full_samples) - 1]))
	print('[INFO]:   . Provided IP for data-management: {}'.format(data_ip))
	print('[INFO]:   . Provided URL for data-management: {}'.format(data_url))
	print('[INFO]:   . Provided token for data-management: {}'.format(data_token))
	print('[INFO]:   . Provided update content: "{}"'.format(str(data)))
	print('[INFO]:   . Provided value to update "sparsematrix" in data-management: {}'.format(value))
	print('[INFO]:   . Created query URL for data-management: {}'.format(url))

	for sam in full_samples:
		q_url = url + sam
		data = "{\"" + sam + "\":\"" + value + "\"}"
		response = requests.post(q_url, data = data, headers = headers, verify = False)
		if response.status_code != 200:
			raise Exception('[ERROR]   . Information for sample "{}" could not be updated using "{}".'.format(sam, q_url))
