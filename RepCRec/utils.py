'''
CSCI-GA 2434 Advanced Database Systems Final Project
Replicated Concurrency Control and Recovery

Team:
Ren Yi--ry708@nyu.edu
Yanqiu Wu--yw1370@nyu.edu

Author: Ren Yi
'''

def get_variable_sites(index):
	"""Get a list of sites where data[index] are stored"""
	num_sites = 10
	if index % 2 == 1:
		return [1 + index % num_sites]
	else:
		return range(1, num_sites + 1)

def replicated_data(index):
	"""Whether data[index] is a replicated data item"""
	return index % 2 == 0

def check_site(index, site):
	"""Check if a particular variable is at site"""
	num_sites = 10
	return (index % 2 == 0) or ((1 + index % num_sites) == site)

def id2name(index):
	"""Given index/name, return name string"""
	if isinstance(index, int):
		s = "x" + str(index)
	return s

def name2id(name):
	"""Given index/name, return int index"""
	if isinstance(name, str):
		if 'x' in name:
			s = int(name.replace('x', ''))
		elif 'T' in name:
			s = int(name.replace('T', ''))
	return s
