'''
CSCI-GA 2434 Advanced Database Systems Final Project
Replicated Concurrency Control and Recovery

Authors:
Ren Yi--ry708@nyu.edu
Yanqiu Wu--yw1370@nyu.edu
'''
# from enum import Enum

def get_variable_sites(index):
	"""
	Input: variable index
	Output: list of sites this variable resides
	"""
	num_sites = 10
	if index % 2 == 1:
		return [1 + index % num_sites]
	else:
		return range(1, num_sites + 1)

def replicated_data(index):
	return index % 2 == 0

def check_site(index, site):
	"""
	Check is a particular variable is at site
	"""
	num_sites = 10
	return (index % 2 == 0) or ((1 + index % num_sites) == site)

def id2name(index):
	if isinstance(index, int):
		s = "x" + str(index)
	return s

def name2id(name):
	if isinstance(name, str):
		if 'x' in name:
			s = int(name.replace('x', ''))
		elif 'T' in name:
			s = int(name.replace('T', ''))
	return s


# class LockStatus(Enum):
# 	ACQUIRED = 1
# 	NOSITE = 2
# 	FAILED = 3

