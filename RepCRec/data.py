'''
CSCI-GA 2434 Advanced Database Systems Final Project
Replicated Concurrency Control and Recovery

Authors:
Ren Yi--ry708@nyu.edu
Yanqiu Wu--yw1370@nyu.edu
'''
from .utils import *

class Data(object):
	"""
	Object Data store data item at a particular site
	
	Attrs: site: data item storage site
		   index: data item index
		   commit_value: committed value at this site
		   commit_time: time when commit_value is updated
		   read_permission: boolean indicating whether the data item is ready to for reading
	"""
	def __init__(self, index, site, time):

		self.index = index
		self.site = site
		self.name = id2name(index)
		self.commit_value = 10 * index
		self.commit_time = time
		self.read_ready = True
		# self.uncommit_value = None


	def get_value(self):
		return self.commit_value


	def get_time(self):
		return self.commit_time


	def read_permission(self):
		return self.read_ready


	def update_value(self, value, time):
		"""If data item is lock free, active transaction writes to uncommit_value"""
		self.commit_value  = value
		self.commit_time = time
		self.read_ready = True




	# def update_commit_transaction(self, transaction):
	# 	"""Upate commit time and value for committed transaction"""
	# 	assert self.lock == transaction.ID
		
	# 	self.lock = None
	# 	self.commit_time = time.time
	# 	self.commit_value = self.uncommit_value
	# 	self.uncommit_value = None
		
	# 	return





