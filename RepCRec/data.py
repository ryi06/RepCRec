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
	Object Data stores data item at a particular site
	
	Attrs: site: data item storage site
		   index: data item index
		   commit_value: committed value at this site
		   commit_time: time when commit_value is updated
		   read_ready: boolean indicating whether the data item is ready to for reading
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
		'''return committed value'''
		return self.commit_value


	def get_time(self):
		'''return commit time'''
		return self.commit_time


	def read_permission(self):
		'''return boolean whether data variable is ready for read'''
		return self.read_ready


	def update_value(self, value, time):
		"""Update commit_value when transaction commits"""
		self.commit_value  = value
		self.commit_time = time
		self.read_ready = True





