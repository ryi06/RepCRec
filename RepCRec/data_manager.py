'''
CSCI-GA 2434 Advanced Database Systems Final Project
Replicated Concurrency Control and Recovery

Authors:
Ren Yi--ry708@nyu.edu
Yanqiu Wu--yw1370@nyu.edu
'''

from .data import Data
from .lock import Lock
from .utils import *

class DataManager(object):
	"""
	Data manager manages data objects at a particular site
	Attrs: site: DataManager site id
		   status: site status, choose between ["UP", "DOWN"]
		   data: dictionary containing Data objects stored at this site
		   lock_table: dictionary containing locking info for each data item
		   			   stored in Lock object
	"""
	def __init__(self, site, time, num_variables=20):
		self.site = site
		self.status = "UP"

		# Initialize data objects and lock_table
		self.lock_table = {}
		self.data = {}
		for i in range(1, num_variables + 1):
			if check_site(i, site):
				self.data[i] = Data(i, site, time)
				self.lock_table[i] = Lock(i, site)


	def get_keys(self):
		"""Get a list of data item indices stored on this site"""
		return list(self.data.keys())

	
	def get_value(self, index):
		"""Get the commit value of data[index] at this site"""
		return self.data[index].get_value()

	
	def update_value(self, index, value, time):
		"""
		Update commit value and commit time of data[index] at this site
		"""
		self.data[index].update_value(value, time)

	
	def dump(self, indices):
		"""
		At this site, get all data items whose index is in indices
		return the committed values in ascending order by variable name
		in one line in x1, 3, x2, 5 format 
		"""
		stdout = []
		for index in indices:
			if check_site(index, self.site):
				name = id2name(index)
				value = self.data[index].get_value()
				tmp = ": ".join([name, str(value) + ","])
				stdout.append(tmp)
		return " ".join(stdout)

	
	def get_value_time(self, index):
		"""
		Get data[index] commit value, time and read_permission
		"""
		return (self.data[index].get_value(), 
				self.data[index].get_time(), 
				self.data[index].read_permission())


	def add_lock(self, index, transaction, lock_type):
		"""
		Acquire lock of lock_type on data[index] for transaction
		Lock cannot be acquired if lock_type == "READ" but data[index] is not 
		ready for read
		"""
		acquired, curr_txn = self.lock_table[index].add_lock_transaction(transaction, lock_type, check_lock=True)
		# If read-only transaction, you can only acquire lock if READ is permitted
		if lock_type == 'READ' and not self.data[index].read_permission():
			acquired = False

		if acquired:
			self.lock_table[index].add_lock_transaction(transaction, lock_type)

		return (acquired, curr_txn)


	def release_lock(self, transaction, index):
		"""Release locks on data[index] held by transaction"""
		self.lock_table[index].clear(transaction)


	def remove_read_permission(self):
		"""WHen a site recovers, replicated data are not ready for read"""
		for index in self.data.keys():
			if replicated_data(index):
				self.data[index].read_ready = False


	def reset_locks(self):
		"""Reset to lock free status after a site fails"""
		for index in self.lock_table.keys():
			self.lock_table[index].reset()






		






