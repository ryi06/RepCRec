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
	Data manager manage data objects at a particular site
	Attrs: site: DataManager site id
		   status: site status, choose between ["UP", "DOWN"]
		   data: dictionary containing Data objects
		   lock_table: dictionary containing locking info for each data item
		   			   in (<lock_transaction>, <lock_type>) tuple format
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
		return list(self.data.keys())

	
	def update_value(self, index, value, time):
		self.data[index].update_value(value, time)

	
	def dump(self, indices):
		"""
		DM accesses all data item whose index is in indices
		return the committed values in ascending order by variable name
		in one line in x1, 3 x2, 5 format 
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
		return (self.data[index].get_value(), 
				self.data[index].get_time(), 
				self.data[index].read_permission())

	
	# def check_lock(self, index, transaction, lock_type):
	# 	curr_locked, curr_txn, curr_type = self.lock_table[index].get_status()
	# 	# If this site is already locked by the same transaction, it is considered lock free
	# 	if (curr_locked) and transaction in curr_txn:
	# 		return (False, None, None)

	# 	return (curr_locked, curr_txn, curr_type)


	def add_lock(self, index, transaction, lock_type):
		'''
		# Acquire lock successful if 
			# lock_type == "READ" and data[index] is ready to be read
			# lock_type == "WRITE" then whatever
		# if read lock already exists and lock_type == "WRITE", change to lock_type
		# if write lock already exist and lock_type == "READ", do nothing
		'''
		acquired, curr_txn = self.lock_table[index].add_lock_transaction(transaction, lock_type, check_lock=True)
		# If read-only transaction, you can only acquire lock if READ is permitted
		if lock_type == 'READ' and not self.data[index].read_permission():
			acquired = False

		if acquired:
			self.self.lock_table[index].add_lock_transaction(transaction, lock_type)

		return (acquired, curr_txn)

		# curr_txn = self.lock_table[index].get_transaction()
		# if (curr_txn is not None) and (curr_txn != transaction):
		# 	return False
		# if (lock_type == "READ") and (not self.data[index].read_permission()):
		# 	return False

		# self.lock_table[index].add_transaction(transaction)
		# self.lock_table[index].add_lock_type(lock_type)
		# return True

	def release_lock(self, transaction, index):
		self.lock_table[index].reset(transaction)

	def remove_read_permission(self):
		for d in self.data.keys():
			if replicated_data(d):
				self.data[d].read_ready = False





		






