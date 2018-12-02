'''
CSCI-GA 2434 Advanced Database Systems Final Project
Replicated Concurrency Control and Recovery

Authors:
Ren Yi--ry708@nyu.edu
Yanqiu Wu--yw1370@nyu.edu
'''
import time

class Transaction(object):
	def __init__(self, ID, name, transType, status):
		self.id = ID
		self.name = name
		self.type = transType
		self.start_time = time.time()
		self.status = status
		self.read_values = dict()
		self.value_copies = dict() # use for RO transactions
		self.write_lock_sites = dict() # use for RW transactions
		self.uncommitted_data = dict() # use for RW transactions

	def set_status(self, status):
		self.status = status

	def get_status(self):
		return self.status

	def get_type(self):
		return self.type


