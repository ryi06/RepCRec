'''
CSCI-GA 2434 Advanced Database Systems Final Project
Replicated Concurrency Control and Recovery

Team:
Ren Yi--ry708@nyu.edu
Yanqiu Wu--yw1370@nyu.edu

Author: Yanqiu Wu
'''
import time

class Transaction(object):
	"""
	Transaction stores transaction item 

	Attrs: id: transaction index
		   name: transaction name
		   type: transaction type, read-write(RW) or read-only(RO)
		   start-time: begin time of the transactoin
		   status: RUN or WAIT
		   read_values: a dictionary stores data itens read by the transaction and their values
		   value_copies: a copy of the latest committed values of all data items when RO transaction begins 
		   write_lock_sites: a dictionary stores the sites where transaction writes for each data item
		   uncommited_data: a dictionary stores the value written by transaction for each data item
	"""
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
		'''set transaction status'''
		self.status = status

	def get_status(self):
		'''return transaction status'''
		return self.status

	def get_type(self):
		'''return transaction type'''
		return self.type


