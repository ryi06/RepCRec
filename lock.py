'''
CSCI-GA 2434 Advanced Database Systems Final Project
Replicated Concurrency Control and Recovery

Authors:
Ren Yi--ry708@nyu.edu
Yanqiu Wu--yw1370@nyu.edu
'''

class Lock(object):
	def __init__(self, index, site):
		self.index = index
		self.site = site
		self.lock_txn = None
		self.lock_type = None


	def get_transaction(self):
		return self.lock_txn

	def add_lock_type(self, new_type):
		assert new_type in ["WRITE", "READ"]
		if self.lock_type is None or self.lock_type == "READ":
			self.lock_type = new_type


	def add_transaction(self, new_txn):
		assert isinstance(new_txn, int)
		if self.lock_txn is None:
			self.lock_txn = new_txn
		else:
			assert self.lock_txn == new_txn


	def reset(self, transaction):
		assert transaction == self.lock_txn
		self.lock_txn = None
		self.lock_type = None


	def get_status(self):
		return (self.__locked(),
				self.lock_txn,
				self.lock_type)


	def __locked(self):
		if self.lock_txn is None and self.lock_type is None:
			return False
		return True