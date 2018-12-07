'''
CSCI-GA 2434 Advanced Database Systems Final Project
Replicated Concurrency Control and Recovery

Team:
Ren Yi--ry708@nyu.edu
Yanqiu Wu--yw1370@nyu.edu

Author: Ren Yi
'''

class Lock(object):
	"""
	Object Lock stores data items locking info at a particular site
	
	Attrs: index: data item index
		   site: site id
		   lock_txn: list of transactions currently holding locks on the data
		   lock_type: Existing lock type
	"""
	def __init__(self, index, site):
		self.index = index
		self.site = site
		self.lock_txn = []
		self.lock_type = None


	def get_transaction(self):
		"""Get the list of existing lock transactions"""
		return self.lock_txn


	def __add_lock_type(self, new_type):
		"""add or upgrade lock type"""
		if self.lock_type is None or self.lock_type == "READ":
			self.lock_type = new_type


	def __add_transaction(self, new_txn):
		"""Append unique transactions"""		
		if new_txn not in self.lock_txn:
			self.lock_txn.append(new_txn)


	def add_lock_transaction(self, new_txn, new_type, check_lock=False):
		"""
		If lock free:
			Acquire lock successful
		if old lock is write:
			there can be only one transaction locking the item
			if new lock is write: new txn needs to be the same of old txn, otherwise cannot acquire lock
			if new lock is read: new txn needs to be the same as old txn, otherwise cannot acquire lock

		if old lock is read:
			if new lock is read: can acquire lock
			if new lock is write: 
				if multiple read transactions already exist, cannot acquire lock
				if only single transaction:
					if the single transaciton is the same as new txn, acquire lock and update type, 
					otherwise cannot acquire lock
		OUTPUT:
			if check_lock is True, return (acquired, conflict_txn):
				acquired: if locks can be acquired
				conflict_txn: a list of conflicting transactions
			else:
				add locks and return None
		"""
		assert new_type in ["WRITE", "READ"]
		assert isinstance(new_txn, int)

		# Lock free
		if self.lock_type is None and self.lock_txn == []:
			if check_lock:
				return (True, [])
			self.__add_transaction(new_txn)
			self.__add_lock_type(new_type)
			return

		# Already have write lock
		if self.lock_type == "WRITE":
			assert len(self.lock_txn) == 1
			if self.lock_txn[0] != new_txn:
				if check_lock:
					return (False, self.lock_txn)
				return
			else:
				if check_lock:
					return (True, [])
				return

		# Already have read lock
		else:
			if new_type == "READ":
				if check_lock:
					return (True, [])
				self.__add_transaction(new_txn)
				self.__add_lock_type(new_type)
				return
			else: # new_type is write
				if len(self.lock_txn) > 1:
					if check_lock:
						# self.lock_txn may contain new_txn so we should remove it if it does
						if new_txn in self.lock_txn:
							# cannot directly remove from self.lock_txn
							con_txn = self.lock_txn
							con_txn.remove(new_txn)
							return (False, con_txn)
						else:
							return (False, self.lock_txn)
					return
				else:
					if self.lock_txn[0] == new_txn:
						if check_lock:
							return (True, [])
						self.__add_lock_type(new_type)
						return
					else:
						if check_lock:
							return (False, self.lock_txn)
						return


	def clear(self, transaction):
		"""Clear locks after transaction has committed"""
		if transaction in self.lock_txn:
			self.lock_txn.remove(transaction)
			if len(self.lock_txn) == 0:
				self.lock_type = None

	def reset(self):
		"""Reset lock status after site fails"""
		self.lock_txn = []
		self.lock_type = None

