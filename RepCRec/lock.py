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
		self.lock_txn = []
		self.lock_type = None


	def get_transaction(self):
		return self.lock_txn

	def clear(self):
		self.lock_txn = []
		self.lock_type = None


	# def check_lock(self, new_txn, new_type):
	# 	'''
	# 	if old lock is write:
	# 		there can be only one transaction locking the item
	# 		if new lock is write: new txn needs to be the same of old txn, otherwise cannot acquire lock
	# 		if new lock is read: new txn needs to be the same as old txn, otherwise cannot acquire lock

	# 	if old lock is read:
	# 		if new lock is read: can acquire lock
	# 		if new lock is write: 
	# 			if multiple read transactions already exist, cannot acquire lock
	# 			if only single transaction:
	# 				if the single transaciton is the same as new txn, acquire lock and update type, 
	# 				otherwise cannot acquire lock

	# 	'''
	# 	if self.lock_type == "WRITE":
	# 		assert len(self.lock_txn) == 1
	# 		if self.lock_txn[0] != new_txn:
	# 			return False
	# 		else:
	# 			return True
	# 	if self.lock_type == "READ":
	# 		if new_type == "READ":
	# 			return ([], [])




	def __add_lock_type(self, new_type):
		"""
		add or upgrade lock type
		"""
		if self.lock_type is None or self.lock_type == "READ":
			self.lock_type = new_type


	def __add_transaction(self, new_txn):
		"""
		Append unique transactions
		"""		
		#self.lock_txn.extend(new_txn if new_txn not in self.lock_txn)
		if new_txn not in self.lock_txn:
			self.lock_txn.append(new_txn)


	def add_lock_transaction(self, new_txn, new_type, check_lock=False):
		"""
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

		return acquired: if new lock can be acquired
		"""
		assert new_type in ["WRITE", "READ"]
		assert isinstance(new_txn, int)

		# Lock free
		if self.lock_type is None and self.lock_txn == []:
			if check_lock:
				# return (True, [], None)
				return (True, [])
			self.__add_transaction(new_txn)
			self.__add_lock_type(new_type)
			return
			# return True

		# Already have write lock
		if self.lock_type == "WRITE":
			assert len(self.lock_txn) == 1
			if self.lock_txn[0] != new_txn:
				if check_lock:
					# return (False, self.lock_txn, self.lock_type)
					return (False, self.lock_txn)
				return
				# return False
			else:
				if check_lock:
					# return (True, [], self.lock_type)
					return (True, [])
				return
				# return  True

		# Already have read lock
		else:
			if new_type == "READ":
				if check_lock:
					#return (True, [], self.lock_type)
					return (True, [])
				self.__add_transaction(new_txn)
				self.__add_lock_type(new_type)
				return
				# return True
			else: # new_type is write
				if len(self.lock_txn) > 1:
					if check_lock:
						# return (False, self.lock_txn, self.lock_type)
						# return (False, self.lock_txn)
						# self.lock_txn may contain new_txn so we should remove it if it does
						if new_txn in self.lock_txn:
							# can not directly remove from self.lock_txn
							con_txn = self.lock_txn
							con_txn.remove(new_txn)
							return (False, con_txn)
						else:
							return (False, self.lock_txn)
					return
					# return False
				else:
					if self.lock_txn[0] == new_txn:
						if check_lock:
							# return (True, [], self.lock_type)
							return (True, [])
						self.__add_lock_type(new_type)
						return
						# return True
					else:
						if check_lock:
							# return (False, self.lock_txn, self.lock_type)
							return (False, self.lock_txn)
						return
						# return False


	def reset(self, transaction):
		# if len(self.lock_txn) > 1:
		# 	self.lock_txn.remove(transaction)
		# else:
		# 	if self.lock_txn[0] == transaction:
		# 		self.lock_txn = []
		# 		self.lock_type = None
		if transaction in self.lock_txn:
			self.lock_txn.remove(transaction)
			if len(self.lock_txn) == 0:
				self.lock_type = None


	def get_status(self):
		return (self.__locked(),
				self.lock_txn,
				self.lock_type)


	def __locked(self):
		if self.lock_txn is None and self.lock_type is None:
			return False
		return True