'''
CSCI-GA 2434 Advanced Database Systems Final Project
Replicated Concurrency Control and Recovery

Authors:
Ren Yi--ry708@nyu.edu
Yanqiu Wu--yw1370@nyu.edu
'''
import time, warnings
from .data_manager import DataManager
from .utils import *

class SiteManager(object):
	"""
	Data manager manage data objects at a particular site
	"""
	def __init__(self, num_sites=10, num_variables=20):
		# Each site has a lock table
		t = time.time() 
		self.sites = {s: DataManager(s, t, num_variables=num_variables) for s in range(1, num_sites + 1)}
		self.num_sites = num_sites
		self.num_variables = num_variables


	def dump(self, sites=None, indices=None):
		"""
		DM accesses committed value of data[indices] at sites
		print committed values in ascending order by variable name
		one line per site
		"""
		if sites is None:
			sites = range(1, self.num_sites + 1)
		# if indices is None:
		# 	indices = range(1, self.num_variables + 1)
		for s in sites:
			if self.sites[s].status == "UP":
				if indices is None:
					# indices = self.sites[s].get_keys()
					indices = list(range(1, self.num_variables + 1))
				stdout = ["site " + str(s)]
				tmp = self.sites[s].dump(indices)
				if tmp != "":
					stdout.append(tmp)
					print(" ".join(stdout))


	def update_value(self, index, value, time):
		# WHat if all sites are down, can we still update values???
		sites = get_variable_sites(index)
		for s in sites:
			if self.sites[s].status == "UP":
				self.sites[s].update_value(index, value, time)


	def get_latest_value(self, index):
		"""
		For read-only transactions
		Given data item index, return value with latest commit time across all up sites
		If all sites down, return None
		"""
		latest_time = -float('Inf')
		latest_val = None
		sites = get_variable_sites(index)

		for s in sites:
			if self.sites[s].status == "UP":
				v, t, p = self.sites[s].get_value_time(index)

				if p and t > latest_time:
					latest_time = t
					latest_val = v

		return latest_val


	def acquire_locks(self, tid, lock_type, index):
		"""
		1. W W
		2. R R
		3. W R
		4. R W
		5. Lock free
		Consider RW transaction and R-only transaction separately for each of these senarios
		What if all sites down??
		acquired: whether there's more than 1 UP site to acquire lock
		(<up>, <acquired>, <curr_txn>, <read_lock_site>)
		"""
		sites = get_variable_sites(index)
		# import ipdb; ipdb.set_trace()
		up, locked, curr_txn, curr_lt = self.__check_locks(sites, index, tid, lock_type)

		# if all sites are down, acquire false, no conflicting transactions
		if not up:
			return (False, False, [], [])

		# acquire locks
		# variable is lock free
		acquired, lock_sites = self.__add_locks(sites, tid, lock_type, index)

		if acquired:
			curr_txn = []

		return (up, acquired, curr_txn, lock_sites)
					

	def __add_locks(self, sites, tid, lock_type, index):
		"""
		Helper function for self.acquire_locks(), assuming there's at least 1 up site
		if lock_type == "WRITE", write to all up sites 
		if lock_type == "READ", write to one up site and return the site id in <read_site>
		"""
		acquired = False
		lock_sites = []
		for s in sites:
			if self.sites[s].status == "UP":
				tmp = self.sites[s].add_lock(index, tid, lock_type)
				if tmp:
					lock_sites.append(s)
					acquired = True
					if lock_type == "READ":
						break	
				else:
					if lock_type == "WRITE":
						acquired = False
						break

		return (acquired, lock_sites)


	def __check_locks(self, sites, index, tid, lock_type):
		"""
		Given a data index, check whether it has a lock, if so, what type
		"""
		up = False
		locked = False
		transactions = []
		types = []

		for s in sites:
			if self.sites[s].status == "UP":
				up = True
				L, T, t = self.sites[s].check_lock(index, tid, lock_type)
				locked = locked or L
				if T is not None and T not in transactions:
					transactions.append(T)
				if t is not None and t not in types:
					types.append(t)

		return (up, locked, transactions, types)

	
	def release_locks(self, transaction, indices):
		for index in indices:
			sites = get_variable_sites(index)
			for s in sites:
				if self.sites[s].status == "UP":
					self.sites[s].release_lock(transaction, index)


	def recover(self, site):
		# Site Recovery
		if self.sites[site].status == 'DOWN':
			self.sites[site].status = 'UP'
		else:
			warnings.warn("Attempting to recover an UP site %i" %site)

		self.sites[site].remove_read_permission()

		DataManager(site, t, recover=True, num_variables=self.num_variables)
		

	def fail(self, site):
		# Site fail 
		self.sites[site].status = "DOWN"


	def get_value(self,siteid,dataid):
		data_manager = self.sites[siteid]
		data = data_manager.data[dataid]
		val = data.commit_value

		return val

	def get_site_keys(self, site):
		"""
		Get a list of data id on this site
		"""
		return self.sites[site].get_keys()

	def get_variables(self):
		return list(range(1, self.num_variables + 1))







