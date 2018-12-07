'''
CSCI-GA 2434 Advanced Database Systems Final Project
Replicated Concurrency Control and Recovery

Team:
Ren Yi--ry708@nyu.edu
Yanqiu Wu--yw1370@nyu.edu

Author: Ren Yi
'''
import time, warnings
from .data_manager import DataManager
from .utils import *

class SiteManager(object):
	"""
	Site manager manages data managers across all sites. 
	It responds to instructions from the transaction manager.

	Attrs:
		sites: a dictionary containing all sites and their corresponding DM
		num_sites: number of sites in the database
		num_variables: number of variables in the database
	"""
	def __init__(self, num_sites=10, num_variables=20):
		t = time.time() 
		self.sites = {s: DataManager(s, t, num_variables=num_variables) for s in range(1, num_sites + 1)}
		self.num_sites = num_sites
		self.num_variables = num_variables


	def dump(self, sites=None, indices=None):
		"""
		Display committed values for data variables across sites
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


	def update_value(self, index, value, time, sites):
		"""
		Update commited value and time after a transaction commits
		"""
		for s in sites:
			if self.sites[s].status == "UP":
				self.sites[s].update_value(index, value, time)
			else:
				print ("Wrong, this transaction should abort.")


	def get_latest_value(self, index):
		"""
		For read-only transactions
		Given a data item index, return commit value and time.with  
		latest commit time across all up sites
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

		return (latest_val, latest_time)


	def acquire_locks(self, tid, lock_type, index):
		"""
		acquire locks on data[index] for transaction
		OUTPUT:
			up: boolean indicating whether there exists at least one UP ste for data[index]
			acquired: boolean indicating whether lock(s) has been acquired
			conflict_txn: list of existing transactions that are holding locks on this data item
			lock_sites: list of sites where lock(s) has been acquired
		"""
		sites = get_variable_sites(index)
		# import ipdb; ipdb.set_trace()
		# up, locked, curr_txn, curr_lt = self.__check_locks(sites, index, tid, lock_type)
		up, acquired, conflict_txn, lock_sites = self.__add_locks(sites, tid, lock_type, index)
		# if all sites are down, acquire False, no conflicting transactions
		if not up:
			return (False, False, [], [])

		return (up, acquired, conflict_txn, lock_sites)
					

	def __add_locks(self, sites, tid, lock_type, index):
		"""Helper function for self.acquire_locks()"""
		up = False
		#acquired = False
		acquired = []
		conflict_txn = []
		lock_sites = []

		for s in sites:
			if self.sites[s].status == "UP":
				up = True
				tmp_acq, tmp_txn = self.sites[s].add_lock(index, tid, lock_type)
				conflict_txn.extend(x for x in tmp_txn if x not in conflict_txn)
				if tmp_acq: 
					lock_sites.append(s)
					acquired.append(True)
					if lock_type == "READ":
						break	
				else:
					acquired.append(False)
					#if lock_type == "WRITE":
						#acquired = False
						#break
		if lock_type == "READ":
			lock_acquisition = any(acquired)
		else:
			lock_acquisition = all(acquired)

		return (up, lock_acquisition, conflict_txn, lock_sites)

	
	def release_locks(self, transaction, indices):
		'''
		Release locks on data item[indices] when transaction commits
		'''
		for index in indices:
			sites = get_variable_sites(index)
			for s in sites:
				if self.sites[s].status == "UP":
					self.sites[s].release_lock(transaction, index)


	def recover(self, site):
		'''
		Recover site and update read permission for replicated data items
		'''
		if self.sites[site].status == 'DOWN':
			self.sites[site].status = 'UP'
		else:
			warnings.warn("Attempting to recover an UP site %i" %site)

		self.sites[site].remove_read_permission()

	def fail(self, site):
		'''Fail site and free up all locks held on this site'''
		self.sites[site].status = "DOWN"
		self.sites[site].reset_locks()


	def get_value(self,site,index):
		'''return commit value for data[index] at site'''
		return self.sites[site].get_value(index)


	def get_site_keys(self, site):
		"""Get a list of data id on this site"""
		return self.sites[site].get_keys()


	def get_variables(self):
		'''Get a list of all variables in the database'''
		return list(range(1, self.num_variables + 1))







