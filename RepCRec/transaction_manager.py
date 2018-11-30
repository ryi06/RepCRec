'''
CSCI-GA 2434 Advanced Database Systems Final Project
Replicated Concurrency Control and Recovery

Authors:
Ren Yi--ry708@nyu.edu
Yanqiu Wu--yw1370@nyu.edu
'''
import time
from .transaction import Transaction
from collections import defaultdict

class TransactionManager(object):
	"""docstring for TransactionManager"""
	def __init__(self, site_manager):
		super(TransactionManager, self).__init__()
		self.transactions = dict()
		self.waiting_commands = set()
		self.wait_for_graph = defaultdict(list)
		self.site_manager = site_manager


	def begin(self, Tid, name):
		T = Transaction(Tid, name, "RW", "RUN")
		self.transactions[Tid] = T

	def begin_RO(self, Tid, name):
		T = Transaction(Tid, name, "RO", "RUN")
		self.transactions[Tid] = T

	def write(self, Tid, dataid, value):
		if Tid not in self.transactions:
			print('Transaction ' + str(Tid) + " not exist.")
			return
		# get T
		T = self.transactions[Tid]
		
		if T.status != "RUN":
			print ("")
			return
		assert T.type != "RO", 'Transaction is read_only, cannot write'

		# acquire locks
		site_flag, acquire_status, conflict_Ts, lock_sites = self.site_manager.acquire_locks(Tid,'WRITE',dataid)
		# if successfully acquired
		if acquire_status:
			print ('Transaction '+str(Tid) +" successfully acquire write lock on x"+str(dataid))
			# update uncommited values
			T.uncommitted_data[dataid] = value
		else: 
			if site_flag:
				# Some sites containing that dataid are up
				# update wait-for-graph
				# ct here is the index of each conflicting transaction
				for ct in conflict_Ts:
					self.wait_for_graph[ct].append(Tid)
			# change the transaction status to wait
			T.status = "WAIT"
			# add the command to queue
			command_tuple = ("WRITE",Tid,dataid,value)
			self.waiting_commands.add(command_tuple)


	def read_only(self, Tid, dataid):
		T = self.transactions[Tid]

		if T.status != "RUN":
			return
		# get the latest committed value of dataid
		val = self.site_manager.get_latest_value(dataid)
		# if all sites are down, no site to read
		if val is None:
			T.status = "WAIT"
			command_tuple = ("READ",Tid,dataid)
			self.waiting_commands.add(command_tuple)
			print ("All available sites are down. Transaction " + str(Tid) +" is waiting on(RO) data "+ str(dataid))
		else:
			T.read_values.append((dataid,val))
			T.read_data.add(dataid)


	def read(self, Tid, dataid):
		if Tid not in self.transactions:
			return
		T = self.transactions[Tid]
		if T.status != "RUN":
			return   
		# read_only transaction read                                      
		if T.type == 'RO':
			self.read_only(Tid,dataid)
		# regular read
		else:
			site_flag, acquire_status, conflict_Ts, lock_sites = self.site_manager.acquire_locks(Tid,'READ',dataid)
			# if successfully acquired
			if acquire_status:
				print ('Transaction '+str(Tid) +" successfully acquire read lock on x"+str(dataid))
				# T has previously write dataid
				if dataid in T.uncommitted_data:
					# read the value written
					val = T.uncommitted_data[dataid]
					T.read_values.append((dataid,val))
					T.read_data.add(dataid)
				# dataid was lock-free
				else:
					# get the value of dadaid at any available copies
					if len(lock_sites) != 1:
						print ("Somthing wrong with read, more than one lock.")
					read_site = lock_sites[0]
					# read value from that site
					val = self.site_manager.get_value(read_site, dataid)
					T.read_values.append((dataid,val))
					T.read_data.add(dataid)
			else:
				# update wait-for-graph
				# ct here is the index of each conflicting transaction
				for ct in conflict_Ts:
					self.wait_for_graph[ct].append(Tid)
				# change the transaction status to wait
				T.status = "WAIT"
				# add the command to queue
				command_tuple = ("READ",Tid,dataid)
				self.waiting_commands.add(command_tuple)
				

	def try_waiting_commands(self):
		waiting_commands = self.waiting_commands
		self.waiting_commands = set()
		for command_tuple in waiting_commands:
			if command_tuple[1] in self.transactions:
				if command_tuple[0] == "READ":
					self.read(command_tuple[1],command_tuple[2])
				else:
					self.write(command_tuple[1],command_tuple[2],command_tuple[3])
			else:
				print ("Transaction "+str(command_tuple[1]) +" is no longer active.")


	def __update_wait_for_graph(self, Tid):
		if Tid in self.wait_for_graph:
			 del self.wait_for_graph[Tid]

	def __update_transaction_status(self):
		wait_list = []
		for id_list in self.wait_for_graph.values():
			wait_list += id_list
		for tid in self.transactions:
			if tid not in wait_list:
				self.transactions[tid].status = "RUN"

	def __update_uncommitted_value(self, Tid):
		T = self.transactions[Tid]
		commit_time = time.time()
		for dataid in T.uncommitted_data:
			value = T.uncommitted_data[dataid]
			self.site_manager.update_value(dataid, value, commit_time)

	def __print_read_values(self,Tid):
		T = self.transactions[Tid]
		for (dataid, value) in T.read_values:
			print ("x"+str(dataid)+": "+str(value))

	def __abort(self, Tid):
		print ("Aborting transaction " + str(Tid))
		# remove locks related to Tid
		print ('Release locks given by transaction ' + str(Tid))
		T = self.transactions[Tid]
		read_ids = list(T.read_data)
		write_ids = list(T.uncommitted_data.keys())
		data_accessed = list(set(read_ids + write_ids))
		self.site_manager.release_locks(Tid, data_accessed)

		# Remove abort transaction from all transactions
		del self.transactions[Tid]

		# update self.wait_for_graph
		print ("Update wait-for-graph after abort")
		self.__update_wait_for_graph(Tid)

		# update remaining transaction status
		print ("Update remaining active transaction status after abort")
		self.__update_transaction_status()

		# retry waiting commands
		print ("Retry waiting commads after abort")
		self.try_waiting_commands()

		print ("Abort is done.")


	def __commit(self, Tid):
		print ("Committing transaction " + str(Tid))

		T = self.transactions[Tid]
		# If T is RW:
		# update uncommited values
		if T.type == "RW":
			self.__update_uncommitted_value(Tid)

		# print out read values
		self.__print_read_values(Tid)
		
		# remove locks related to Tid
		print ('Release locks given by transaction ' + str(Tid))
		T = self.transactions[Tid]
		read_ids = list(T.read_data)
		write_ids = list(T.uncommitted_data.keys())
		data_accessed = list(set(read_ids + write_ids))
		self.site_manager.release_locks(Tid, data_accessed)

		# Remove commit transaction from all transactions
		del self.transactions[Tid]

		# update self.wait_for_graph
		print ("Update wait-for-graph after commit")
		self.__update_wait_for_graph(Tid)

		# update remaining transaction status
		print ("Update remaining active transaction status after commit")
		self.__update_transaction_status()

		# retry waiting commands
		print ("Retry waiting commads after commit")
		self.try_waiting_commands()

		print ("Commit is done.")

		
	def __visit(self,tid,path,visited):
		if tid in visited:
			return False, path, visited
		visited.add(tid)
		path.add(tid)

		if tid in self.wait_for_graph:
			for neighbour in self.wait_for_graph[tid]:
				if neighbour in path or self.__visit(neighbour,path,visited)[0]:
					return True, path, visited

		path.remove(tid)
		return False, path, visited

	def __detect_deadlock(self):
		path = set()
		visited = set()

		for tid in self.wait_for_graph:
			flag, path, visited = self.__visit(tid, path, visited) 
			if flag == True:
				# There is cycle
				# abort the youngest transaction
				return True, path

		return False, path
				
	def clear_deadlocks(self):
		cycle, path = self.__detect_deadlock()
		while cycle == True:
			if path:
				print ("Found deadlock.")
				# abort the youngest transaction
				time = None 
				abortid = 0
				for tid in path:
					if time is None:
						time = self.transactions[tid].start_time
						abortid = tid
					if self.transactions[tid].start_time > time:
						time = self.transactions[tid].start_time
						abortid = tid
				# get the youngest tid
				self.__abort(abortid)

				cycle, path = self.__detect_deadlock()

			else:
				print ("Something wrong with cycle detection.")
				break

		#print ("No deadlocks found.")


	def dump(self, sites=None, indices=None):
		self.site_manager.dump(sites, indices)


	def end(self, Tid):
		if Tid in self.transactions:
			T = self.transactions[Tid]
			if T.status == "WAIT":
				print ("End: Transaction "+str(Tid)+" should abort.")
				self.__abort(Tid)
			else:
				print ("End: Transaction "+str(Tid)+" should commit.")
				self.__commit(Tid)
		else:
			print ('End: Transaction '+str(Tid)+" is no longer active.")


	def recover(self, siteid):
		self.site_manager.recover(siteid)


	def fail(self, siteid):
		self.site_manager.fail(siteid)


