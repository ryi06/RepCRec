'''
CSCI-GA 2434 Advanced Database Systems Final Project
Replicated Concurrency Control and Recovery

Team:
Ren Yi--ry708@nyu.edu
Yanqiu Wu--yw1370@nyu.edu

Author: Yanqiu Wu
'''
import time
from .transaction import Transaction
from collections import defaultdict

class TransactionManager(object):
	"""
		Transaction manager manages all transactions,
		is in charge of transaction commands, including
		begin(), beginRO(), read(), write(), end(), dump(), etc.
		Transaction manager also detects deadlocks
	"""
	def __init__(self, site_manager):
		super(TransactionManager, self).__init__()
		self.transactions = dict()
		self.waiting_commands = list()
		self.wait_for_graph = defaultdict(list)
		self.site_manager = site_manager


	def begin(self, Tid, name):
		"""
			initialize a read-write transaction
			Tid: transaction index
			name: transaction name
			and store it in the transaction manager
		"""
		T = Transaction(Tid, name, "RW", "RUN")
		self.transactions[Tid] = T

	def begin_RO(self, Tid, name):
		"""
			initialize a read-only transaction
			Tid: transaction index
			name: transaction name
			store it in the transaction manager
			and get a copy of latest committed values of all data items
		"""
		T = Transaction(Tid, name, "RO", "RUN")
		self.transactions[Tid] = T

		for dataid in self.site_manager.get_variables():
			val, time = self.site_manager.get_latest_value(dataid)
			T.value_copies[dataid] = val

	def write(self, Tid, dataid, value):
		"""
			Tid: transaction index
			dataid: data index
			value: the value transaction tries to write
			transaction manager will communicate with site manager 
			and get lock acquisition info ofTid on dataid
			if successfully acquire lock, store written value in transaction
			otherwise, transaction waits and add the command into waiting commands.
		"""
		if Tid not in self.transactions:
			#print('T' + str(Tid) + " not exist.")
			return
		# get T
		T = self.transactions[Tid]
		
		if T.get_status() != "RUN":
			# add the command to list
			command_tuple = ("WRITE",Tid,dataid,value)
			self.waiting_commands.append(command_tuple)
			return
		assert T.get_type() != "RO", 'Transaction is read_only, cannot write'

		# acquire locks
		site_flag, acquire_status, conflict_Ts, lock_sites = self.site_manager.acquire_locks(Tid,'WRITE',dataid)
		# if successfully acquired
		if acquire_status:
			print ('T'+str(Tid) +" successfully acquires write lock(s) on x"+str(dataid))
			# update uncommited values
			T.uncommitted_data[dataid] = value
			T.write_lock_sites[dataid] = lock_sites
		else: 
			# All sites containing that dataid are down
			if not site_flag:
				print ("All sites are down for x"+str(dataid))
				print ('T'+str(Tid) +" is waiting to write on x"+str(dataid))
			else:
				print ('T'+str(Tid) +" is waiting for write lock(s) on x"+str(dataid))
				# update wait-for-graph
				# ct here is the index of each conflicting transaction
				for ct in conflict_Ts:
					self.wait_for_graph[ct].append(Tid)
				# change the transaction status to wait
				T.set_status("WAIT")
			# add the command to list
			command_tuple = ("WRITE",Tid,dataid,value)
			self.waiting_commands.append(command_tuple)

	def __replicate(self,dataid):
		"""
			decide whether data item is replicated data or not
		"""
		if dataid % 2 == 0:
			return True	
		else:
			return False

	def __read_only(self, Tid, dataid):
		"""
			read the latest committed value (before Tid begins) of dataid
			if the all sites are fail for dataid before Tid begins
			Tid waits until site recovers
		"""
		T = self.transactions[Tid]

		# get the latest committed value of dataid from value copies
		# if all sites are down, no site to read
		val = T.value_copies[dataid]
		if val is None:
			# if x is unreplicated data:
			if not self.__replicate(dataid):
				val, time = self.site_manager.get_latest_value(dataid)
			else:
				print ('All sites are down for replicated data x'+str(dataid))
				self.__abort(Tid)
			if val is None:
				command_tuple = ("READ",Tid,dataid)
				self.waiting_commands.append(command_tuple)
				print ("All sites are down for x"+str(dataid))
				print ("T" + str(Tid) +" is waiting on(RO) x"+ str(dataid))
			else:
				if time < T.start_time:
					T.set_status("RUN")
					T.read_values[dataid] = val
					print ("R(T" + str(Tid) +", x"+ str(dataid) + "):")
					print ("x"+str(dataid)+": "+str(val))
				
		else:
			T.set_status("RUN")
			T.read_values[dataid] = val
			print ("R(T" + str(Tid) +", x"+ str(dataid) + "):")
			print ("x"+str(dataid)+": "+str(val))


	def read(self, Tid, dataid):
		"""
			If transaction is read_only, call read_only function
			otherwise, transaction manager will comminicate with site manager to receive read lock acquisition info
			of Tid on dataid
			if successfully aquired, we print out the read value
			otherwise, Tid waits and command gets added to waiting command queue
		"""
		if Tid not in self.transactions:
			return
		T = self.transactions[Tid]
		if T.get_status() != "RUN":
			# add the command to list
			command_tuple = ("READ",Tid,dataid)
			self.waiting_commands.append(command_tuple)
			return   
		# read_only transaction read                                      
		if T.get_type() == 'RO':
			self.__read_only(Tid,dataid)
		# regular read
		else:
			site_flag, acquire_status, conflict_Ts, lock_sites = self.site_manager.acquire_locks(Tid,'READ',dataid)
			# if successfully acquired
			if acquire_status:
				print ('T'+str(Tid) +" successfully acquires read lock on x"+str(dataid))
				# T has previously write dataid
				if dataid in T.uncommitted_data:
					# read the value written
					val = T.uncommitted_data[dataid]
					T.read_values[dataid] = val
				# dataid was lock-free
				else:
					# get the value of dadaid at any available copies
					if len(lock_sites) != 1:
						print ("Somthing wrong with read, more than one lock.")
					read_site = lock_sites[0]
					# read value from that site
					val = self.site_manager.get_value(read_site, dataid)
					T.read_values[dataid] = val
				print ("R(T" + str(Tid) +", x"+ str(dataid) + "):")
				print ("x"+str(dataid)+": "+str(val))
					
			else:
				# update wait-for-graph
				# ct here is the index of each conflicting transaction
				if not site_flag:
					# all sites are down
					print ("All sites are down for x"+str(dataid))
					print ("T"+str(Tid)+" is waiting to read x"+str(dataid))
				else:
					if len(conflict_Ts) == 0:
						print ('T' + str(Tid) + " is waiting for read permission on x"+str(dataid)+" after site recovery.")
					else:
						print ('T' + str(Tid) + " is waiting for read lock on x"+str(dataid))
						for ct in conflict_Ts:
							self.wait_for_graph[ct].append(Tid)
					# change the transaction status to wait
					T.set_status("WAIT")
				# add the command to list
				command_tuple = ("READ",Tid,dataid)
				self.waiting_commands.append(command_tuple)
				

	def __try_waiting_commands(self):
		"""
			recall command in the watiitng commad queue
		"""
		waiting_commands = self.waiting_commands
		self.waiting_commands = list()
		for command_tuple in waiting_commands:
			if command_tuple[1] in self.transactions:
				if command_tuple[0] == "READ":
					self.read(command_tuple[1],command_tuple[2])
				else:
					self.write(command_tuple[1],command_tuple[2],command_tuple[3])


	def __update_wait_for_graph(self, Tid):
		"""
			update wait for graph after commit or abort Tid
			we remove the edges where other transactions are waiting for Tid
		"""
		if Tid in self.wait_for_graph:
			 del self.wait_for_graph[Tid]

	def __update_transaction_status(self):
		"""
			update transaction_status, usually 
			happen after we update wait_for_graph
			if the transaction is no longer in the 
			wait-for-graph, we change it status to "RUN"
		"""
		wait_list = []
		for id_list in self.wait_for_graph.values():
			wait_list += id_list
		for tid in self.transactions:
			if tid not in wait_list:
				self.transactions[tid].set_status("RUN")

	def __update_uncommitted_value(self, Tid):
		"""
			When transaction Tid commits, transaction manager 
			will communicate with site manager
			to update the committed values of dataids
			that has been written by transaction Tid.
		"""
		T = self.transactions[Tid]
		commit_time = time.time()
		for dataid in T.uncommitted_data:
			value = T.uncommitted_data[dataid]
			lock_sites = T.write_lock_sites[dataid]
			self.site_manager.update_value(dataid, value, commit_time, lock_sites)

	def __abort(self, Tid):
		"""
			abort transaction Tid
			remove the transaction from transaction manager,
			release locks hold be the transaction
			update the wait-for-graph
			update transaction status of other transactions
			retry waiting commands 
		"""
		if Tid not in self.transactions:
			print ("T"+str(Tid)+" is no longer active.")
		print ("Aborting T" + str(Tid))
		
		# remove locks related to Tid
		T = self.transactions[Tid]
		read_ids = list(T.read_values.keys())
		write_ids = list(T.uncommitted_data.keys())
		data_accessed = list(set(read_ids + write_ids))
		self.site_manager.release_locks(Tid, data_accessed)

		# Remove abort transaction from all transactions
		del self.transactions[Tid]

		# update self.wait_for_graph
		self.__update_wait_for_graph(Tid)

		# update remaining transaction status
		self.__update_transaction_status()
		print ("Abort is done.")

		# retry waiting commands if there is any
		if self.waiting_commands:
			print ("Retry waiting commads after abort")
			self.__try_waiting_commands()


	def __commit(self, Tid):
		"""
			commit transaction Tid
			update the committed values of data items written by Tid if Tid is read-write 
			remove the transaction from transaction manager,
			release locks hold be the transaction
			update the wait-for-graph
			update transaction status of other transactions
			retry waiting commands 
		"""
		print ("Committing T" + str(Tid))
		T = self.transactions[Tid]
		# If T is RW:
		# update uncommited values
		if T.get_type() == "RW":
			self.__update_uncommitted_value(Tid)

		# remove locks related to Tid
		#print ('Release locks given by transaction ' + str(Tid))
		T = self.transactions[Tid]
		read_ids = list(T.read_values.keys())
		write_ids = list(T.uncommitted_data.keys())
		data_accessed = list(set(read_ids + write_ids))
		self.site_manager.release_locks(Tid, data_accessed)

		# Remove commit transaction from all transactions
		del self.transactions[Tid]

		# update self.wait_for_graph
		#print ("Update wait-for-graph after commit")
		self.__update_wait_for_graph(Tid)

		# update remaining transaction status
		#print ("Update remaining active transaction status after commit")
		self.__update_transaction_status()
		print ("Commit is done.")

		# retry waiting commands if there is any
		if self.waiting_commands:
			print ("Retry waiting commads after commit")
			self.__try_waiting_commands()

		#print ("Commit is done.")

		
	def __visit(self,tid,path,visited):
		"""
			recursive function to detect cycles in the wait-for-graph
		"""
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
		"""
			DFS
			call recursive function to detect deadlocks
			and store the path of the cycle if any
		"""
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
		"""
			keep finding deadlocks and abort the youngest transaction to eliminate deadlock
			until no deadlocks can be found.
		"""
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
		"""
			dump data values
		"""
		self.site_manager.dump(sites, indices)


	def end(self, Tid):
		"""
			We will not end waiting transaction
			since we abort transactions inmmediately after site failure or deadlocks,
			we only commit transaction in the end function
		"""
		if Tid in self.transactions:
			T = self.transactions[Tid]
			if T.get_status() == "WAIT":
				print ("End T"+str(Tid))
				#self.__abort(Tid)
				print ('T ' +str(Tid) +" is still waiting.")
			else:
				print ("End T"+str(Tid))
				self.__commit(Tid)
		#else:
			#print ('End: T'+str(Tid)+" is no longer active.")


	def recover(self, siteid):
		"""
			recover site(siteid)
			transaction manager asks site manager to recover site siteid
			then retry all the waiting commands
		"""
		print ("Site "+str(siteid) + " recovers.")
		self.site_manager.recover(siteid)
		self.__try_waiting_commands()


	def fail(self, siteid):
		"""
			fail site(siteid)
			transaction manager asks site manager to fail site siteid
			then transaction manager aborts 
			all transactions that accessed that site
		"""
		print ("Site "+str(siteid) + " fails.")
		self.site_manager.fail(siteid)

		abort_ids = []
		data_ids = self.site_manager.get_site_keys(siteid)
		for dataid in data_ids:
			for Tid in self.transactions:
				T = self.transactions[Tid]
				if dataid in T.read_values or dataid in T.uncommitted_data:
					abort_ids.append(Tid)

		abort_ids = list(set(abort_ids))
		for abortid in abort_ids:
			print ("T"+str(abortid)+" aborts due to site failure.")
			self.__abort(abortid)



