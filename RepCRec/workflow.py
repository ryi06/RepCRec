'''
CSCI-GA 2434 Advanced Database Systems Final Project
Replicated Concurrency Control and Recovery

Authors:
Ren Yi--ry708@nyu.edu
Yanqiu Wu--yw1370@nyu.edu
'''
import re
from .transaction_manager import TransactionManager
from .site_manager import SiteManager
from .utils import *

class Workflow(object):
	def __init__(self):
		self.file_name = None

	def run(self):
		# Initialize database
		self.site_manager = SiteManager()
		self.transaction_manager = TransactionManager(self.site_manager)
		# Process
		self.process_instructions()

	def process_instructions(self):
		"""Process the input transaction file"""
		with open(self.file_name, 'r') as T:
			for line in T:
				record = self.parse_instruction(line.strip())
				self.distribute_instruction(record)


	def parse_instruction(self, record):
		"""parse transaction instructions one line at a tine"""
		keyword, params, _ = re.split(r'\((.*)\)', record)
		return (keyword, params)

	def distribute_instruction(self, record):
		"""Pass instruction to transaction_manager"""
		keyword, params = record

		# begin(T1)
		if keyword == 'begin':
			self.transaction_manager.begin(name2id(params), params)
		
		# beginRO(T2)
		if keyword == 'beginRO':
			self.transaction_manager.begin_RO(name2id(params), params)
		
		# R(T2,x2)
		elif keyword == 'R':
			t, x = params.split(",")
			self.transaction_manager.read(name2id(t), name2id(x))

			
		# W(T1,x1,101) 
		elif keyword == 'W':
			t, x, v = params.split(",")
			self.transaction_manager.write(name2id(t), name2id(x), int(v))
		
		# dump()/dump(i)/dump(xi)
		elif keyword == 'dump':
			if params == "":
				self.transaction_manager.dump()
			elif "x" in params:
				self.transaction_manager.dump(indices=[name2id(params)])
			else:
				self.transaction_manager.dump(sites=[int(params)])
		
		# end(T1) 
		elif keyword == 'end':
			self.transaction_manager.end(name2id(params))
		
		# recover(2)
		elif keyword == 'recover':
			self.transaction_manager.recover(int(params))
		
		# fail(2)
		elif keyword == 'fail':
			self.transaction_manager.fail(int(params))




