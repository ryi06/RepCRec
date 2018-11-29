'''
CSCI-GA 2434 Advanced Database Systems Final Project
Replicated Concurrency Control and Recovery

Authors:
Ren Yi--ry708@nyu.edu
Yanqiu Wu--yw1370@nyu.edu
'''
import time
from collections import defaultdict

class Transaction(object):
	def __init__(self, ID, name, transType, status):
		self.id = ID
		self.name = name
		self.type = transType
		self.start_time = time.time()
		self.status = status
		self.read_values = []
		self.read_data = set()
		self.uncommitted_data = dict()

