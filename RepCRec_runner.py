'''
CSCI-GA 2434 Advanced Database Systems Final Project
Replicated Concurrency Control and Recovery

Team:
Ren Yi--ry708@nyu.edu
Yanqiu Wu--yw1370@nyu.edu

Running instructions
	1. With input file: python RepCRec_runner.py <input_file_name>
	2. From stdin: 
		1) cat <input_file_name> | python RepCRec_runner.py
		2) python RepCRec_runner.py < <input_file_name>
		3) echo -e <instruction_string> | python RepCRec_runner.py
	   	   Note: <instructions_string> need to be separated by '\n'. For example:
	   	   "begin(T1)\nbegin(T2)\nW(T1,x1,101)\nW(T2,x2,202)\nW(T1,x2,102)\nW(T2,x1,201)\nend(T1)\ndump()"

'''

import argparse, sys
from RepCRec import Workflow


workflow = Workflow()

# input file
if sys.stdin.isatty():
	parser = argparse.ArgumentParser('RepCRec')
	parser.add_argument('file_name', type=str, help="Path to sample test file")
	args = parser.parse_args()
	workflow.file_name = args.file_name
else: # input from stdin
	workflow.stdin = sys.stdin.readlines()


workflow.run()
