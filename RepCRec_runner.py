'''
CSCI-GA 2434 Advanced Database Systems Final Project
Replicated Concurrency Control and Recovery

Authors:
Ren Yi--ry708@nyu.edu
Yanqiu Wu--yw1370@nyu.edu
'''
import argparse
from RepCRec import Workflow


parser = argparse.ArgumentParser('A distributed database with replicated concurrency control and recovery system')

parser.add_argument('file_name', type=str, help="Path to sample test file")

args = parser.parse_args()

workflow = Workflow()

workflow.file_name = args.file_name

workflow.run()
