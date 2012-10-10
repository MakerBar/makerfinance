__author__ = 'andriod'

import argparse
parser = argparse.ArgumentParser(
    description='Command line script for interaction with the makerfinance system',
)

command_subparsers = parser.add_subparsers(help = "commands")

post_parser = command_subparsers.add_parser('post',help='Post ready transactions')
post_parser.add_argument('--command',action='store',default = 'post')

report_parser = command_subparsers.add_parser('report',help='Generate report on current state')
report_parser.add_argument('--command',action='store',default = 'report', help=False)

opt = parser.parse_args()

print opt