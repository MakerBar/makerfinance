__author__ = 'andriod'

import argparse
parser = argparse.ArgumentParser(
    description='Command line script for interaction with the makerfinance system',
)

command_subparsers = parser.add_subparsers(help = "commands",dest='command')

post_parser = command_subparsers.add_parser('post',help='Post ready transactions')

report_parser = command_subparsers.add_parser('report',help='Generate report on current state')

opt = parser.parse_args()

print opt