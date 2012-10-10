import pickle
from pprint import pprint
from doublex.pyDoubles import proxy_spy

__author__ = 'andriod'
from datetime import datetime
import makerfinance.config as mfconfig

from makerfinance.ledger import INCOME, EXPENSE, FOUNDERS_LOAN,\
    PRIMARY_CHECKING, CASH_BOX,\
    connect_config_ledger
from makerfinance.reports import  make_quarterly_zipfile, cash_flow_report_set, list_transactions, member_report, cash_flow_monthly
import argparse
parser = argparse.ArgumentParser(
    description='Command line script for interaction with the makerfinance system',
)
parser.add_argument("--save_cache",action="store_true",default=False )
command_subparsers = parser.add_subparsers(help = "commands",dest='command')

post_parser = command_subparsers.add_parser('post',help='Post ready transactions')

report_parser = command_subparsers.add_parser('report',help='Generate report on current state')

opt = parser.parse_args()

print opt

mfconfig.init()
config = mfconfig.config
ledger = connect_config_ledger(config, cache=opt.save_cache)

if  opt.command == "report":


    bankBalances = ledger.balances()
    print "\n\nBalances:"
    print "\n".join("%s $%s" % (" - ".join(name), amount) for name, amount in bankBalances.iteritems() if amount)
    ledger.dump_to_csv("test_ledger.csv")

    print member_report(ledger)
#    pprint(ledger.cache)


    print "\nEvent net income -loss"
    eventBalances = ledger.balances(group_by='event', depth=1)
    print "\n".join("%s $%s" % (" - ".join(name), amount) for name, amount in eventBalances.iteritems())

    print "\nBalance of budget accounts"
    budgetBalances = ledger.balances(group_by='budget_account', depth=2)
    print "\n".join("%s $%s" % (" - ".join(name), amount) for name, amount in budgetBalances.iteritems())

    print
    print cash_flow_monthly(ledger,True)
    print
    print cash_flow_monthly(ledger,False)


if opt.save_cache:
    # remove domain if present
    for type,cache in ledger.cache.iteritems():
        for call, ret in cache.iteritems():
            for rs in ret:
                if hasattr(rs,"domain"):
                    del rs.domain
    pickle.dump(ledger.cache,open("mf_cache.pkl","w"))
