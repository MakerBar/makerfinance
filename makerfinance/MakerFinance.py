import pickle
from pprint import pprint
from time import mktime
from doublex.pyDoubles import proxy_spy
from makerfinance.util import decode
from poodledo.apiclient import ApiClient, PoodledoError

__author__ = 'andriod'
from datetime import datetime, time, timedelta
import makerfinance.config as mfconfig

from makerfinance.ledger import INCOME, EXPENSE, FOUNDERS_LOAN,\
    PRIMARY_CHECKING, CASH_BOX,\
    connect_config_ledger, MONTH
from makerfinance.reports import  make_quarterly_zipfile, cash_flow_report_set, list_transactions, member_report, cash_flow_monthly
import argparse
parser = argparse.ArgumentParser(
    description='Command line script for interaction with the makerfinance system',
)
parser.add_argument("--save_cache",action="store_true",default=False )
command_subparsers = parser.add_subparsers(help = "commands",dest='command')

todo_parser = command_subparsers.add_parser('update-todos',help='Update toodledo todos')

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
elif opt.command == "update-todos":

    user_email = config.get('toodledo', 'username')
    password = config.get('toodledo', 'password')
    app_id = config.get('toodledo', 'id')
    app_token = config.get('toodledo', 'token')
    client = ApiClient(app_id=app_id,app_token=app_token,cache_xml=True)
    client.authenticate(user_email, password)
#        config.set('cache','user_token',str(cached_client._key))
#        store_config(config)

    for member in sorted(ledger.member_list(),key=lambda member:member[2]):
        member_id, name, plan, start, end = member
        start, end = decode(start), decode(end)
#        if end < datetime.now():
#            plan = "Expired " + plan
        taskName = name+" dues reminder"
        task = None
        try:
            task = client.getTask(taskName)
        except PoodledoError:
            if plan == "Individual" or end > datetime.now():
                client.addTask(taskName)
                task = client.getTask(taskName)

        if not task:
            continue
        if end < datetime.now() - timedelta(days=MONTH*2):
            client.deleteTask(task.id)
        else:
            noteTemplate = """Dear {member} just a reminder that your MakerBar membership {willHas} at {end} please bring a check at the next MakerBar event or setup Chase Quick Pay to Treasurer@MakerBar.com for a way to transfer money without a fee for either you or MakerBar.  Chase Quick Pay can even be scheduled to pay automatically."""
            client.editTask(task.id,duedate=mktime(end.date().timetuple()), note = noteTemplate.format(member = name, start = start, end=end,
                            willHas = ("has expired" if end < datetime.now() else "will expire"))) #duetime = time(end.time().timetuple),

if opt.save_cache:
    # remove domain if present
    for type,cache in ledger.cache.iteritems():
        for call, ret in cache.iteritems():
            for rs in ret:
                if hasattr(rs,"domain"):
                    del rs.domain
    pickle.dump(ledger.cache,open("mf_cache.pkl","w"))
