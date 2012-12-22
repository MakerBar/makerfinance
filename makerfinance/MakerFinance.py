import csv
import dateutil.parser
from decimal import Decimal
import pickle

import re
from time import mktime
from makerfinance.util import decode

POODLEDO_AVAILABLE = False
try:
    from poodledo.apiclient import ApiClient, PoodledoError

    POODLEDO_AVAILABLE = True
except ImportError:
    pass
__author__ = 'andriod'

from datetime import datetime, timedelta
import makerfinance.config as mfconfig

from makerfinance.ledger import INCOME, EXPENSE, FOUNDERS_LOAN,\
    PRIMARY_CHECKING, CASH_BOX,\
    connect_config_ledger, MONTH
from makerfinance.reports import  make_quarterly_zipfile, cash_flow_report_set, list_transactions, member_report, cash_flow_monthly
import argparse


check_regex = re.compile(r'CHECK (\d+) ')
trans_regexs = [
    re.compile(r'Chase QuickPay Electronic Transfer (\d+) (from|to) (.+)'), # QuickPay
    re.compile(r'.*PPD ID: (.+)'), #PPD?
    re.compile(r'CHASE            EPAY       (\d+)      WEB ID: (\d+)')  #EPAY
]

parser = argparse.ArgumentParser(
    description='Command line script for interaction with the makerfinance system',
)
parser.add_argument("--save_cache", action="store_true", default=False)
command_subparsers = parser.add_subparsers(help="commands", dest='command')

if POODLEDO_AVAILABLE:
    todo_parser = command_subparsers.add_parser('update-todos', help='Update toodledo todos')

check_parser = command_subparsers.add_parser('check', help='Check transactions against report from bank')
check_parser.add_argument("file", action="store", type=argparse.FileType('rt'))
check_parser.add_argument("--hide-phone", action="store_false", dest="phone",
    help="hide messages about unmatchable digital deposits")
check_parser.add_argument("--min", action="store", dest='min', default=.01, help='Minimum transaction to flag.',
    type=float)
check_parser.add_argument("--bank_account", action="store", dest="bank_account", default=None,
    help="Include only transactions in a particular bank account", type=str)

post_parser = command_subparsers.add_parser('post', help='Post ready transactions')

report_parser = command_subparsers.add_parser('report', help='Generate report on current state')
report_parser.add_argument("--report", action="append", dest="reports", choices="members",
    help="List of reports to run")
report_parser.add_argument("--date", action="store", dest="date", type=dateutil.parser.parse, default=datetime.now(),
    help="Date on which to run the report (where supported)")

opt = parser.parse_args()

print opt

mfconfig.init()
config = mfconfig.config
ledger = connect_config_ledger(config, cache=opt.save_cache)

def find_transaction_id(row):
    for regex in reversed(trans_regexs):
        match = regex.match(row['Description'])
        if match:
            return match.group(1)
    return row['Description']

if  opt.command == "report":
    if not len(opt.reports):
        bankBalances = ledger.balances()
        print "\n\nBalances:"
        print "\n".join("%s $%s" % (" - ".join(name), amount) for name, amount in bankBalances.iteritems() if amount)
        ledger.dump_to_csv("test_ledger.csv")

    if not len(opt.reports) or "members" in opt.reports:
        print member_report(ledger, asof_date=opt.date)
        #    pprint(ledger.cache)

    if not len(opt.reports):
        print "\nEvent net income -loss"
        eventBalances = ledger.balances(group_by='event', depth=1)
        print "\n".join("%s $%s" % (" - ".join(name), amount) for name, amount in eventBalances.iteritems())

        print "\nBalance of budget accounts"
        budgetBalances = ledger.balances(group_by='budget_account', depth=2)
        print "\n".join("%s $%s" % (" - ".join(name), amount) for name, amount in budgetBalances.iteritems())

        print
        print cash_flow_monthly(ledger, True)
        print
        print cash_flow_monthly(ledger, False)
elif opt.command == "update-todos":
    user_email = config.get('toodledo', 'username')
    password = config.get('toodledo', 'password')
    app_id = config.get('toodledo', 'id')
    app_token = config.get('toodledo', 'token')
    client = ApiClient(app_id=app_id, app_token=app_token, cache_xml=True)
    client.authenticate(user_email, password)
    #        config.set('cache','user_token',str(cached_client._key))
    #        store_config(config)

    for member in sorted(ledger.member_list(), key=lambda member: member[2]):
        member_id, name, plan, last_payment, start, end = member
        start, end = decode(start), decode(end)
        #        if end < datetime.now():
        #            plan = "Expired " + plan
        taskName = name + " dues reminder"
        task = None
        try:
            task = client.getTask(taskName)
        except PoodledoError:
            if plan == "Individual" or end > datetime.now():
                client.addTask(taskName)
                task = client.getTask(taskName)

        if not task:
            continue
        if end < datetime.now() - timedelta(days=MONTH * 2):
            client.deleteTask(task.id)
        else:
            noteTemplate = """Dear {member} just a reminder that your MakerBar membership {willHas} at {end} please
            bring a check at the next MakerBar event or setup Chase Quick Pay to Treasurer@MakerBar.com for a way to
            transfer money without a fee for either you or MakerBar.  Chase Quick Pay can even be scheduled to pay
            automatically.

            Your last payment was on {last_payment}"""
            client.editTask(task.id, duedate=mktime(end.date().timetuple()),
                note=noteTemplate.format(member=name, start=start, end=end,
                    willHas=("has expired" if end < datetime.now() else "will expire"),
                    last_payment=last_payment)) #duetime = time(end.time().timetuple),
elif opt.command == 'check':
    bankLedger = csv.DictReader(opt.file)
    bankLedger.fieldnames = [x.strip() for x in bankLedger.fieldnames]
    for row in bankLedger:
        bank_id = None
        type = None
        transaction = None
        bankTransactionType = row.get('Type', None)
        if 'Amount' in row:
            bankTransactionAmount = Decimal(row['Amount'])
        else:
            bankTransactionAmount = Decimal(row['Net'])

        if "Transaction ID" in row: # Only PayPal is so convenient so far
            bank_id = row["Transaction ID"]
            if bankTransactionType in ['Update to eCheck Received', 'Invoice Received', 'Cancelled Fee']:
                continue # non financial record in PayPal Export
        elif bankTransactionType == 'CHECK':
            type = EXPENSE
            match = check_regex.match(row['Description'])
            if match:
                bank_id = match.group(1)
        elif bankTransactionType == 'CREDIT':
            type = INCOME
            bank_id = find_transaction_id(row)
        elif bankTransactionType == 'DEBIT':
            type = EXPENSE
            bank_id = find_transaction_id(row)
        elif bankTransactionType == 'DSLIP':
            if opt.phone:
                print "Unable to match phone deposit for ${amount}".format(amount=bankTransactionAmount)
            continue
        else:
            print "unknown type:", bankTransactionType
            continue

        if bank_id:
            trans_filters = {'bank_id': bank_id}
            if type:
                trans_filters['type'] = type
            if opt.bank_account:
                trans_filters['bank_account'] = opt.bank_account

            transactions = ledger.find_transactions(**trans_filters)

        if not transactions:
            if abs(float(bankTransactionAmount)) >= opt.min:
                print "#{bank_id} Unmatched row ".format(bank_id=bank_id), row
            continue

        ledgerTransactionAmount = 0
        ledgerTransactionSubtypes = []
        ledgerCounterParties = []
        ledgerIDs = []

        for transaction in transactions:
            ledgerTransactionAmount += decode(transaction['amount'])
            ledgerTransactionSubtypes.append(transaction['subtype'])
            ledgerCounterParties.append(transaction['counter_party'])
            ledgerIDs.append(transaction.name)

        if ledgerTransactionAmount != bankTransactionAmount:
            print "#{bank_id} Mismatched amounts {bank} {ledger} #{ledger_id} {subtype} {counterparty}".format(
                bank_id=bank_id,
                bank=bankTransactionAmount, ledger=ledgerTransactionAmount, subtype=",".join(ledgerTransactionSubtypes),
                counterparty=",".join(ledgerCounterParties), ledger_id=",".join(ledgerIDs))



            #Type,Post Date,Description,Amount
if opt.save_cache:
    # remove domain if present
    for type, cache in ledger.cache.iteritems():
        for call, ret in cache.iteritems():
            for rs in ret:
                if hasattr(rs, "domain"):
                    del rs.domain
    pickle.dump(ledger.cache, open("mf_cache.pkl", "w"))

