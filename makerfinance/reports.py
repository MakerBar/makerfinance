# Copyright 2012, 2013 Andy Fundinger
# This file is part of makerfinance.
#
# makerfinance is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# makerfinance is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with makerfinance.  If not, see <http://www.gnu.org/licenses/>.

from StringIO import StringIO
from collections import OrderedDict, defaultdict, Counter
from csv import DictWriter
import csv
from datetime import date, timedelta, time, datetime
from decimal import Decimal
from itertools import groupby
import pickle
from pprint import pformat
from zipfile import ZipFile
from makerfinance.util import encode, decode

__author__ = 'andriod'

def daily_balance(ledger,filter=None, format=None):

    bankBalances = ledger.balances(group_by=['day','bank_account'])
    ret = ""
    if format != "csv":
        ret =  "\n\nBalances:\n"
    balances = OrderedDict()
    currDate = None
    for (date,account),amount in bankBalances.iteritems():
        if date != currDate and balances:
            if format == "csv":
                ret +=  ",".join([str(currDate.date())]+['"%d"'%x for x in balances.values()])+"\n"
            else:
                ret += "Date: %s\n"%str(currDate.date())
                for dAccount, balance in sorted(balances.iteritems()):
                    if balance != 0:
                        ret += "\t%s\t%f\n"%(dAccount,balance)
        currDate = date
        if not filter or account in filter:
            balances[account] = balances.get(account,Decimal(0)) + amount
    if format == "csv":
        ret = ",".join(["Date"]+['"%s"'%x for x in balances.keys()])+"\n"+ret
    return ret

def make_posting_reports(postingTime, toPost):
    """
    Used by the ledger to prepare mandatory reports when posting.
    """
    agentKey = lambda item: item["agent"]
    toPost.sort(key=agentKey)
    postingReports = {}
    for agent, agentToPost in groupby(toPost, agentKey):
        postingReports[agent] = make_posting_report(agentToPost)
    postingReports['Board'] = make_posting_report(toPost)
    postingSummary = "transaction id, date, checksum, checksum version\n"
    for entry in toPost:
        postingSummary += "{id},{date},{checksum},{version}\n".format(id=entry.name, date=entry["effective_date"],
            checksum=entry["checksum"], version=entry["checksum_version"])
    postDetails = ZipFile("Posting_Details_" + postingTime + ".zip", "w")
    postDetails.writestr("Posting_Summary_" + postingTime + ".txt", postingSummary)
    for agent, (textReport, binaryReport) in postingReports.iteritems():
        postDetails.writestr("Posting_Report_" + postingTime + "_" + agent + ".txt", textReport)
        postDetails.writestr("Posting_Report_" + postingTime + "_" + agent + ".pkl", binaryReport)


def make_posting_report(entries):
    """
    Prepare text and binary format versions of the posted records
    """
    entries = list(entries)
    lines = []
    for entry in entries:
        lines.append(format_entry(entry))

    binary = pickle.dumps([(entry.name, dict(entry)) for entry in entries])
    return "\n".join(lines), binary


def format_entry(entry, verbose=False):
    """
    Format entry in a standard, readable way.
    * if not verbose, pop fields that are generally unnecessary
    * unknown fields are pretty printed as a best effort fallback
    """
    ret = unicode(entry.name)
    entry = dict(entry)

    posted = entry.pop('posted')
    ret += "\t" + entry.pop('state')
    if posted:
        ret += "\t" + str(decode(posted).date()) + "\t" + entry.pop('checksum', "MISSING CHECKSUM")

    ret += "\n"
    flags = ("E" if decode(entry['external']) else ("T" if entry.pop("transfer_id", False) else "I"))
    account = entry.pop("bank_account") + "\t" + entry.pop("budget_account")
    ret += u"\t{date}\t{amount}\t{flags}\t{account}\t{type}:{subtype}\t{cpty}\t{agent}".format(
        amount=entry.pop("amount")
        ,
        account=account, cpty=entry.pop("counter_party", "Internal" if not decode(entry.pop("external")) else "ERROR"),
        agent=entry.pop("agent"),
        flags=flags, type=entry.pop("type"), subtype=entry.pop("subtype"),
        date=decode(entry.pop("effective_date")).date())
    if 'notes' in entry:
        ret += "\n\t" + entry.pop('notes')
    if not verbose:
        entry.pop("test")
        entry.pop("tax_inclusive")
        entry.pop('entered')
        entry.pop('modified')
        entry.pop("bank_id")
        entry.pop("checksum_version", "")
        entry.pop("date")
        entry.pop("effective_until", "")
        entry.pop("plan", "")
        entry.pop("event", "")
        entry.pop("fee_for", '')
        entry.pop("agent_id", "")
        entry.pop("counter_party_id", "")
    if not entry:
        return ret
    return ret + "\n" + pformat(entry)


def list_transactions(ledger,**filters):
    """
    Print a list of transactions
    :param ledger: Ledger to print transactions from.
    :param filters: a dictionary of functions to filter by, only transactions for which all the filter functions are true will be included
    """
    for item in ledger:
        if not all(filterFunc(item[filterName]) for filterName,filterFunc in filters.iteritems()):
            continue
        print format_entry(item)


def all_balances(ledger, group_by='bank_account', *args, **kwargs):
    ret = {}
    ret.update(ledger.balances(group_by, -1, *args, **kwargs))
    if not ret:  #empty selection
        return {}
    maxDepth = max(
        max(sub_key.count(":") for sub_key in bal_key if hasattr(sub_key, "count")) for bal_key in ret.iterkeys())
    while maxDepth >= 0:
        ret.update(ledger.balances(group_by, maxDepth, *args, **kwargs))
        maxDepth -= 1
    return OrderedDict(sorted(ret.iteritems()))


def format_account_balances(balances_by_account):
    quarterFlowReport = "Account\tBalance\n"
    for (budgetAccount,), amount in balances_by_account.iteritems():
        quarterFlowReport += "{budget_account}\t${amount}\n".format(budget_account=budgetAccount, amount=amount)
    return quarterFlowReport


def initialize_writer(fieldnames, buffer, months):
    flowSummaryWriter = DictWriter(buffer, fieldnames=fieldnames, delimiter="\t")
    flowSummaryWriter.writerow(dict(
        zip(fieldnames, ["Account"] + (["Start"] if "Start" in fieldnames else []) +\
                        [month.strftime("%B %Y") for month in months] + ["Net"] +\
                        (["End"] if "End" in fieldnames else []))))
    return flowSummaryWriter


def cash_flow_report_set(ledger, start, end, account_grouping):
    ret = {}
    months = set()
    end -= timedelta(microseconds=1)
    inWhere = "effective_date between '{start}' and '{end}'".format(start=encode(start),
        end=encode(end, epsilon=True))
    startWhere = "effective_date < '{start}'".format(start=encode(start),
        end=encode(end, epsilon=True))
    endWhere = "effective_date <= '{end}'".format(start=encode(start),
        end=encode(end, epsilon=True))

    rs = ledger.select_taxable_transactions(start, end)
    gross = sum(decode(transaction['amount']) for transaction in rs)
    tax_inclusive = sum(decode(transaction['tax_inclusive']) for transaction in rs)
    taxable = tax_inclusive / (1 + ledger.tax_rate)
    tax = taxable * ledger.tax_rate
    gross -= tax
    deductions = gross - taxable
    ret["Tax"] = "Quarterly Tax Statement\n"
    ret["Tax"] += "Gross Receipts\tDeductions\tTaxable\tTax Due\n"
    ret["Tax"] += "\t".join(str(x) for x in [gross, deductions, taxable, tax]) + "\n"
    ret["Tax"] += "Sales Tax Due this quarter {tax} on {taxable}".format(tax=tax, taxable=taxable)

    startingBalances = all_balances(ledger, group_by=account_grouping, effective_date_before=start)
    startingBalanceReport = format_account_balances(startingBalances)
    activeBudgetAccounts = set(x[0] for x in startingBalances.keys())
    endingBalances = all_balances(ledger, group_by=account_grouping, effective_date_before=end)
    endingBalanceReport = format_account_balances(endingBalances)
    monthlyFlowReport = "Month\tAccount\tAmount\n"
    monthlyFlow = all_balances(ledger, group_by=('effective_month', account_grouping), effective_date_after=start, effective_date_before=end)
    for (month, budgetAccount), amount in monthlyFlow.iteritems():
        monthlyFlowReport += "{month}\t{budget_account}\t${amount}\n".format(month=month.strftime("%B %Y"),
            budget_account=budgetAccount, amount=amount)
        activeBudgetAccounts.add(budgetAccount)
        months.add(month)
    activeBudgetAccounts = sorted(activeBudgetAccounts)
    months = sorted(months)
    quarterFlow = all_balances(ledger, group_by=account_grouping, effective_date_after=start, effective_date_before=end)
    quarterFlowReport = format_account_balances(quarterFlow)

    flowSummary = OrderedDict()
    flowSummaryBuffer = StringIO()

    netFlow = OrderedDict()
    netFlowBuffer = StringIO()

    flowSummaryWriter = initialize_writer(["Account", "Start"] + months + ["Net", "End"], flowSummaryBuffer, months)
    netFlowWriter = initialize_writer(["Account", ] + months + ["Net"], netFlowBuffer, months)
    for budgetAccount in activeBudgetAccounts:
        row = {"Account": "\t " * budgetAccount.count(":") + budgetAccount if budgetAccount else "Total",
               "Net": quarterFlow.get((budgetAccount,), "")}
        for month in months:
            row[month] = monthlyFlow.get((month, budgetAccount), "")
        if row['Net']:
            netFlow[budgetAccount] = row
        row = dict(row)
        row.update({
            "Start": startingBalances.get((budgetAccount,), 0),
            "End": endingBalances.get((budgetAccount,), 0)
        })
        flowSummary[budgetAccount] = row
    flowSummaryWriter.writerows(flowSummary.itervalues())
    netFlowWriter.writerows(netFlow.itervalues())
    ret["Flow Summary"] = flowSummaryBuffer.getvalue()
    ret["Net Flow"] = netFlowBuffer.getvalue()
    ret["Monthly Net Cash Flow"] = monthlyFlowReport
    ret["Quarter Net Cash Flow"] = quarterFlowReport
    ret["Starting Balances"] = startingBalanceReport
    ret["Ending Balances"] = endingBalanceReport
    return ret


def quarterly_reports(ledger, quarter, year=None,
                      account_grouping='budget_account'):
    baseDate = date.today()
    if year is not None:
        baseDate = baseDate.replace(year=year)
    start = end = datetime.combine(baseDate,time(0))
    start = start.replace(month=(quarter - 1) * 3 + 1, day=1)
    end = datetime.combine(end.replace(month=quarter * 3 + 1, day=1), time(0))

    return cash_flow_report_set(ledger, start, end, account_grouping)


def make_quarterly_zipfile(ledger, reports_zip, quarter, year=None, account_grouping='budget_account'):
    quarterly = ZipFile(reports_zip, "w")
    print "Saving Quarterly Report to ", reports_zip
    for title, text in quarterly_reports(ledger, quarter, year, account_grouping=account_grouping).iteritems():
        quarterly.writestr(title.replace(" ", "_") + ".tsv", text)


def member_report(ledger, max_days=90, asof_date=None):
    if asof_date is None:
        asof_date = datetime.now()
    ret = "\nMembers\n"
    writer = csv.writer(open("member_list.csv", "w"))
    writer.writerow(("member_id", "name", "plan", "start", "end"))
    ret += "Name\t\tPlan\tMember Until\n"
    for member in sorted(ledger.member_list(), key=lambda member: (member[2],member[1],member[7])):
        member_id, name, plan, last_payment, last_bank_id, last_bank_acct, start, end = member
        if decode(start) > asof_date:
            continue
        if decode(end) < asof_date - timedelta(days=max_days):
            continue
        writer.writerow(member)
        if decode(end) < asof_date:
            plan = "Expired " + plan
        ret += "{name}\t{plan}\t{end}\n".format(name=name, plan=plan, end=end)
    return ret

def member_stats(ledger, format='text'):
    #writer.writerow(("member_id", "name", "plan", "start", "end"))
    #ret += "Name\t\tPlan\tMember Until\n"
    memberList = ledger.member_list()
    interestingDates = {}
    for member in memberList:
        member_id, name, plan, last_payment, last_bank_id, last_bank_acct, start, end = member
        interestingDates[decode(start)] = Counter(dict(zip(ledger.membership_plans.keys(),[0]*len(ledger.membership_plans))))
        interestingDates[decode(end)] = Counter(dict(zip(ledger.membership_plans.keys(),[0]*len(ledger.membership_plans))))
    for member in memberList:
        member_id, name, plan, last_payment, last_bank_id, last_bank_acct, start, end = member
        for dateOfInterest, counts in interestingDates.iteritems():
            if dateOfInterest<=decode(end) and dateOfInterest>= decode(start):
                counts[plan]+=1
    buffer = StringIO()
    fields = ["Date"]+ledger.membership_plans.keys()
    writer = DictWriter(buffer, fieldnames=fields,quoting=csv.QUOTE_ALL)
    writer.writerow(dict(zip(fields,fields)))
    for dateOfInterest, counts in sorted(interestingDates.iteritems()):
        row = dict(counts)
        row["Date"] = dateOfInterest
        writer.writerow(row)

    return buffer.getvalue()

def cash_flow_monthly(ledger, effective=False):
    currMonth = datetime.now().month
    quarterTotals = defaultdict(Decimal)

    ret = "Cash flow by month (%s)\n" % ('effective' if effective else 'actual')
    monthlyFlow = ledger.balances(group_by=('effective_month' if effective else 'month', 'type'),
        external=True)
    lastThree = defaultdict(list)
    for (month, type), amount in monthlyFlow.iteritems():
        if currMonth - 3 <= month.month < currMonth:
            quarterTotals[type] += amount
        lastThree[type].insert(0, amount)
        lastThree[type] = lastThree[type][0:3]
        ret += "%s %s %s \t\t3mo avg $%.2f\n" % (
            month.strftime("%B %Y"), type, amount, sum(lastThree[type]) / len(lastThree[type]))

    ret += "\n3 month averages\n"
    for type, amount in quarterTotals.iteritems():
        ret += "%s %s\n" % (type, float(amount) / 3.0)
    return ret

def entities(ledger):
    report = ""
    members = set()
    students = set()
    vendors = set()
    agents = set()
    customers = set()
    for transaction in ledger:
        if decode(transaction['amount']) == 0:
            continue
        if transaction['subtype'] == "Dues":
            if transaction['plan'] == "Class Only":
                students.add((transaction['counter_party_id'],transaction['counter_party']))
            else:
                members.add((transaction['counter_party_id'],transaction['counter_party']))
        elif 'transfer_id' in transaction:# transaction['subtype'] in ["Bank Transfer","Budget Transfer", "Reimbursement"]:
            pass
        elif transaction['type'] == "Income":
            customers.add((transaction['counter_party_id'],transaction['counter_party']))
        elif transaction['type'] == "Expense" and transaction['subtype'] != "Refund":
            vendors.add((transaction['counter_party_id'],transaction['counter_party']))
        agents.add((transaction['agent_id'],transaction['agent']))

    leaders = members.intersection(agents)
    customers.difference_update(members)
    customers.difference_update(students)
    nonMemberAgents = agents.difference(members)
    for section, entities in [("Members",members),
                              ("Students",students),
                              ("Non-Student Customers",customers),
                              ("Vendors",vendors),
                              ("Leaders",leaders),
                              ("Non-member Agents",nonMemberAgents)]:
        report += section + "\n-------\n"+"\n".join("%s:%s"%(id,name) for id,name in entities) + "\n\n"

    return report
