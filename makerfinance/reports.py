from StringIO import StringIO
from collections import OrderedDict
from csv import DictWriter
import csv
from datetime import date, timedelta, time, datetime
from itertools import groupby
import pickle
from pprint import pformat
from zipfile import ZipFile
from makerfinance.util import encode, decode

__author__ = 'andriod'


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
    open("Posting_Summary_" + postingTime + ".txt", "wt").write(postingSummary)
    for agent, (textReport, binaryReport) in postingReports.iteritems():
        open("Posting_Report_" + postingTime + "_" + agent + ".txt", "wt").write(textReport)
        open("Posting_Report_" + postingTime + "_" + agent + ".pkl", "wb").write(binaryReport)

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
    ret += u"\t{date}\t{amount}\t{flags}\t{account}\t{type}:{subtype}\t{cpty}\t{agent}".format(amount=entry.pop("amount")
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
        entry.pop("fee_for",'')
        entry.pop("agent_id","")
        entry.pop("counter_party_id","")
    if not entry:
        return ret
    return ret + "\n" + pformat(entry)


def list_transactions(ledger):
    for item in ledger.domain:
        print format_entry(item)

def all_balances(ledger,group_by='bank_account', *args, **kwargs):
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
        zip(fieldnames, ["Account"] + (["Start"] if "Start" in fieldnames else []) + \
                        [month.strftime("%B %Y") for month in months] + ["Net"]+ \
                        (["End"] if "End" in fieldnames else []))))
    return flowSummaryWriter


def cash_flow_report_set(ledger,start, end, account_grouping):
    ret = {}
    months = set()
    end -= timedelta(microseconds=1)
    inWhere = "effective_date between '{start}' and '{end}'".format(start=encode(start),
        end=encode(end, epsilon=True))
    startWhere = "effective_date < '{start}'".format(start=encode(start),
        end=encode(end, epsilon=True))
    endWhere = "effective_date <= '{end}'".format(start=encode(start),
        end=encode(end, epsilon=True))
    startingBalances = all_balances(ledger,group_by=account_grouping, where=startWhere)
    startingBalanceReport = format_account_balances(startingBalances)
    activeBudgetAccounts = set(x[0] for x in startingBalances.keys())
    endingBalances = all_balances(ledger,group_by=account_grouping, where=endWhere)
    endingBalanceReport = format_account_balances(endingBalances)
    monthlyFlowReport = "Month\tAccount\tAmount\n"
    monthlyFlow = all_balances(ledger,group_by=('effective_month', account_grouping), where=inWhere)
    for (month, budgetAccount), amount in monthlyFlow.iteritems():
        monthlyFlowReport += "{month}\t{budget_account}\t${amount}\n".format(month=month.strftime("%B %Y"),
            budget_account=budgetAccount, amount=amount)
        activeBudgetAccounts.add(budgetAccount)
        months.add(month)
    activeBudgetAccounts = sorted(activeBudgetAccounts)
    #Move total to end
    activeBudgetAccounts.remove('')
    activeBudgetAccounts.append('')
    months = sorted(months)
    quarterFlow = all_balances(ledger,group_by=account_grouping, where=inWhere)
    quarterFlowReport = format_account_balances(quarterFlow)

    flowSummary = OrderedDict()
    flowSummaryBuffer = StringIO()

    netFlow = OrderedDict()
    netFlowBuffer = StringIO()

    flowSummaryWriter = initialize_writer(["Account", "Start"] + months + ["Net", "End"], flowSummaryBuffer, months)
    netFlowWriter = initialize_writer(["Account",] + months + ["Net"], netFlowBuffer, months)
    for budgetAccount in activeBudgetAccounts:
        row = {"Account": "\t "*budgetAccount.count(":")+budgetAccount if budgetAccount else "Total",
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
                      account_grouping = 'budget_account'):

    baseDate = date.today()
    if year is not None:
        baseDate = baseDate.replace(year=year)
    start = end = baseDate
    start = start.replace(month=(quarter - 1) * 3 + 1, day=1)
    end = datetime.combine(end.replace(month=quarter * 3 + 1, day=1), time(0))

    return cash_flow_report_set(ledger,start, end, account_grouping)

def make_quarterly_zipfile(ledger, reports_zip, quarter, year=None, account_grouping = 'budget_account'):
    quarterly = ZipFile(reports_zip, "w")
    print "Saving Quarterly Report to ", reports_zip
    for title, text in quarterly_reports(ledger, quarter, year, account_grouping=account_grouping).iteritems():
        quarterly.writestr(title.replace(" ", "_") + ".tsv", text)


def member_report(ledger):
    print "\nMembers"
    writer = csv.writer(open("member_list.csv", "w"))
    writer.writerow(("member_id", "name", "plan", "start", "end"))
    print "Name\t\tPlan\tMember Until"
    for member in ledger.member_list():
        writer.writerow(member)
        member_id, name, plan, start, end = member
        if decode(end) < datetime.now():
            plan = "Expired " + plan
        print "{name}\t{plan}\t{end}".format(name=name, plan=plan, end=end)