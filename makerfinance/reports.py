from StringIO import StringIO
from collections import OrderedDict
from csv import DictWriter
from datetime import date, timedelta, time, datetime
from zipfile import ZipFile
from makerfinance.ledger import encode

__author__ = 'andriod'

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
