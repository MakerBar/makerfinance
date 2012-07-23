from StringIO import StringIO
from collections import OrderedDict
from csv import DictWriter
from datetime import date, timedelta, time, datetime
from zipfile import ZipFile
from makerfinance.ledger import all_balances, encode

__author__ = 'andriod'

def format_account_balances(balances_by_account):
    quarterFlowReport = "Account\tBalance\n"
    for (budgetAccount,), amount in balances_by_account.iteritems():
        quarterFlowReport += "{budget_account}\t${amount}\n".format(budget_account=budgetAccount, amount=amount)
    return quarterFlowReport


def quarterly_reports(quarter, year=None,
                      account_grouping = 'budget_account'):
    ret = {}
    months = set()

    baseDate = date.today()
    if year is not None:
        baseDate = baseDate.replace(year=year)
    start = end = baseDate
    start = start.replace(month=(quarter - 1) * 3 + 1, day=1)
    end = datetime.combine(end.replace(month=quarter * 3 + 1, day=1), time(0)) - timedelta(microseconds=1)

    quarterWhere = "effective_date between '{start}' and '{end}'".format(start=encode(start),
        end=encode(end, epsilon=True))
    startWhere = "effective_date < '{start}'".format(start=encode(start),
        end=encode(end, epsilon=True))
    endWhere = "effective_date <= '{end}'".format(start=encode(start),
        end=encode(end, epsilon=True))

    startingBalances = all_balances(group_by=account_grouping, where=startWhere)
    startingBalanceReport = format_account_balances(startingBalances)
    activeBudgetAccounts = set(x[0] for x in startingBalances.keys())

    endingBalances = all_balances(group_by=(account_grouping), where=endWhere)
    endingBalanceReport = format_account_balances(endingBalances)

    monthlyFlowReport = "Month\tAccount\tAmount\n"
    monthlyFlow = all_balances(group_by=('effective_month', account_grouping), where=quarterWhere)
    for (month, budgetAccount), amount in monthlyFlow.iteritems():
        monthlyFlowReport += "{month}\t{budget_account}\t${amount}\n".format(month=month.strftime("%B %Y"),
            budget_account=budgetAccount, amount=amount)
        activeBudgetAccounts.add(budgetAccount)
        months.add(month)
    activeBudgetAccounts = sorted(activeBudgetAccounts)
    months = sorted(months)

    quarterFlow = all_balances(group_by=account_grouping, where=quarterWhere)
    quarterFlowReport = format_account_balances(quarterFlow)

    flowSummary = OrderedDict()

    flowSummaryBuffer = StringIO()
    fieldnames = ["Account","Start"]+months+["Quarter","End"]
    flowSummaryWriter= DictWriter(flowSummaryBuffer,fieldnames = fieldnames,delimiter = "\t")
    flowSummaryWriter.writerow(dict(zip(fieldnames,["Account","Start"]+[month.strftime("%B %Y") for month in months]+["Quarter","End"])))
    for budgetAccount in activeBudgetAccounts:
        flowSummary[budgetAccount] = {"Account":budgetAccount,
                                      "Start": startingBalances.get((budgetAccount,), 0),
                                      "End": endingBalances.get((budgetAccount,), 0),
                                      "Quarter": quarterFlow.get((budgetAccount,), "")}
        for month in months:
            flowSummary[budgetAccount][month] = monthlyFlow.get((month, budgetAccount), "")

    flowSummaryWriter.writerows(flowSummary.itervalues())

    ret["Monthly Net Cash Flow"] = monthlyFlowReport
    ret["Quarter Net Cash Flow"] = quarterFlowReport
    ret["Starting Balances"] = startingBalanceReport
    ret["Ending Balances"] = endingBalanceReport
    ret["Flow Summary"] = flowSummaryBuffer.getvalue()

    return ret

def make_quarterly_zipfile(reports_zip, quarter, year=None, account_grouping = 'budget_account'):
    quarterly = ZipFile(reports_zip, "w")
    print "Saving Quarterly Report to ", reports_zip
    for title, text in quarterly_reports(quarter, year, account_grouping=account_grouping).iteritems():
        quarterly.writestr(title.replace(" ", "_") + ".tsv", text)
