from collections import OrderedDict
from datetime import date, timedelta, time, datetime
from makerfinance.ledger import all_balances, encode

__author__ = 'andriod'

def format_account_balances(balances_by_account):
    quarterFlowReport = ""
    for (budget_account,), amount in balances_by_account.iteritems():
        quarterFlowReport += "{budget_account}  ${amount}\n".format(budget_account=budget_account, amount=amount)
    return quarterFlowReport


def quarterly_reports(quarter,year = None):
    ret = {}
    months = set()

    baseDate = date.today()
    if year is not None:
        baseDate = baseDate.replace(year = year)
    start = end = baseDate
    start = start.replace(month = (quarter-1) * 3+1,day = 1)
    end = datetime.combine(end.replace(month = quarter*3+1,day=1),time(0)) - timedelta(microseconds = 1)

    quarterWhere = "effective_date between '{start}' and '{end}'".format(start=encode(start),
        end=encode(end, epsilon=True))
    startWhere = "effective_date < '{start}'".format(start=encode(start),
        end=encode(end, epsilon=True))
    endWhere = "effective_date <= '{end}'".format(start=encode(start),
        end=encode(end, epsilon=True))


    startingBalances = all_balances(group_by=('budget_account'), where =startWhere)
    startingBalanceReport = format_account_balances(startingBalances)
    activeBudgetAccounts = set(x[0] for x in startingBalances.keys())

    endingBalances = all_balances(group_by=('budget_account'), where =endWhere)
    endingBalanceReport = format_account_balances(endingBalances)

    monthlyFlowReport = ""
    monthlyFlow = all_balances(group_by=('effective_month', 'budget_account'), where =quarterWhere)
    for (month, budget_account), amount in monthlyFlow.iteritems():
        monthlyFlowReport += "{month}   {budget_account}  ${amount}\n".format(month=month.strftime("%B %Y"),budget_account= budget_account, amount=amount)
        activeBudgetAccounts.add(budget_account)
        months.add(month)
    activeBudgetAccounts = sorted(activeBudgetAccounts)
    months = sorted(months)
    
    quarterFlow =  all_balances(group_by=('budget_account'), where =quarterWhere)
    quarterFlowReport = format_account_balances(quarterFlow)


    flowSummary = OrderedDict()
    for budgetAccount in activeBudgetAccounts:
        flowSummary[budgetAccount] = {"starting_balance":startingBalances.get((budgetAccount),0),
                                        "ending_balance":endingBalances.get((budgetAccount),0),
                                        "quarter_flow":quarterFlow.get((budgetAccount),0)}
        for month in months:
            flowSummary[budgetAccount][month] = monthlyFlow.get((month,budgetAccount),0)


    ret["Monthly Net Cash Flow"] = monthlyFlowReport
    ret["Quarter Net Cash Flow"] = quarterFlowReport
    ret["Starting Balances"] = startingBalanceReport
    ret["Ending Balances"] = endingBalanceReport

    return ret