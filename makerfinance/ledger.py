#!/usr/bin/python
# -*- coding: latin-1 -*-

from collections import namedtuple
from csv import DictWriter
import hashlib
from itertools import groupby
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
import pickle
from pprint import pformat
import uuid
from collections import OrderedDict

import dateutil.parser
import boto

__author__ = 'andriod'

MembershipPlan = namedtuple("MembershipPlan", "rate period")
MONTH = 30.4375
SEMESTER = "Semester"
INCOME = "Income"
EXPENSE = "Expense"
INK_CARD = "Ink Card"
FOUNDERS_LOAN = "Founder's Loan"
PRIMARY_CHECKING = "Primary Checking"
CASH_BOX = "Cash Box"

tax_rate = Decimal(0.070)
agent_account_types = [INCOME, EXPENSE, FOUNDERS_LOAN, INK_CARD]

checksum_fields = ['amount', 'agent', 'agent_id', 'counter_party', 'counter_party_id', 'bank_account', 'external',
                   'effective_date', 'effective_until', 'plan', 'transfer_id', 'bank_id', 'test', 'type',
                   'tax_inclusive', 'entered']

membership_plans = {
    "Individual": MembershipPlan(100.0, MONTH),
    "Dependant": MembershipPlan(50.0, MONTH),
    "Class Only": MembershipPlan(25.0, MONTH),
    "Student": MembershipPlan(100.0, SEMESTER),
    "Null": MembershipPlan(0.0, 0),
    }

#class Account(object):
#    def __init__(self, ledger, name):
#        self.ledger = ledger
#        self.name = name
#
#    def balance(self, date):
#        query = "select * from {domain} where account == {name} and date<{date}".format(domain=domain.name,
#            name=self.name, date=encode(date))
#        rs = ledger.select(query)
#        ret = sum(decode(transaction['amount']) for transaction in rs)
#        return ret

def adjust_total(total, subtotal, adjustment):
    """
    Adjust the total on a partially reimbursed receipt by subtracting non-reimbursed expenses and proportionately
    adjusting tax and shipping
    """
    return total*(float(subtotal-adjustment)/subtotal)

def delete_test_data():
    for item in domain:
        if item['test']:
            domain.delete_item(item)

def member_list(date=None):
    if date is None:
        date = datetime.now()
    query = u"select counter_party, plan, effective_until from {domain} where plan > '' and subtype='Dues' and effective_until>'{date}' and effective_date<'{date}' ".format(
        domain=domain.name, date=encode(date))
    query += "order by plan"

    rs = domain.select(query)
    for member_num, member in enumerate(rs):
        print member_num+1, member['counter_party'], member['plan'], member['effective_until']

def member_report(date=None):
    ret = []
    query = u"select counter_party, counter_party_id, plan, effective_date, effective_until from {domain} where subtype='Dues' and counter_party_id > ''".format(
        domain=domain.name)
    query += "order by counter_party_id"

    rs = domain.select(query)
    for member_id,dues in groupby(rs,lambda result: result['counter_party_id']):
        dues = sorted(dues,key=lambda trans:trans['effective_date'])

        last = None
        for pmt in dues:
            if last is None:
                effective = pmt['effective_date']
                last = pmt
            elif last['effective_until'] != pmt['effective_date'] or pmt['plan'] != last['plan']:
                ret.append((member_id,last['counter_party'], last['plan'], effective, last['effective_until']))
                last = None
            else:
                last = pmt
        if last is not None:
            ret.append((member_id,last['counter_party'], last['plan'], effective, last['effective_until']))
    return ret

def is_member(member,date, plan = None):
    query = u"select effective_until, plan from {domain} where counter_party = '{member}' and subtype='Dues' and effective_until>='{date}' and effective_date<='{date} '".format(
        domain=domain.name, member=member, date=encode(date))
    if plan is not None:
        query += "and plan='{plan} '".format(plan=plan)
    rs = domain.select(query)
    try:
        rs.next()
        return True
    except StopIteration:
        return False

def dues(members, collector, amount=100.0, plan=None, bank_id="Cash", effective_date=None, date=None, test="",
         prorated=False, rounded=False, fees=(), **kwargs):
    other_members = []
    dues_amount = amount - sum(fee[0] for fee in fees)
    if isinstance(members, (tuple, list)):
        primary_member = members[0]
        other_members = members[1:]
    else:
        primary_member = members
    if effective_date is None:
        query = "select effective_until, plan from {domain} where counter_party = '{primary_member}' and subtype='Dues'".format(
            domain=domain.name, primary_member=primary_member.replace("'","''"))
        if plan is not None:
            query += "and plan='{plan}'".format(plan=plan)
        rs = domain.select(query)
        try:
            lastDues = max((result for result in rs), key=lambda result: decode(result['effective_until']))
            effective_date, plan = decode(lastDues['effective_until']), lastDues['plan']
            plan = str(plan)
        except ValueError:
            if date is not None:
                effective_date = date
            else:
                raise
    if date is None:
        date = effective_date.date()
    dependants = len(other_members)

    dependantPlan = membership_plans["Dependant" if dependants else "Null"]
    effectivePlan = membership_plans[plan]

    effectiveRate = effectivePlan.rate + dependants * dependantPlan.rate
    if not(prorated or rounded):
        assert dues_amount%effectiveRate == 0,"Inexact dues, you must specify prorated=True or rounded=True"

    if effectivePlan.period == SEMESTER:
        if effective_date.month <= 5:
            effective_until = effective_date.replace(month=8, day=31)
        elif effective_date.month <= 8:
            effective_until = effective_date.replace(month=12, day=31)
        else: #if effective_date.month <= 12:
            effective_until = effective_date.replace(year=effective_date.year + 1, month=5, day=31)
    else:
        if rounded:
            effective_until = effective_date + timedelta(days=effectivePlan.period * round(dues_amount / effectiveRate,0))
        else:
            effective_until = effective_date + timedelta(days=effectivePlan.period * dues_amount / effectiveRate)
    add(counter_party=primary_member, agent=collector, amount=amount, subtype="Dues", bank_id=bank_id,
        effective_date=effective_date, effective_until=effective_until, plan=plan, test=test,date=date,
        budget_account="Dues:"+plan,fees=fees,**kwargs)
    for dependant in other_members:
        raise NotImplementedError("Family membership needs to be revised/fixed")
#        add(counter_party=dependant, agent=collector, amount=dues_amount, subtype="Dues", bank_id=bank_id,
#            effective_date=effective_date, effective_until=effective_until, plan="Dependant",
#            primary_member=primary_member, test=test, **kwargs)

def bank_transfer(amount, from_account, to_account, agent, bank=True,subtype=None, date=None, **kwargs):
    transfer(amount, {'bank_account':from_account}, {'bank_account':to_account}, agent, bank, subtype, date, **kwargs)

def transfer(amount, from_, to, agent, bank=True, subtype=None, date=None, **base_details):
    transfer_id = mk_id()
    if not subtype:
        if bank:
            subtype = "Bank Transfer"
        else:
            subtype = "Budget Transfer"

    assert from_.keys() == to.keys(), "You must specify both from and to values for all changed fields in your transfer"

    if bank:
        assert from_.keys() == ["bank_account"], "Bank transfers can only change bank_account"
    else:
        assert "bank_account" not in from_, "bank_account cannot be changed by non-bank transfers"
        base_details["bank_account"] = "Budget Transfer"
        base_details["bank_id"] = None

    if date is None:
        date = datetime.now()

    fromDetails = dict(base_details)
    fromDetails.update(from_)
    toDetails = dict(base_details)
    toDetails.update(to)
    add(amount, agent, date=date, external=False, transfer_id=transfer_id, subtype=subtype, **toDetails)
    add(-amount, agent, date=date, external=False, transfer_id=transfer_id, subtype=subtype, **fromDetails)

entity_cache={None:None}
def get_entity_id(entity_name):
    if entity_name not in entity_cache:
        query = u"select counter_party, counter_party_id, agent, agent_id from {domain} where counter_party = '{name}' or agent = '{name}'"
        query = query.format(name = entity_name.replace("'","''"),domain=domain.name)
        rs = list(domain.select(query))
        if not rs:
            entity_cache[entity_name]=mk_id()
        else:
            assert len(rs) == 1, "Duplicate ids for entity {name}, resolve before adding new transactions for {name}:\n{details}".format(name=entity_name,details=rs)
            if rs[0]['counter_party'] == entity_name:
                entity_cache[entity_name] =  rs[0]['counter_party_id']
            elif rs[0]['agent'] == entity_name:
                entity_cache[entity_name] = rs[0]['agent_id']
            else:
                raise AssertionError("Unexpected mismatch in entity name {name} not found in  search results:{details}".format(name=entity_name,details=rs))
    return entity_cache[entity_name]


def dump_entity_cache():
    return "\n".join("%s - %s"%(name, id) for name, id in sorted(entity_cache.iteritems()) )


def mk_id():
    return uuid.uuid4().hex


def add(amount, agent, subtype, counter_party=None, event=None, bank_id="Cash", bank_account=None,
        external=True, date=None, effective_date=None, budget_account=None,
        test="", income=None, notes="", tax_inclusive=0, fees=(), state = "New", **other_fields):
    if counter_party is None and external:
        if event is None:
            raise TypeError("Either event or counter_party must be specified for external transactions")
        counter_party = "Event:" + event
    if income is None:
        income = amount > 0
    if date is None:
        date = datetime.now()
    elif isinstance(date, str):
        date = dateutil.parser.parse(date)
    if effective_date is None:
        effective_date = date
    elif isinstance(effective_date, str):
        effective_date = dateutil.parser.parse(effective_date)
    if bank_account is None:
        if income:
            if bank_id == "Cash":
                bank_account = CASH_BOX
            else:
                bank_account = PRIMARY_CHECKING
        else:
            bank_account = EXPENSE
    if bank_account in agent_account_types:
        bank_account = ":".join((agent, bank_account))

    subtype = subtype.title()

    if budget_account is None:
        budget_account = [subtype]
    elif isinstance(budget_account,str):
        budget_account = [budget_account]

    if event:
        budget_account.append(event)
    budget_account = ":".join(budget_account)

    # for now
    other_fields['tax_inclusive'] = tax_inclusive

    item = domain.new_item(mk_id())

    item['amount'] = encode(Decimal(amount))
    item['agent'] = encode(agent)
    item['agent_id'] = get_entity_id(agent)
    item['counter_party'] = encode(counter_party)
    item['counter_party_id'] = get_entity_id(counter_party)
    if event:
        item['event'] = event
    item['bank_account'] = encode(bank_account)
    item['budget_account'] = encode(budget_account)
    item['external'] = encode(external)
    item['date'] = encode(date)
    item['effective_date'] = encode(effective_date)
    item['entered'] = encode(datetime.now())
    item['modified'] = encode(datetime.now())
    item['type'] = encode("Income" if income else "Expense")
    item['subtype'] = encode(subtype)
    item['bank_id'] = encode(bank_id)
    item['notes'] = encode(notes)
    item['test'] = encode(test)
    item['state'] = encode(state)

    for key, value in other_fields.iteritems():
        item[key] = encode(value)

    item['posted'] = ""

    item.save()

    for fee_amount, fee_cpty in fees:
        add(-fee_amount, agent, subtype="Fees", counter_party=fee_cpty, event=event,
            bank_id=bank_id, bank_account=bank_account,
            external=True, date=date, test=test, income=False,
            fee_for=item.name, **other_fields)

    return item.name


def list_transactions():
    for item in domain:
        print format_entry(item)


def _mk_balance_group(depth, group,effective):
    date = "effective_date" if effective else "date"
    if group == "month":
        column = date
        l = lambda result: decode(result[date]).month
    else:
        column = group
        l = (lambda result: ":".join(result[group].split(":")[0:depth])) if depth else (lambda result: result[group])
    return column, l


def balances(group_by='bank_account',depth=0,effective=False):
    ret = OrderedDict()
    if not isinstance(group_by,(tuple,list)):
        group_by= [group_by,]

    columns = []
    lambdas = []
    for group in group_by:
        column, l = _mk_balance_group(depth, group, effective)
        columns.append(column)
        lambdas.append(l)

    query = "select * from {domain} where {wheres}  order by {group_by}".format(domain=domain.name,wheres = " and ".join(x+" is not null" for x in columns),group_by=columns[0])
    rs = domain.select(query)
    keyfunc = lambda result: tuple(l(result) for l in lambdas)
    for group_name, transactions in groupby(sorted(rs,key=keyfunc), keyfunc):
        ret[group_name] = sum(decode(transaction['amount']) for transaction in transactions)
    return ret


def tax():
    query = "select tax_inclusive from {domain} where tax_inclusive is not null".format(domain=domain.name)
    rs = domain.select(query)
    ret = sum(decode(transaction['tax_inclusive']) for transaction in rs)
    return (ret * tax_rate) / (1 + tax_rate)


def calculate_checksum(entry):
    try:
        return hashlib.sha256(",".join(encode(entry[item]) for item in checksum_fields if item in entry)).hexdigest()
    except:
        print "Failed to checksum", entry
        raise


def _get_entry(entry_or_id, consistent_read=False):
    if isinstance(entry_or_id, basestring):
        entry = domain.get_item(entry_or_id, consistent_read=consistent_read)
    else:
        entry = entry_or_id
    return entry


def set_state(entry_or_id, state):
    """
    Sets the state of the given transaction, managing any details as needed.
    """
    if state == "Posted":
        raise TypeError("set_state cannot set state to 'Posted' use post_transactions instead.")
    entry = _get_entry(entry_or_id, consistent_read=True)
    entry['state'] = state
    entry['modified'] = encode(datetime.now())
    entry.save()

def update_state(transactions, from_state, to_state):
    """
    Updates all transactions in from_state to to_state, returns list of updated transactions.
    """
    updated = []
    for entry_or_id in transactions:
        entry = _get_entry(entry_or_id, True)
        if entry['state'] != from_state:
            continue
        set_state(entry,to_state)
        updated.append(entry)
    return updated

def post_transactions(transactions):
    toPost = []
    postingTime = encode(datetime.now())

    # final check, transactions already posted or in the "Hold" state may not be posted
    for transaction in transactions:
        entry = _get_entry(transaction)
        if entry['posted']:
            continue
        if entry['state'] == "Hold":
            continue
        toPost.append(entry)

    agentKey = lambda item: item["agent"]
    toPost.sort(key=agentKey)

    for entry in toPost:
        entry['checksum'] = calculate_checksum(entry)
        entry['checksum_version'] = 1
        entry['posted'] = postingTime
        entry['state'] = "Posted"

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

    for entry in toPost:
        entry.save(replace=True)

    return postingTime


def make_posting_report(entries):
    entries = list(entries)
    lines = []
    for entry in entries:
        lines.append(format_entry(entry))

    binary = pickle.dumps([(entry.name, dict(entry)) for entry in entries])
    return "\n".join(lines), binary


def format_entry(entry, verbose=False):
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


def encode(to_encode):
    if hasattr(to_encode, "isoformat"):        
        if isinstance(to_encode,datetime):
            # if this is really a date
            if to_encode.hour == 0 and to_encode.minute == 0 and to_encode.second ==0:
                to_encode = to_encode.date()
            else:
                return to_encode.replace(microsecond=0).isoformat()
        return to_encode.isoformat()
    elif isinstance(to_encode, bool):
        return unicode(to_encode)
    try:
        return "%.2f" % to_encode
    except TypeError:
        return unicode(to_encode)


def decode(string):
    if string == "False":
        return False
    if not len(string):
        return string
    try:
        return Decimal(string)
    except InvalidOperation:
        try:
            return dateutil.parser.parse(string)
        except ValueError:
            return string


def check_pickle(filename):
    entries = pickle.load(open(filename, 'rb'))
    for name, entry in entries:
        ledger_entry = domain.get_item(name)
        assert entry['checksum'] == calculate_checksum(entry)
        assert entry['checksum'] == ledger_entry['checksum']
        assert ledger_entry['checksum'] == calculate_checksum(ledger_entry)


def connect_config_ledger(config):
    global domain, test
    aws_access_key_id = config.get('auth','aws_access_key_id')
    aws_secret_access_key = config.get('auth','aws_secret_access_key')
    domain_name = config.get('auth','domain_name')
    test = config.getboolean('auth','test') if config.has_option('auth','test') else False
    print aws_access_key_id, aws_secret_access_key, domain_name, test
    sdb = boto.connect_sdb(aws_access_key_id, aws_secret_access_key, debug=0)
    domain = sdb.create_domain(domain_name)
    return domain


def select(before, state=None):
    if state is None:
        return [entry for entry in domain if decode(entry["date"]) < before]
    return [entry for entry in domain if decode(entry["date"]) < before and entry['state'] == state]


#def post(before, state="Ready To Post"):
#    postingTime = post_transactions(select(before, state))
#    return postingTime
def add_class(amount_paid,student,agent,bank_account,bank_id,class_name,class_date,tax_inclusive=0,date_paid=None,test=False,
              fees=(), **other_fields):
    subtype="Class"

    class_name += class_date.strftime(":%B %d, %Y")
    if not is_member(student,class_date):
        dues_paid = membership_plans["Class Only"].rate
        assert amount_paid > dues_paid,"Paid only {paid} insufficient to cover dues of {dues}".format(paid=amount_paid, dues=dues_paid)
        amount_paid -= dues_paid
        dues(members=student,collector=agent,bank_account=bank_account,bank_id=bank_id, amount=dues_paid,
            effective_date=class_date,date=date_paid,plan="Class Only",test=test,event=class_name,**other_fields)
    add(amount_paid, agent, subtype, counter_party=student, event=class_name, bank_id=bank_id, bank_account=bank_account,
        date=date_paid, effective_date=class_date, test=test, tax_inclusive=tax_inclusive, fees=fees, **other_fields)

def list_fields(transactions = None):
    if transactions is None:
        transactions = domain
    return reduce(lambda x, y: x.union(y),[item.keys() for item in transactions], set())

def dump_to_csv(filename,transactions=None):
    if transactions is None:
        transactions = list(domain)
    all_fields = sorted(list_fields(transactions))
    writer = DictWriter(open(filename,"w"),fieldnames=all_fields)
    writer.writerow(dict((x,x) for x in all_fields))
    for item in transactions:
       writer.writerow(dict(((name, value.encode("utf-8")) for name,value in  item.iteritems())))
