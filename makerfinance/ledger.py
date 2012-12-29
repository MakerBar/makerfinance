#!/usr/bin/python
# -*- coding: latin-1 -*-

from collections import namedtuple, defaultdict
from csv import DictWriter
import csv
import hashlib
from itertools import groupby
from datetime import datetime, timedelta
from decimal import Decimal
import pickle
from collections import OrderedDict

import dateutil.parser
import boto
from makerfinance.reports import make_posting_reports
from makerfinance.util import encode, decode, mk_id

__author__ = 'andriod'

MembershipPlan = namedtuple("MembershipPlan", "rate period")
MONTH = 30.4375

SEMESTER = "Semester"
INCOME = "Income"
EXPENSE = "Expense"
FOUNDERS_LOAN = "Founder's Loan"
PRIMARY_CHECKING = "Primary Checking"
CASH_BOX = "Cash Box"


class Ledger(object):
    """
    Hold the records of a ledger or journal

    The ledger may be the organization's general ledger or a sales
    journal or other special ledger.
    """
    # Eventually configurable
    checksum_fields = ['amount', 'agent', 'agent_id', 'counter_party', 'counter_party_id', 'bank_account', 'external',
                       'effective_date', 'effective_until', 'plan', 'transfer_id', 'bank_id', 'test', 'type',
                       'tax_inclusive', 'entered', 'subtype']
    tax_rate = Decimal(0.070)

    #When an agent account type is given, the actual account is  <agent>:<account>
    agent_account_types = [INCOME, EXPENSE, FOUNDERS_LOAN]

    membership_plans = {
        "Individual": MembershipPlan(100.0, MONTH),
        "Dependant": MembershipPlan(50.0, MONTH),
        "Class Only": MembershipPlan(25.0, MONTH),
        "Student": MembershipPlan(100.0, SEMESTER),
        "Null": MembershipPlan(0.0, 0),
    }

    def __init__(self, domain, cache=False):
        self.domain = domain
        self.entity_cache = {None: None}
        if isinstance(cache, dict):
            self.cache = cache
        elif cache:
            self.cache = defaultdict(OrderedDict)
        else:
            self.cache = None

    def __iter__(self):
        for item in self.domain:
            yield item

    def _get_entry(self, entry_or_id, consistent_read=False):
        if isinstance(entry_or_id, basestring):
            entry = self.domain.get_item(entry_or_id, consistent_read=consistent_read)
        else:
            entry = entry_or_id
        return entry

    def balance(self, where=""):
        if where:
            query = "select * from {domain} where {wheres}".format(domain=self.domain.name, wheres=where)
        else:
            query = "select * from {domain}".format(domain=self.domain.name)

        rs = self._select(query)
        return sum(decode(transaction['amount']) for transaction in rs)

    def find_transactions(self, **conditions):
        where = " and ".join("%s = '%s'" % (field, encode(value)) for field, value in conditions.iteritems())
        query = "select * from {domain} where {wheres}".format(domain=self.domain.name, wheres=where)
        rs = self._select(query)
        return list(rs)

    def find_transaction(self, **conditions):
        transactions = self.find_transactions(**conditions)

        if len(transactions) == 1:
            return transactions[0]
        if len(transactions) == 0:
            return None
        raise AssertionError("find_transaction found multiple transactions, only one is allowed")


    def delete_test_data(self):
        """
        Delete all data with the test flag set
        """
        for item in self:
            if item['test']:
                self.domain.delete_item(item)

    def is_member(self, member, date, plan=None):
        query = u"select effective_until, plan from {domain} where counter_party = '{member}' and subtype='Dues' and effective_until>='{date}' and effective_date<='{date} '".format(
            domain=self.domain.name, member=member, date=encode(date))
        if plan is not None:
            query += "and plan='{plan} '".format(plan=plan)
        rs = self._select(query)
        try:
            rs.next()
            return True
        except StopIteration:
            return False

    def _select(self, query):
        if self.cache and (query,) in self.cache['select']:
            return self.cache['select'][(query,)]
        rs = self.domain.select(query)
        if self.cache is not None:
            self.cache['select'][(query,)] = list(rs)
            return self.cache['select'][(query,)]
        return rs

    def select_non_empty(self, columns, external=None, effective_date_after=None, effective_date_before=None, where=''):
        """
        Select transactions where the given columns are not empty
        will be sorted by the first column
        :param columns: columns that must be non-null
        :param where: existing where clause if any
        :return: list of transactions
        """
        if where:
            where += " and "

        if external is not None:
            where += "and external='%s'" % external

        if effective_date_after is not None:
            where += "and effective_date >= {date}".format(date=encode(effective_date_after))

        if effective_date_before is not None:
            where += "and effective_date < {date}".format(date=encode(effective_date_after))

        where += " and ".join(x + " is not null" for x in columns)
            #    for filter, value in filter_by.iteritems():
        #        if isinstance(value,(tuple,list)):
        #            where += "and {field} in ({values})".format(field= filter, values = ",".join('{value}'.format(value = x) for x in value))
        #        else:
        #            where += "and {field} in ({values})".format(field= filter, values = ",".join('{value}'.format(value = x) for x in value))
        query = "select * from {domain} where {wheres}  order by {group_by}".format(domain=self.domain.name,
            wheres=where, group_by=columns[0])
        rs = self._select(query)
        return rs

    def select_taxable_transactions(self, start=None, end=None):
        query = """select amount, tax_inclusive from {domain}
            where type = 'Income'
                and external = 'True'""".format(domain=self.domain.name)
        if start is not None:
            query += """and date between '{start}' and '{end}'""".format(domain=self.domain.name, start=encode(start),
            end=encode(end, epsilon=True))
        rs = self._select(query)
        return rs

    def select_member_dues(self, member, plan):
        query = "select effective_until, plan from {domain} where counter_party = '{member}' and subtype='Dues'".format(
            domain=domain.name, member=member.replace("'", "''"))
        if plan is not None:
            query += "and plan='{plan}'".format(plan=plan)
        rs = domain.select(query, consistent_read=True)
        return rs

    def select_all_dues(self):
        query = u"""select counter_party, counter_party_id, plan,date, bank_id, bank_account, effective_date, effective_until
                    from {domain} where subtype='Dues' and counter_party_id > ''""".format(
            domain=self.domain.name)
        query += "order by counter_party_id"
        rs = self._select(query)
        return rs

    def member_list(self):
        """
        List of all members current and past by member id, name, plan, and effective dates
        """
        ret = []

        for member_id, dues in groupby(self.select_all_dues(),
            lambda result: result['counter_party_id']):
            dues = sorted(dues, key=lambda trans: trans['effective_date'])

            last = None
            for pmt in dues:
                if last is None:
                    effective = pmt['effective_date']
                elif last['effective_until'] != pmt['effective_date'] or pmt['plan'] != last['plan']:
                    ret.append((
                        member_id, last['counter_party'], last['plan'], last['date'], last['bank_id'],
                        last['bank_account'], effective,
                        last['effective_until']))
                    effective = pmt['effective_date']
                last = pmt
            if last is not None:
                ret.append(
                    (
                    member_id, last['counter_party'], last['plan'], last['date'], last['bank_id'], last['bank_account'],
                    effective, last['effective_until']))
        return ret

    def tax(self, where=""):
        rs = self.select_taxable_transactions()
        ret = sum(decode(transaction['tax_inclusive']) for transaction in rs)
        return (ret * self.tax_rate) / (1 + self.tax_rate)

    def list_fields(self, transactions=None):
        if transactions is None:
            transactions = self.domain
        return reduce(lambda x, y: x.union(y), [item.keys() for item in transactions], set())

    def dump_to_csv(self, filename, transactions=None):
        if transactions is None:
            transactions = list(self)
        all_fields = sorted(self.list_fields(transactions))
        writer = DictWriter(open(filename, "w"), fieldnames=all_fields, quoting=csv.QUOTE_ALL)
        writer.writerow(dict((x, x) for x in all_fields))
        for item in transactions:
            writer.writerow(dict(((name, unicode(value).encode("utf-8")) for name, value in item.iteritems())))

    def get_transaction(self, transaction_id):
        return self.domain.get_item(transaction_id)

    def check_pickle(self, filename_or_file):
        try:
            entries = pickle.load(open(filename_or_file, 'rb'))
        except:
            entries = pickle.load(filename_or_file)

        for name, entry in entries:
            ledger_entry = self.get_transaction(name)
            assert entry['checksum'] == self.calculate_checksum(entry)
            assert entry['checksum'] == ledger_entry['checksum']
            assert ledger_entry['checksum'] == self.calculate_checksum(ledger_entry)

    def _create_item(self):
        item = self.domain.new_item(mk_id())
        return item

    def _save_item(self, item):
        item.save()

    def add(self, amount, agent, subtype, counter_party=None, event=None, bank_id="Cash", bank_account=None,
            external=True, date=None, effective_date=None, budget_account=None,
            test="", income=None, notes="", tax_inclusive=0, fees=(), state="New", append_event=True, **other_fields):
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
        if bank_account in self.agent_account_types:
            bank_account = ":".join((agent, bank_account))

        subtype = subtype.title()

        if budget_account is None:
            budget_account = [subtype]
        elif isinstance(budget_account, str):
            budget_account = [budget_account]

        if event and not append_event:
            budget_account.append(event)
        budget_account = ":".join(budget_account)

        if tax_inclusive is True:
            tax_inclusive = amount
            # for now
        other_fields['tax_inclusive'] = tax_inclusive
        if tax_inclusive > 0:
            assert external and income, "Tax may only be collected on external sales"

        item = self._create_item()

        item['amount'] = encode(Decimal(amount))
        item['agent'] = encode(agent)
        item['agent_id'] = self.get_entity_id(agent)
        item['counter_party'] = encode(counter_party)
        item['counter_party_id'] = self.get_entity_id(counter_party)
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
        item['bank_id'] = unicode(bank_id)
        item['notes'] = encode(notes)
        item['test'] = encode(test)
        item['state'] = encode(state)

        for key, value in other_fields.iteritems():
            item[key] = encode(value)

        item['posted'] = ""

        #assert amount != 0, "You must be saving a transaction with some amount to it."
        self._save_item(item)

        other_fields.pop("tax_inclusive") #fees are not tax inclusive
        for fee in fees:
            if len(fee) == 3:
                fee_amount, fee_cpty, bank_id = fee
            else:
                fee_amount, fee_cpty = fee
            self.add(-fee_amount, agent, subtype="Fees:" + fee_cpty, counter_party=fee_cpty, event=event,
                bank_id=bank_id, bank_account=bank_account,
                external=True, date=date, test=test, income=False,
                fee_for=item.name, **other_fields)

    def transfer(self, amount, from_, to, agent, bank=True, subtype=None, date=None, **base_details):
        transfer_id = mk_id()
        if not subtype:
            if bank:
                subtype = "Bank Transfer"
            else:
                subtype = "Budget Transfer"

        assert from_.keys() == to.keys(), "You must specify both from and to values for all changed fields in your transfer"

        if bank:
            assert from_.keys() == ["bank_account"] or set(from_.keys()) == {"bank_account", "bank_id"}, "Bank transfers can only change bank_account, found changes to: " + str(
                from_.keys())
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
        self.add(amount, agent, date=date, external=False, transfer_id=transfer_id, subtype=subtype, **toDetails)
        self.add(-amount, agent, date=date, external=False, transfer_id=transfer_id, subtype=subtype, **fromDetails)

    def bank_transfer(self, amount, from_account, to_account, agent, bank=True, subtype=None, date=None,
                      from_bank_id=None, to_bank_id=None, both_bank_id=None, **kwargs):
        assert 'bank_id' not in kwargs
        if both_bank_id is not None:
            to_bank_id = from_bank_id = both_bank_id
        self.transfer(amount, {'bank_account': from_account, 'bank_id': from_bank_id},
            {'bank_account': to_account, 'bank_id': to_bank_id}, agent, bank, subtype, date,
            **kwargs)

    def dues(self, members, collector, amount=100.0, plan=None, bank_id="Cash", effective_date=None, date=None, test="",
             prorated=False, rounded=False, fees=(), **kwargs):
        other_members = []
        dues_amount = amount - sum(fee[0] for fee in fees)
        if isinstance(members, (tuple, list)):
            primary_member = members[0]
            other_members = members[1:]
        else:
            primary_member = members
        if effective_date is None:
            rs = self.select_member_dues(primary_member, plan)
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

        dependantPlan = self.membership_plans["Dependant" if dependants else "Null"]
        effectivePlan = self.membership_plans[plan]

        effectiveRate = effectivePlan.rate + dependants * dependantPlan.rate
        if not(prorated or rounded):
            assert dues_amount % effectiveRate == 0, "Inexact dues, you must specify prorated=True or rounded=True"

        if effectivePlan.period == SEMESTER:
            if effective_date.month <= 5:
                effective_until = effective_date.replace(month=8, day=31)
            elif effective_date.month <= 8:
                effective_until = effective_date.replace(month=12, day=31)
            else: #if effective_date.month <= 12:
                effective_until = effective_date.replace(year=effective_date.year + 1, month=5, day=31)
        else:
            if rounded:
                effective_until = effective_date + timedelta(
                    days=effectivePlan.period * round(dues_amount / effectiveRate, 0))
            else:
                effective_until = effective_date + timedelta(days=effectivePlan.period * dues_amount / effectiveRate)
        if len(other_members):
            raise NotImplementedError("Family membership needs to be revised/fixed")
        self.add(counter_party=primary_member, agent=collector, amount=amount, subtype="Dues", bank_id=bank_id,
            effective_date=effective_date, effective_until=effective_until, plan=plan, test=test, date=date,
            budget_account="Dues:" + plan, fees=fees, append_event=False, **kwargs)

        #    for dependant in other_members:

    #        add(counter_party=dependant, agent=collector, amount=dues_amount, subtype="Dues", bank_id=bank_id,
    #            effective_date=effective_date, effective_until=effective_until, plan="Dependant",
    #            primary_member=primary_member, test=test, **kwargs)

    def add_class(self, amount_paid, student, agent, bank_account, bank_id, class_name, class_date, materials=0,
                  date_paid=None, test=False, membership_effective_date=None,
                  fees=(), **other_fields):
        class_name.replace(":", "|")
        class_name += class_date.strftime(":%B %d, %Y")
        if not self.is_member(student, class_date):
            dues_paid = self.membership_plans["Class Only"].rate
            assert amount_paid > dues_paid, "Paid only {paid} insufficient to cover dues of {dues}".format(
                paid=amount_paid, dues=dues_paid)
            amount_paid -= dues_paid
            if membership_effective_date is None:
                membership_effective_date = class_date
            self.dues(members=student, collector=agent, bank_account=bank_account, bank_id=bank_id, amount=dues_paid,
                effective_date=membership_effective_date, date=date_paid, plan="Class Only", test=test,
                event=class_name,
                **other_fields)

        self.add(amount_paid - materials, agent, "Class:Instruction", counter_party=student, event=class_name,
            bank_id=bank_id, bank_account=bank_account,
            date=date_paid, effective_date=class_date, test=test, fees=fees, **other_fields)
        if materials:
            self.add(materials, agent, "Class:Supplies", counter_party=student, event=class_name, bank_id=bank_id,
                bank_account=bank_account,
                date=date_paid, effective_date=class_date, test=test, tax_inclusive=materials, **other_fields)

    @staticmethod
    def _mk_balance_group(depth, group):
        if group in ("month", "effective_month"):
            column = "effective_date" if group == "effective_month" else "date"
            l = lambda result: decode(result[column]).replace(day=1, hour=0, minute=0, second=0, microsecond=0,
                tzinfo=None)
        elif group in ("day", "effective_day"):
            column = "effective_date" if group == "effective_month" else "date"
            l = lambda result: decode(result[column]).replace(hour=0, minute=0, second=0, microsecond=0,
                tzinfo=None)
        else:
            column = group
            l = (lambda result: ":".join(result[column].split(":")[0:depth])) if depth >= 0 else (
                lambda result: result[column])
        return column, l

    def balances(self, group_by='bank_account', depth=-1, external=None, effective_date_after=None, effective_date_before=None, where=""):
        """

        """
        ret = OrderedDict()
        if not isinstance(group_by, (tuple, list)):
            group_by = [group_by, ]

        #Create special Rounding functions in the same order as the group bys
        #These are needed to round dates down to months
        #also creates columns for the query below
        columns = []
        roundingFuncs = []
        for group in group_by:
            column, l = self._mk_balance_group(depth, group)
            columns.append(column)
            roundingFuncs.append(l)

        rs = self.select_non_empty(columns, effective_date_after=effective_date_after,
            effective_date_before=effective_date_before, external=external, where=where)

        # keyfunc for both sorting and grouping is to use the rounding functions
        keyfunc = lambda result: tuple(rnd(result) for rnd in roundingFuncs)
        for group_key, transactions in groupby(sorted(rs, key=keyfunc), keyfunc):
            total = sum(decode(transaction['amount']) for transaction in transactions)
            if total:
                if group_by[-1] == "bank_account":
                    net_key = group_key[0:-1]+("net",)
                    net = ret.get(net_key,0)
                    net += total
                    ret[net_key] = net
                    if not group_key[-1].endswith(FOUNDERS_LOAN):
                        net_key = group_key[0:-1]+("operating net",)
                        net = ret.get(net_key,0)
                        net += total
                        ret[net_key] = net


                ret[group_key] = total


        return ret


    def set_state(self, entry_or_id, state):
        """
        Sets the state of the given transaction, managing any details as needed.
        """
        if state == "Posted":
            raise TypeError("set_state cannot set state to 'Posted' use post_transactions instead.")
        entry = self._get_entry(entry_or_id, consistent_read=True)
        entry['state'] = state
        entry['modified'] = encode(datetime.now())
        entry.save()

    def update_state(self, transactions, from_state, to_state):
        """
        Updates all transactions in from_state to to_state, returns list of updated transactions.
        """
        updated = []
        for entry_or_id in transactions:
            entry = self._get_entry(entry_or_id, True)
            if entry['state'] != from_state:
                continue
            self.set_state(entry, to_state)
            updated.append(entry)
        return updated

    def calculate_checksum(self, entry):
        try:
            return hashlib.sha256(
                ",".join(encode(entry[item]) for item in self.checksum_fields if item in entry)).hexdigest()
        except:
            print "Failed to checksum", entry
            raise

    def post_transactions(self, transactions):
        toPost = []
        postingTime = encode(datetime.now())

        # final check, transactions already posted or in the "Hold" state may not be posted
        for transaction in transactions:
            entry = self._get_entry(transaction)
            if entry['posted']:
                continue
            if entry['state'] == "Hold":
                continue
            toPost.append(entry)

        for entry in toPost:
            entry['checksum'] = self.calculate_checksum(entry)
            entry['checksum_version'] = 1
            entry['posted'] = postingTime
            entry['state'] = "Posted"

        make_posting_reports(postingTime, toPost)

        for entry in toPost:
            entry.save(replace=True)

        return postingTime

    def select(self, before=None, external = None, state=None):
        if before is None:
            dateTest = lambda x:True
        else:
            dateTest = lambda entry: decode(entry["effective_date"]) < before or decode(
                entry["entered"]) < before or decode(entry["date"]) < before

        return [entry for entry in self if dateTest(entry) and
                                        (state is None or entry['state'] == state) and
                                        (external is None or entry['external'] == external)]

    def dump_entity_cache(self):
        return "\n".join("%s - %s" % (name, id) for name, id in sorted(self.entity_cache.iteritems()))

    def select_entity_by_name(self, entity_name):
        query = u"""select counter_party, counter_party_id, agent, agent_id
                        from {domain} where counter_party = '{name}' or agent = '{name}'"""
        query = query.format(name=entity_name.replace("'", "''"), domain=domain.name)
        rs = list(domain.select(query))
        return rs

    def get_entity_id(self, entity_name):
        if entity_name not in self.entity_cache:
            rs = self.select_entity_by_name(entity_name)
            if not rs:
                self.entity_cache[entity_name] = mk_id()
            else:
                assert len(
                    rs) == 1, "Duplicate ids for entity {name}, resolve before adding new transactions for {name}:\n{details}".format(
                    name=entity_name, details=rs)
                if rs[0]['counter_party'] == entity_name:
                    self.entity_cache[entity_name] = rs[0]['counter_party_id']
                elif rs[0]['agent'] == entity_name:
                    self.entity_cache[entity_name] = rs[0]['agent_id']
                else:
                    raise AssertionError(
                        "Unexpected mismatch in entity name {name} not found in  search results:{details}".format(
                            name=entity_name, details=rs))
        return self.entity_cache[entity_name]

class DictItem(dict):
    def __init__(self,**kwargs):
        super(DictItem, self).__init__(**kwargs)
        self.name = mk_id()

    def save(self,replace=None):
        pass

class DictLedger(Ledger):
    def __init__(self):
        self.storage = {}
        self.entity_cache = {None: None}

    def __iter__(self):
        for item in self.storage.itervalues():
            yield item

    def _create_item(self):
        return DictItem()

    def _save_item(self, item):
        self.storage[item.name] = item

    def select_non_empty(self, columns, external=None, effective_date_after=None, effective_date_before=None, where=""):
        """
        Select transactions where the given columns are not empty
        :param columns: columns that must be non-null
        :return: list of transactions
        """

        if where != '':
            raise NotImplementedError,"DictLedger does not support adhoc where clauses"

        return [transaction for transaction in self if all(column in transaction for column in columns)
                                and (external is None or bool(decode(transaction['external'])) == external)
                                and (effective_date_after is None or decode(transaction['effective_date']) >= effective_date_after)
                                and (effective_date_before is None or decode(transaction['effective_date']) < effective_date_before)
                                ]


    def select_all_dues(self):
        return [trans for trans in self if trans['subtype'] == 'Dues']

    def select_taxable_transactions(self, start=None, end=None):
        return [trans for trans in self if (start is None or end is None or start<decode(trans['date'])<=end) and trans['external'] and trans['type']=='Income']

    def select_entity_by_name(self, entity_name):
        entity_name = entity_name.replace("'", "''")
        return [trans  for trans in self if trans['counter_party'] == entity_name or trans['agent'] == entity_name]

    def select_member_dues(self, member, plan = None):
        member=member.replace("'", "''")
        if plan is None:
            return [trans for trans in self if trans['subtype'] == 'Dues' and trans['counter_party'] == member]
        return [trans for trans in self if trans['subtype'] == 'Dues' and trans['counter_party'] == member and plan == plan]

    def is_member(self, member, date, plan=None):
        if plan is None:
           return len([trans for trans in self if trans['subtype'] == 'Dues' and trans['counter_party'] == member and decode(trans['effective_date'])<=date<=decode(trans['effective_until'])])
        return len([trans for trans in self if trans['subtype'] == 'Dues' and trans['counter_party'] == member and decode(trans['effective_date'])<=date<=decode(trans['effective_until']) and trans['plan']==plan])

    def get_transaction(self, transaction_id):
        return self.storage[transaction_id]

def connect_config_ledger(config, cache=False):
    global domain, test
    aws_access_key_id = config.get('auth', 'aws_access_key_id')
    aws_secret_access_key = config.get('auth', 'aws_secret_access_key')
    domain_name = config.get('auth', 'domain_name')
    test = config.getboolean('auth', 'test') if config.has_option('auth', 'test') else False
    print aws_access_key_id, aws_secret_access_key, domain_name, test
    sdb = boto.connect_sdb(aws_access_key_id, aws_secret_access_key, debug=0)
    domain = sdb.create_domain(domain_name)
    return Ledger(domain, cache)


#def post(before, state="Ready To Post"):
#    postingTime = post_transactions(select(before, state))
#    return postingTime

