import pickle
from makerfinance.ledger import Ledger
from makerfinance.reports import member_report


__author__ = 'andriod'

import unittest

class DummyDomain(object):
    def __init__(self,domain_name):
        self.name=domain_name


class MemberReportTest(unittest.TestCase):
    def test_member_report(self):
        dummyDomain=DummyDomain("makerbar_test_ledger")
        ledger = Ledger(dummyDomain, pickle.load(open("memberReport_cache.pkl")))
        report = member_report(ledger)
        print report
        self.assertEqual( report.count("Richard Jedrzejek"),2)
        self.assertEqual( report.count("Martha Garvey"),2)

if __name__ == '__main__':
    unittest.main()
