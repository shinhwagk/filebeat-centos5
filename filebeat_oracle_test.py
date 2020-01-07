import filebeat_oracle
import unittest


class ParametrizedTestCase(unittest.TestCase):
    def __init__(self, methodName='runTest', args=None):
        super(ParametrizedTestCase, self).__init__(methodName)
        self.args = args

    @staticmethod
    def parametrize(testcase_klass, args=None):
        testnames = unittest.TestLoader().getTestCaseNames(testcase_klass)
        _suite = unittest.TestSuite()
        for name in testnames:
            _suite.addTest(testcase_klass(name, args=args))
        return _suite


class TestOracleAlertLogToElastic(ParametrizedTestCase):

    # def setUp(self):

    def testlogTimestampToElasticTimestamp(self):
        year, mon, day, hour, minu, sec = filebeat_oracle.logTimestampToSingleDateNumbers(
            "Fri Jan 02 08:00:00 2019")
        ests = filebeat_oracle.elasticTimestampTemplate(
            year, mon, day, hour, minu, sec)
        self.assertEqual(ests, '20190102T00:00:00.000Z')

        # def testElasticDocumentTemplate(self):
        #     # document = lib.elasticDocumentTemplate(
        #     #     '20190102T00:00:00.000Z1', "aaa")
        #     # print document
        #     print {"a": 1} == {"a": 2}


if __name__ == '__main__':
    SUITE = unittest.TestSuite()
    SUITE.addTest(ParametrizedTestCase.parametrize(TestOracleAlertLogToElastic, args=['--oracleVersion', "10", "--alertFilePath",
                                                                                      "./log/10.log", "--elasticHost", "elastic", "--elasticPort", "9200", "--elasticIndex", "oracle_alert_log"]))
    SUITE.addTest(ParametrizedTestCase.parametrize(TestOracleAlertLogToElastic, args=['--oracleVersion', "11", "--alertFilePath",
                                                                                      "./log/11.log", "--elasticHost", "elastic", "--elasticPort", "9200", "--elasticIndex", "oracle_alert_log"]))
    unittest.TextTestRunner(verbosity=2).run(SUITE)
