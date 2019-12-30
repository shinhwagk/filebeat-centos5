import lib
import unittest

# message = """aaa
# bbb"""

# message = message.replace('\n', '\\n')

# body = agent.elasticDocumentTemplate("2019-12-30T08:03:36.902Z",   message)
# print body

# agent.httpPost(body)


# x = agent.logTimestampToElasticTimestamp("Mon Feb 09 16:28:55 CST 2019")
# print x


class TestSequenceFunctions(unittest.TestCase):

    # def setUp(self):
    #     lib.parserArgs([])

    # def testshuffle(self):
    #     random.shuffle(self.seq)
    #     self.seq.sort()
    #     self.assertEqual(self.seq, range(10))

    # def testchoice(self):
    #     element = random.choice(self.seq)
    #     self.assert_(element in self.seq)
    def testElasticDocumentTemplate(self):
        document = lib.elasticDocumentTemplate(
            '20190102T00:00:00.000Z1', "aaa")
        print document

    def testElasticTimestampTemplate(self):
        timestamp = lib.elasticTimestampTemplate(2019, 1, 2, 8, 0, 0)
        self.assertEqual(timestamp, '20190102T00:00:00.000Z')


if __name__ == '__main__':
    unittest.main()
