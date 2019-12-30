import agent


message = """aaa
bbb"""

message = message.replace('\n', '\\n')

body = agent.elasticDocumentTemplate("2019-12-30T08:03:36.902Z",   message)
print body

# agent.httpPost(body)


# x = agent.logTimestampToElasticTimestamp("Mon Feb 09 16:28:55 CST 2019")
# print x
