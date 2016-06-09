from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
from operator import add


links = sc.textFile('s3://hits-data-pagelinks/links/links-simple-sorted.txt')
links = links.map(lambda x: x.split(": "))
links = links.map(lambda x: [(x[0], i) for i in x[1].split(' ')])
links = links.flatMap(lambda x: x)
links = links.map(lambda x: (x[0], x[1]))

links = sqlContext.createDataFrame(links, ['from_page','to_page'])
sqlContext.registerDataFrameAsTable(links, "pagelinks")


hubsRDD = links.map(lambda x: x[0]).distinct()
hubsRDD = hubsRDD.map(lambda x: (x,1.0))
hubs = sqlContext.createDataFrame(hubsRDD, ['page','score'])
sqlContext.registerDataFrameAsTable(hubs, "hubs")

for i in range(0,8):
	auths = sqlContext.sql('SELECT pagelinks.to_page AS page, SUM(temp_hub.score) AS score FROM temp_hub JOIN pagelinks ON (temp_hub.page = pagelinks.from_page) GROUP BY pagelinks.to_page')
	hubs = sqlContext.sql('SELECT pagelinks.from_page AS page, SUM(temp_auth.score) AS score FROM temp_auth JOIN pagelinks ON (temp_auth.page = pagelinks.to_page) GROUP BY pagelinks.from_page')



