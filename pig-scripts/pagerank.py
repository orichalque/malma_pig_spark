#!/usr/bin/python
# *-* coding:utf-8 *-* 
from org.apache.pig.scripting import *

UPDATE2 = Pig.compile("""
-- PR(A) = (1-d) + d (PR(T1)/C(T1) + ... + PR(Tn)/C(Tn))
previous_pagerank = 
    LOAD '$docs_in'
    USING PigStorage(' ') 
    AS ( url: chararray, pagerank: float, links:{ link:tuple(url: chararray) } );

outbound_pagerank = 
    FOREACH previous_pagerank 
    GENERATE 
        pagerank / COUNT ( links ) AS pagerank, 
        FLATTEN ( links ) AS to_url;

new_pagerank = 
	FOREACH ( COGROUP outbound_pagerank BY to_url, previous_pagerank BY url INNER )
	GENERATE 
		group AS url, 
        	(1-$d)+$d*SUM(outbound_pagerank.pagerank) AS pagerank, 
        	FLATTEN ( previous_pagerank.links ) AS links,
		FLATTEN ( previous_pagerank.pagerank ) as previous_pagerank; 

store new_pagerank into '$docs_out';
nonulls           = filter new_pagerank by previous_pagerank is not null and
                        pagerank is not null;
pagerank_diff     = foreach nonulls generate ABS (previous_pagerank - pagerank);
grpall            = group pagerank_diff all;
max_diff          = foreach grpall generate MAX (pagerank_diff);
store max_diff into '$max_diff';
""")

params = { 'd': '0.8', 'docs_in': '../data/etl-xaaCC-MAIN-20160924173739-00000-ip-10-143-35-109.ec2.internal.warc.txt' }

##'../data/test.data'
##'../data/etl-xaaCC-MAIN-20160924173739-00000-ip-10-143-35-109.ec2.internal.warc.txt'

for i in range(1):
	out = "out/pagerank_data_" + str(i + 1)
	max_diff = "out/max_diff_" + str(i + 1)
	params["docs_out"] = out
	params["max_diff"] = max_diff
	Pig.fs("rmr " + out)
	Pig.fs("rmr " + max_diff)
	bound = UPDATE2.bind(params)
	stats = bound.runSingle()
	if not stats.isSuccessful():
        	raise 'failed'
    	##mdv = float(str(stats.result("max_diff").iterator().next().get(0)))
    	##print "max_diff_value = " + str(mdv)
    	##if mdv < 0.01:
        ##	print "done at iteration " + str(i)
	params["docs_in"] = out

