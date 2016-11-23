previous_pagerank = 
    LOAD '../data/etl-xaaCC-MAIN-20160924173739-00000-ip-10-143-35-109.ec2.internal.warc.txt' 
    USING PigStorage('\t') 
    AS ( url: chararray, pagerank: float, links:{ link:tuple(url: chararray) } );

outbound_pagerank = 
    FOREACH previous_pagerank 
    GENERATE 
        pagerank / (COUNT ( links ) == null ? 0 : COUNT(links)) AS pagerank, 
        FLATTEN ( links ) AS to_url;

new_pagerank = 
	FOREACH ( COGROUP outbound_pagerank BY to_url, previous_pagerank BY url INNER )
	GENERATE 
		group AS url, 
        	0.5+0.5*SUM(outbound_pagerank.pagerank) AS pagerank, 
        	FLATTEN ( previous_pagerank.links ) AS links;
        
STORE new_pagerank 
    INTO './pagerank-output' 
    USING PigStorage('\t');

