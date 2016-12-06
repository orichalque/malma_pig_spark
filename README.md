## Project for LSDM (Large Scale Data Management)

- xaaCC-MAIN-20160924173739-00000-ip-10-143-35-109.ec2.internal.warc.tar.gz is a compressed file of a 100MB part of the first file downloaded from the commoncrawl 2016 

- etl-xaaCC-MAIN-20160924173739-00000-ip-10-143-35-109.ec2.internal.warc.txt.zip is a compressed file translated with the etl-script

- The Etl script works with python3.3 and bs4 module ( ```bash sudo pip3 install bs4``` )

- pig-script works with the format of etl-*.warc.txt file ( ```bash pig -x local -embedded jython pagerank.py``` )


## Split a file into 18 pieces 
```bash
split --additional-suffix=CC-MAIN-20160924173740-00280-ip-10-143-35-109.ec2.internal.warc --number=l/18 CC-MAIN-20160924173740-00280-ip-10-143-35-109.ec2.internal.warc
```

