// Script to run one iteration of the pagerank algorithm in Spark with Scala
case class Row(url : String, pagerank: Double, urls: Array[String])

val rawData = sc.textFile("input/input_10")

val data = rawData.map(line => {
	val fields = line.split("\t")
	val urls = fields(2).substring(1, fields(2).length - 1)
	.split(",")
	Row(fields(0), fields(1).toDouble, urls)
})

val contributions = data.flatMap { case Row(url, pagerank, urls) => urls.map(url => (url, pagerank / urls.length)) }

val results = contributions.reduceByKey((x, y) => x + y).mapValues(v => 0.15 + 0.85*v)
