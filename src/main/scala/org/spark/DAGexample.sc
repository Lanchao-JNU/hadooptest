import org.apache.spark.{SparkConf, SparkContext}

val conf = new SparkConf()
conf.setAppName("Wow,my first spark app")
conf.setMaster("local")
val sc = new SparkContext(conf)

val lines = sc.textFile()