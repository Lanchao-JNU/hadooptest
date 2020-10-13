import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

val conf = new SparkConf().setMaster("local[*]").setAppName("RDD_Movie_Users_Analyzer")
/**
 */
val spark = SparkSession.builder.config(conf).getOrCreate()
val sc = spark.sparkContext
sc.setLogLevel("warn")
import spark.implicits._



val pathIn = "/Users/zhanglanchao/Downloads/hadooptest/src/main/resources/data/users.parquet"

val b = 1
val a = "abc"+b+"bcd"

val people = spark.read.parquet(pathIn)
val ageCol = people("name")


object OnetoOneDependency {
  def main(args: Array[String]): Unit = {
    val num1 = Array(100, 80, 70)
    val rddnum1 = sc.parallelize(num1)
    val mapRdd = rddnum1.map(_ * 2)
    mapRdd.collect().foreach(println)
  }
}
OnetoOneDependency.main(Array())

object RangeDependency{
  def main(args:Array[String]): Unit ={
    val data1 = Array("spark","scala","hadoop")
    val data2 = Array("SPARK","SCALA","HADOOP")
    val rdd1 = sc.parallelize(data1)
    val rdd2 = sc.parallelize(data2)
    val unionRdd = rdd1.union(rdd2)
    unionRdd.collect().foreach(println)
  }
}
RangeDependency.main(Array())

object ShuffleDependency{
  def main(args:Array[String]): Unit ={
    val data = Array(Tuple2("spark",100),Tuple2("spark",95),
      Tuple2("hadoop",99),Tuple2("hadoop",80),Tuple2("hadoop",92))
    val rdd= sc.parallelize(data)
    val rddGroupKey = rdd.groupByKey()
    rddGroupKey.collect().foreach(println)
  }
}
ShuffleDependency.main(Array())
spark.stop()

