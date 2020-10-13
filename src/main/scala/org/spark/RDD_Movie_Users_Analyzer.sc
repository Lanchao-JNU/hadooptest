import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


val conf = new SparkConf().setMaster("local[*]").setAppName("RDD_Movie_Users_Analyzer")
/**
 */
val spark = SparkSession.builder.config(conf).getOrCreate()
val sc = spark.sparkContext
sc.setLogLevel("warn")

val datapath = "/Users/zhanglanchao/Downloads/hadooptest/src/main/resources/data/"
val usersRDD = sc.textFile(datapath+"users.dat")
val movieRDD = sc.textFile(datapath+"movies.dat")
val ratingRDD = sc.textFile(datapath+"ratings.dat")



println("The best movies:")
val movieInfo = movieRDD.map(_.split("::")).map(x=>(x(0),x(1))).cache()
val ratings = ratingRDD.map(_.split("::")).map(x=> (x(0),x(1),x(2))).cache()


val moviesAndRatings = ratings.map(x => (x._2, (x._3.toDouble, 1)))
  .reduceByKey((x,y)=>(x._1 + y._1,x._2 + y._2))

val avgRatings: RDD[(String, Double)] = moviesAndRatings.map(x => (x._1, x._2._1.toDouble / x._2._2))

val temp=avgRatings.join(movieInfo).map(item=>(item._2._1,item._2._2))
temp.sortByKey(false).take(20)
  .foreach(record=>println(record._2+"评分为"+record._1+"\n"))

val usersGenders = usersRDD.map(_.split("::")).map(x=>(x(0),x(1)))

val genderRatings = ratings.map(x=>(x._1,(x._1,x._2,x._3))).join(usersGenders).cache()

//genderRatings.take(10).foreach(println)


val MaleFilterRatings = genderRatings.filter(x=>x._2._2.equals("M")).map(x=>x._2._1)

val FemaleFilterRatings = genderRatings.filter(x=>x._2._2.equals("F")).map(x=>x._2._1)


println("最受男性欢迎的电影top10：\n")

val Maletemp = MaleFilterRatings.map(x=>(x._2,(x._3.toDouble,1)))
    .reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
    .map(x=>(x._1,x._2._1.toDouble/x._2._2))
    .join(movieInfo).map(item=>(item._2._1,item._2._2))
Maletemp.sortByKey(false).take(10)
    .foreach(record=>println(record._2+"评分为："+record._1+"\n"))
println("最受女性欢迎的电影top10：\n")
val Femaletemp = FemaleFilterRatings.map(x=>(x._2,(x._3.toDouble,1)))
  .reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
  .map(x=>(x._1,x._2._1.toDouble/x._2._2))
  .join(movieInfo).map(item=>(item._2._1,item._2._2))
Femaletemp.sortByKey(false).take(10)
  .foreach(record=>println(record._2+"评分为："+record._1+"\n"))

class SecondarySortKey(val first:Double,val second:Double)
extends Ordered[SecondarySortKey] with Serializable{
  override def compare(other: SecondarySortKey): Int = {
    if (this.first-other.first!=0){
      (this.first-other.first).toInt
    }else{
      if (this.second-other.second>0){
        Math.ceil(this.second-other.second).toInt
      }else if (this.second-other.second<0){
        Math.floor(this.second-other.second).toInt
      }else{
        (this.second-other.second).toInt
      }
    }
  }
}

println("对电影数据以Timestamp和Rating两个维度进行二次降序排列")

val pairWithSortKey = ratingRDD.map(line=>{
  val splited =line.split("::")
  (new SecondarySortKey(splited(3).toDouble,splited(2).toDouble),line)

})

val sorted = pairWithSortKey.sortByKey(false)

val sortedResult = sorted.map(sortedline=>sortedline._2)

sortedResult.take(10).foreach(println)


spark.stop







