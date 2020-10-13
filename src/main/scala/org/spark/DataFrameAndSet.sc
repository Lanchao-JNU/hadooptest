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

val datapath = "/Users/zhanglanchao/Downloads/hadooptest/src/main/resources/data/"
val usersRDD = sc.textFile(datapath+"users.dat")
val movieRDD = sc.textFile(datapath+"movies.dat")
val ratingRDD = sc.textFile(datapath+"ratings.dat")

println("功能一：通过DataFrame实现某部电影观看者中男性和女性不同年龄人数")

val schemaForUsers = StructType(
  "UserID::Gender::Age::OccupationID::Zip_code".split("::")

    .map(column=>StructField(column,StringType,true)))

val usersRDDRows = usersRDD
  .map(_.split("::"))
  .map(line=>
    Row(line(0).trim,line(1).trim,line(2).trim,line(3).trim,line(4).trim))

val usersDataFrame = spark.createDataFrame(usersRDDRows,schemaForUsers)
val schemaforratings = StructType("UserID::MovieID".split("::")
  .map(column=>StructField(column,StringType,true)))
  .add("Rating",DoubleType,true)
  .add("Timestamp",StringType,true)

val ratingsRDDRows = ratingRDD
  .map(_.split("::"))
  .map(line=>
  Row(line(0).trim,line(1).trim,line(2).trim.toDouble,line(3).trim))

val ratingsDataFrame = spark.createDataFrame(ratingsRDDRows,schemaforratings)

val schemaformovies = StructType("MovieID::Title::Genres".split("::").map(column=>StructField(column,StringType,true)))

val moviesRDDRows = movieRDD.map(_.split("::"))
  .map(line=>Row(line(0).trim,line(1).trim,line(2).trim))

val moviesDataFrame = spark.createDataFrame(moviesRDDRows,schemaformovies)

ratingsDataFrame.filter(s" MovieID=1193")
  .join(usersDataFrame,"UserID")
  .select("Gender","Age")
  .groupBy("Gender","Age")
  .count().show(10)

println("功能二：用LocalTempView实现某部电影观看者中不同性别不同年龄分别有多少人？")
ratingsDataFrame.createTempView("ratings")
usersDataFrame.createTempView("users")
val sql_local =
  "select Gender,Age,count(1) from users u join " +
    "ratings as r on u.UserID=r.UserID where MovieID = 1193 group by Gender,Age"

spark.sql(sql_local).show(10)

println("功能三：把二中的会话级别临时表换成application级别的")
ratingsDataFrame.createGlobalTempView("ratings")
usersDataFrame.createGlobalTempView("users")
val sql =
  "select Gender,Age,count(1) from users u join " +
    "ratings as r on u.UserID=r.UserID where MovieID = 1193 group by Gender,Age"

spark.sql(sql).show(10)


import spark.sqlContext.implicits._
println("功能四：引用隐式转换实现复杂功能")
ratingsDataFrame.select("MovieID","Rating")
    .groupBy("MovieID").avg("Rating")
    .orderBy($"avg(Rating)".desc).show(10)

//println("功能五：混合DataFrame和RDD")
//val temp = ratingsDataFrame.select("MovieID","Rating")
//    .rdd.map(row=>(row(1),(row(0),row(1))))
//temp.sortBy(_._1.toString.toDouble,false)
//    .map(tuple=>tuple._2)
//    .collect().take(10).foreach(println)

println("功能六：DataSet")

case class User(UserID:String,Gender:String,Age:String,OccupationID:String,Zip_Code:String)
case class Rating(UserID:String,MovieID:String,Rating:Double,Timestamp:String)

val usersForDSRDD = usersRDD.map(_.split("::")).map(line=>
  User(line(0).trim,line(1).trim,line(2).trim,line(3).trim,line(4).trim)

)
val usersDataSet = spark.createDataset[User](usersForDSRDD)
usersDataSet.show(10)



println("功能七：DataSet找出观看某部电影的不同性别不同年龄的人数")
val ratingsForDSRDD = ratingRDD.map(_.split("::")).map(line=>
  Rating(line(0).trim,line(1).trim,line(2).trim.toDouble,line(3).trim))
val ratingDataSet = spark.createDataset[Rating](ratingsForDSRDD)


ratingDataSet.filter("MovieID=1193").join(usersDataSet,"userID")
    .select("Gender","Age").groupBy("Gender","Age").count()
  .orderBy($"Gender".desc,$"Age".desc).show(10)


spark.stop()








