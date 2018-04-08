object RDD_PRACTICE extends App with Context {
  System.setProperty("hadoop.home.dir", "C:\\hadoop-2.6.0")
  val data = sparkSession.sparkContext.textFile("C:\\Users\\Mehdi\\Downloads\\olympix_data.csv")
  val map_test = data.map(line => line.split("\t"))
  val map_test2= map_test.map(line =>(line(0),line(1)))
  map_test.foreach(array => println(array.mkString(",")))


}
