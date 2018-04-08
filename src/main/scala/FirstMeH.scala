import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object FirstMeH {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\hadoop-2.6.0")

    val conf = new SparkConf().setAppName("MehApp").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = SparkSession.builder().appName("Spark In Action").master("local").getOrCreate()
    import sqlContext.implicits._
    val File = sc.textFile("C:\\Users\\Mehdi\\Desktop\\MeHData.txt").zipWithIndex
    /*val NbrLine = File.map(s=> (s,1))
    val LineWithNum = File.map(s=> s._2+","+s._1)
    val NbrWords =File.map(s => (s._1.split(" ").size,s._2))
    NbrWords.collect().foreach(println)
    val bigWord = NbrWords.reduce((a,b) => if (a._1 > b._1) a else b)
    val list = List('a','b','c','d').zipWithIndex
    println("La ligne la plus longue est "+bigWord._2+"il y a "+bigWord._1+"  mots")*/
    /*val input = sc.textFile("C:\\Users\\Mehdi\\Desktop\\SparkTest.txt");
    val lines = input.flatMap(line => line.split("\\r\\n"));
    val nbr_lines = lines.map(word => (word,1))
    nbr_lines.collect().foreach(l => println(l));*/
    //lines.collect().foreach(l=>println(l))
    /*val Wordss = Words.map(word => (word,1));
    val count = Wordss.reduceByKey((x,y)=> x+y);
    count.collect().foreach( x=> println(x));
    val Nbr = count.reduce((x,y)=> x+y);*/
    val set = Set("mehdi","mehdi","zzzz");
    println(set.size);
    val dataset = Seq((0, "hello"),(1, "world")).toDF("id","text");
    val upper: String => String =_.toUpperCase
    val test = upper("test")
    import org.apache.spark.sql.functions.udf
    val upperUDF = udf(upper)
    upperUDF()
    dataset.withColumn("upper", upperUDF('text)).show
  }
}

