import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object SparkTool {
  def main(args: Array[String]): Unit = {
    println("Hello, world!")
    val conf = new SparkConf().setMaster("local").setAppName("Spark Tool")
    val sc = new SparkContext(conf)

    val input = sc.textFile("/Users/diegobenavides/Documentos/DataSets/numbers.txt")

    println(input.collect().mkString(","))

    val result = input.aggregate((0,0))(
    				(acc,value) => (acc._1 + value.toInt,acc._2 + 1),
    				(acc1,acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    )

    val avg = result._1 / result._2.toDouble

    println("====> Promedio:" + avg)
  }
}
