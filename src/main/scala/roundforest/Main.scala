package roundforest

import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by dbortnichuk on 26-Feb-17.
  */
object Main extends App {


//  val runtime = Runtime.getRuntime
//  println("MEMORY: " + runtime.maxMemory())

  val defaultFile = "Reviews-small.csv"
  val fileName = if (args.length > 0) args(0) else {
    println("No arg for filename, using default " + defaultFile)
    this.getClass.getClassLoader.getResource(defaultFile).getFile
  }

  val start = System.currentTimeMillis()

  val sparkConf = new SparkConf()
  sparkConf.setAppName("roundforest-task")
  sparkConf.setMaster("local[*]")
  val sc = new SparkContext(sparkConf)
  val textRDD = sc.textFile(fileName)
  sc.setLogLevel("ERROR")

  println("-----MOST ACTIVE USERS-----")
  val mostActiveUsers = textRDD
    .map(reviewLine => reviewLine.split(",").apply(3)) //profileName
    .map(profileName => (profileName, 1))
    .reduceByKey(_ + _)
    .sortBy(t => t._2, ascending = false)
    .take(10)
    .foreach(t => println("ProfileName: " + t._1 + " Reviews: " + t._2))

  println()
  println("-----MOST COMMENTED ITEMS-----")
  val mostCommentedFoodItems = textRDD
    .map(reviewLine => reviewLine.split(",").apply(1)) //productId
    .map(productId => (productId, 1))
    .reduceByKey(_ + _)
    .sortBy(t => t._2, ascending = false)
    .take(10)
    .foreach(t => println("ProductId: " + t._1 + " Reviews: " + t._2))

  println()
  println("-----MOST USED WORDS IN REVIEWS TEXT-----")
  val mostUsedWords = textRDD
    .map(reviewLine => reviewLine.split(",").apply(9)) //text
    .flatMap(textline => textline.split(" "))
    .map(w => (w, 1))
    .reduceByKey(_ + _)
    .sortBy(t => t._2, ascending = false)
    .take(10)
    .foreach(t => println("Word: " + t._1 + " Count: " + t._2))
  sc.stop()

  val end = System.currentTimeMillis() - start

  println()
  println("COMPLETED IN: " + end + " ms")

}
