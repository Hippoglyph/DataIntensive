import org.apache.spark._
import org.apache.spark.streaming._

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._

import org.apache.spark.sql._
import org.apache.log4j.Logger
import org.apache.log4j.Level

import breeze.linalg._
import breeze.numerics._

object closestWord {

	def getWordData() = {
		Logger.getLogger("org").setLevel(Level.OFF)
		Logger.getLogger("akka").setLevel(Level.OFF)

		val conf = new SparkConf().setAppName("BFFs sick stream").setMaster("local[2]")
		val spark = SparkSession.builder.config(conf).getOrCreate()
    	import spark.implicits._
    	val df = spark.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "words", "keyspace" -> "project" )).load()
    	//scala.collection.mutable.Map[String, (Int, breeze.linalg.DenseVector[Double])]()
    	Map[String, DenseVector[Double]](df.rdd.map(row => (row.getString(0), DenseVector(row.getAs[Seq[Double]](2).toArray))).collectAsMap().toSeq: _*)
	}

	def getDistance(a: DenseVector[Double], b: DenseVector[Double]) = {
		val c = a-b
		val c2 = c:*c
		val s = sum(c2)
		scala.math.sqrt(s)
	}

	def printNClosest(word: String, wordData: Map[String, DenseVector[Double]], N: Int){
		val point = wordData(word)
		val distanceList = wordData.map{case (w, vector) => (w, getDistance(point, vector))}.toArray
		val sorted = distanceList.sortWith((a,b) => a._2 < b._2)
		val maxDist = distanceList.map(x => x._2).reduceLeft(_ max _)

		for(i <- 0 to N){
			if (sorted(i)._1 != word){
				println(sorted(i)._1 + " (" + scala.math.floor(100*(1 - (sorted(i)._2/maxDist))) + "%)")
			}
		}
	}

	def printUsage(){
		println("Usage: realdonaldtrump 5")
	}

	def toInt(s: String): Int = {
		try{
			s.toInt
		} catch{
			case e: Exception => 0
		}

	}

	def main(args: Array[String]){
		val wordData = getWordData()
		printUsage()
		while(true){
			println("")
			print(">")
			val word = scala.io.StdIn.readLine()
			val input = word.split(" ")
			if(input.length > 2)
				printUsage()
			else{
				val word = input(0).toLowerCase()
				val N = if (input.length == 2) toInt(input(1)) else 5
				if (N == 0){
					printUsage()
				}
				else{
					if(wordData.contains(word))
						printNClosest(word, wordData, N)
					else
						println("I am sorry, I dont know that word yet =(")
				}
			}
		}
	}
}