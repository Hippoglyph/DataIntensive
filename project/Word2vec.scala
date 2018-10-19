import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._
import org.apache.spark._
import org.apache.spark.sql._
//import org.apache.spark.implicits._
import org.apache.spark.streaming._



class Word2vec(conf: SparkConf) extends Serializable {
	//val spark = SparkSession.builder.config(conf).config("spark.cassandra.connection.host", "127.0.0.1").getOrCreate()
	//val sc = spark.sparkContext
	//import spark.implicits._
	//createTable()

	var wordData = scala.collection.mutable.Map[String, Int]()
	var mapLength = wordData.keys.size
	
	/*
	val df = spark.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "words", "keyspace" -> "project" )).load()

	df.show()

    val rdd = sc.parallelize(Seq(("Hello", 0, List(3.4,4.3))))

    rdd.saveToCassandra("project", "words", SomeColumns("word", "id", "vector"))
    */

    def process(tweet: String){
    	var newWord = false
    	val tokens = tweet.split(" ")
    	tokens.foreach{x =>
    		if (!wordData.contains(x)){
    			wordData(x) = mapLength
    			mapLength += 1
    			newWord = true
    		}
    	}
    	if (newWord){
    		println("Unique words: " + mapLength)
    		//storeWordData()
    	}
    }

    /*def storeWordData(){
    	val rdd = sc.parallelize(wordData.toSeq).map{case (key, value) => (key, value, List(0.0,0.0))}
    	rdd.saveToCassandra("project", "words", SomeColumns("word", "id", "vector"))
    }

    def createTable(){
		val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
	    val session = cluster.connect()
	    session.execute("CREATE KEYSPACE IF NOT EXISTS project WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
	    session.execute("CREATE TABLE IF NOT EXISTS project.words (word text PRIMARY KEY, id int, vector list<double>);")
	}*/

}