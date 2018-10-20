package Word2vec

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._
import org.apache.spark._
import org.apache.spark.sql._
//import org.apache.spark.implicits._
import org.apache.spark.streaming._



object Word2vec {	
	/*
	val df = spark.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "words", "keyspace" -> "project" )).load()

	df.show()
    */

    def process(tweet: String, wordData: scala.collection.mutable.Map[String, Int]) = {
    	var newWord = false
    	val reTweet = tweet.filter(purge)
    	val tokens = reTweet.split(" ")
    	var newWords = scala.collection.mutable.Set[String]()
    	tokens.foreach{x =>
    		var append = true
    		val word = x.toLowerCase
    		if (wordData.contains(word))
    			append = false
    		else if (word == "")
    			append = false
    		else if(word.startsWith("http"))
    			append = false
    		else if(isStopWord(word))
    			append = false
    		if(append)
    			newWords += word
    	}
    	newWords.toSet
    }

    def purge(c: Char) = {
    	val llegals = Set('q','w','e','r','t','t','y','u','i','o','p','a','s','d','f','g','h','j','k','l','z','x','c','v','b','n','m','`',''',' ','\t')
    	llegals.contains(c)
    }

    def isStopWord(word: String) = {
    	val stopWord = Set("a","about","above","after","again","against","all","am","an","and","any","are","aren't","as","at","be","because","been","before","being","below","between","both","but","by","can't","cannot","could","couldn't","did","didn't","do","does","doesn't","doing","don't","down","during","each","few","for","from","further","had","hadn't","has","hasn't","have","haven't","having","he","he'd","he'll","he's","her","here","here's","hers","herself","him","himself","his","how","how's","i","i'd","i'll","i'm","i've","if","in","into","is","isn't","it","it's","its","itself","let's","me","more","most","mustn't","my","myself","no","nor","not","of","off","on","once","only","or","other","ought","our","ours","ourselves","out","over","own","same","shan't","she","she'd","she'll","she's","should","shouldn't","so","some","such","than","that","that's","the","their","theirs","them","themselves","then","there","there's","these","they","they'd","they'll","they're","they've","this","those","through","to","too","under","until","up","very","was","wasn't","we","we'd","we'll","we're","we've","were","weren't","what","what's","when","when's","where","where's","which","while","who","who's","whom","why","why's","with","won't","would","wouldn't","you","you'd","you'll","you're","you've","your","yours","yourself","yourselves")
		stopWord.contains(word)    
    }

    def containsNumber(word: String): Boolean = {
    	val numbers = Set('0', '1', '2', '3', '4', '5', '6', '7', '8', '9')
    	for(c <- word){
    		if(numbers.contains(c))
    			return true
    	}
    	false
    }

    def getInitWordData() = {
    	scala.collection.mutable.Map[String, Int]()
    }

    def addToContender(contenders: scala.collection.mutable.Map[String, Int],newWords: Array[String]) = {
    	newWords.foreach{x =>
    		if(contenders.contains(x)){
    			contenders(x) = contenders(x) + 1
    		}
    		else{
    			contenders(x) = 1
    		}
    	}
    }

    def addToWordData(wordData: scala.collection.mutable.Map[String, Int],contenders: scala.collection.mutable.Map[String, Int]) = {
    	var index = wordData.keys.size
    	for((k,v) <- contenders){
    		if (v > 5){
    			wordData(k) = index
    			contenders.remove(k)
    			index += 1
    		}
    	}
    }
}