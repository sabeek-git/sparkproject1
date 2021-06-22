package SparkGitPack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import sys.process._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object SparkGitObj {
  
case class schema(txnno:String, txndate:String, custno:String, amount:String, category:String,product:String, city:String, state:String, spendby:String)

def main(args:Array[String]):Unit={ 
  
val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")

					val spark = SparkSession.builder().getOrCreate()
					import spark.implicits._

					println("================Revision1========================")
					println()
					
					val columns_list = List("category","product","txnno","txndate","amount","city","state","spendby","custno")
					
					println("========================1=======================")
					val listint = List(1,4,6,7)
					val resint = listint.map(x=>x+2)
					resint.foreach(println)
}
}