package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._
import org.elasticsearch.spark.rdd.EsSpark    
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat

object casemanager {
  
    case class Cases(case_key:String, case_identifier:String, case_description:String, case_narrative:String,number_of_alerts:String,creation_timestamp:String)
  
    def mapper(line:String): Cases = {
    //Dropping the header record and spliting the rest of the records using comma
    
      
    val data = line.split(',')
    val cases:Cases = Cases(data(0), data(1), data(2).toUpperCase,data(4), data(6),data(9))
    val date_format = new java.text.SimpleDateFormat("yyyy-MM-dd mm:ss")
    date_format.format(new java.util.Date())
    date_format.parse(data(9))
    
    return cases
      }
  
    /** Our main function where the action happens */
  def main(args: Array[String])
  {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
    val spark = SparkSession.builder.appName("CaseManager").master("local[*]").config("spark.sql.warehouse.dir", "file:///C:/temp").getOrCreate()
    import spark.implicits._
   //val dataFrame = spark.read.format("CSV").option("header","true").load("cases.csv")
   //println("going to print the dataframe")
   //dataFrame.show()
    
    // Processing the cases csv file
    val lines = spark.sparkContext.textFile("cases.csv")
    // Reading the header record which is coming as first row
    val lines_header = lines.first()
    //Remove the header record from data
    val lines_data = lines.filter(row => row != lines_header)
    println("Data records after header removal########\n")
    val input_print = lines_data.collect()
    input_print.foreach(println)
    val source_data_count = lines_data.count()
    println(s"Number of data records before parsing: $source_data_count\n")
    //Calling the method to split the file and process
    val cases = lines_data.map(mapper).toDS().cache()
    //counting the number of records
    val cases_count = cases.count()
    println (s"Number of records after parsing : $cases_count\n")
    
    if (source_data_count == cases_count)
      println("Counts are matching\n")
        if (source_data_count != cases_count)
      println("Counts are not matching\n")
    
    
    println("Data records ########\n")
    val cases_data = cases.toDF().show
     
   println("Printing completed.")
    //spark.stop()
   
  }
   
}
   
