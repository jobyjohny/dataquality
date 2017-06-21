package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLImplicits
import org.joda.time._
import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._
import org.elasticsearch.spark.rdd.EsSpark    
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat

object casemanager {
  
    case class Cases      (case_key:String, case_identifier:String, case_description:String, case_narrative:String,
                           number_of_alerts:String,creation_timestamp:String)
                           
    case class Alert_Cases(case_identifier:String,alert_key:Int,entity_name:String,alert_creation_timestamp:String)
    
    case class Mon_Alert  (id:Int, mon_base_alert_id:Int,check_name:String,event_date:String,check_score:String,
                         customer_id:String,customer_name:String,scc:String,sar_previously_filed:String,
                         alert_score_date:String,workflow_workitem_id:String,alert_identifier:String)
                         
    case class Transactions(alert_identifier:String,txn_id:String,account_id:String,primary_customer_id:String,
                            branch_id:String,txn_source_type_code:String,txn_amount_orig:Float,txn_amount_base:Float,
                            credit_debit_code:String,txn_status_code:String,txn_channel_code:String,originator_key:String,
                            originator_address:String,originator_country:String,beneficiary_key:String,
                            beneficiary_address:String,beneficiary_country:String,run_date:String)
                            
   case class Customers  (customer_id:String,customer_name:String,date_of_birth:String,
                           address:String,postal_code:String,country_of_residence:String,
                           country_of_origin:String,occupation:String,customer_type_code:String,
                           phone_number:String,email_address:String,ssn:String,scc:String,
                           work_phone_number:String,is_active:String,creation_timestamp:String)

        
    // CASES - SPLIT 
    def mapper(line:String): Cases = {
    //Dropping the header record and spliting the rest of the records using comma
    val data = line.split(',')
    val cases:Cases = Cases(data(0), data(1), data(2).toUpperCase,data(4), data(6),data(9))
    //val date_format = new java.text.SimpleDateFormat("yyyy-MM-dd mm:ss")
    //date_format.format(new java.util.Date())
    //date_format.parse(data(9))
    
    return cases
      }
    
    def toInt(s:String):Option[Int] = { 
      try {
        Some(s.toInt)
      }
      catch {
        case e: NumberFormatException => None
        }
    }
      
    //toInt("a") match {
    //case Some(i) => println(i)
    //case None => println("That didn't work.")
    //}
    
    
  // ALERT CASES - SPLIT AND FILTER 
    def mapper_ac(line:String): Alert_Cases = {
      val data = line.split(',')
     val alert_cases:Alert_Cases = Alert_Cases(data(0), data(1).toInt, data(2).toUpperCase,data(3))
     return alert_cases
      }
    
     // MON ALERT - SPLIT AND FILTER 
    def mapper_ma(line:String): Mon_Alert = {
      val data = line.split(',')
      val mon_alert:Mon_Alert = Mon_Alert (data(0).toInt, data(1).toInt, data(2).trim, data(3), data(4), 
                                            data(5), data(6).toUpperCase, data(7), data(8), data(9), data(10),
                                            data(11).trim)
     return mon_alert
      }
     // TRANSACTIONS - SPLIT AND FILTER 
    def mapper_trxn(line:String): Transactions = {
      val data = line.split(',')
     val transactions:Transactions = Transactions (data(0).trim,data(1).trim,data(2),data(3),data(4), 
                                                   data(5), data(6).toFloat, data(7).toFloat, data(8), 
                                                   data(9), data(10),data(11).trim,data(12).trim,
                                                   data(13).trim,data(14).trim,data(15).trim,
                                                   data(16).trim,data(17))
     return transactions
      }
         // CUSTOMERS - SPLIT AND FILTER 
    def mapper_cust(line:String): Customers = {
      val data = line.split(',')
     val customers:Customers = Customers (data(0).trim,data(4).trim, data(5), data(6).trim, data(7), 
                                          data(8), data(9), data(10),data(11).trim,data(12),data(13).trim,
                                          data(14).trim,data(15),data(16).trim,data(17),data(18))
     return customers
      }
    
    
    def count_checker(source_cnt:Int,target_cnt:Int,table:String) 
    {
      if (source_cnt == target_cnt) 
      {
        println(s" $table counts are matching before and after parsing\n")
      }
        
      else
      {
        println(s" $table counts are not matching before and after parsing\n")
      }

    }
    
      
    /** Our main function where the action happens */
  def main(args: Array[String])
  {
  
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
       
    // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
    //comment 1
    val spark = SparkSession.builder.appName("CaseManager").master("local[*]").config("spark.sql.warehouse.dir", "file:///C:/temp").getOrCreate()
    import spark.implicits._
    
    //val conf = new SparkConf() 
    //val sc = new SparkContext(conf)  
    
    println("###########################  Testing elasticsearch #######################################\n")
    
    //conf.set("es.index.auto.create", "true")
    //conf.set("es.nodes", "127.0.0.1")
    //conf.set("es.port", "9200")
    spark.conf.set("es.index.auto.create", "true")
    spark.conf.set("es.nodes", "127.0.0.1")
    spark.conf.set("es.port", "9200")
    //val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
    //val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")
    //val test_df = spark.
    
    
    //case class Trip(departure: String, arrival: String)               

    //val up = Trip("OTP", "SFO")
    //val down = Trip("MUC", "OTP")

    //(Seq(upcomingTrip, lastWeekTrip))
    //val rdd = spark.createDataset(Seq(1,2))
    //test.collect()
    println("###########################Writing into  elasticsearch starting ###################################\n")
   // EsSpark.saveToEs(rdd, "spark/docs") 
    //EsSpark.saveToEs(test,"abc/def")
    //EsSpark.saveToEs(test, "spark_20170612/docs")    
    
     println("###########################Writing into  elasticsearch completed ###################################\n")
   //val sqlContext = new org.apache.spark.sql.SQLContext(spark)
   //val dataFrame = spark.read.format("CSV").option("header","true").load("cases.csv")
   //println("going to print the dataframe")
   //dataFrame.show()
    
    //##################################################################################
    // CASES CSV FILE PROCESSING STARTS
    //comment 2
    val lines = spark.sparkContext.textFile("cases.csv")
    //val lines = sc.textFile("cases.csv")
  // Reading the header record which is coming as first row
    val lines_header = lines.first()
    //Remove the header record from data
    val lines_data = lines.filter(row => row != lines_header)

    val source_data_count = lines_data.count()

    //Calling the method to split the file and process
    val cases = lines_data.map(mapper).toDS().cache()
    //val cases = lines_data.map(mapper).
    
    //counting the number of records
    val cases_count = cases.count()

    // Calling the count checker function to validate the counts
    
    count_checker(source_data_count.toInt,cases_count.toInt,"CASES -")
    
    // #################################################################################
    // ALERT CASES CSV FILE PROCESSING STARTS
    // comment 3
    val ac_input = spark.sparkContext.textFile("alert_cases.csv")
    //val ac_input = sc.textFile("alert_cases.csv")
    // Reading the header record which is coming as first row
    val ac_header = ac_input.first()
    //Remove the header record from data
    val ac_data = ac_input.filter(row => row != ac_header)

    val ac_source_data_count = ac_data.count()
    println(s"Number of data records before parsing: $ac_source_data_count\n")
    //Calling the method to split the file and process
    val ac = ac_data.map(mapper_ac).toDS().cache()
    //val ac = ac_data.map(mapper_ac).cache()
    //counting the number of records
    val ac_count = ac.count()
    //println (s"Number of records after parsing : $ac_count\n")
    
    // Calling the count checker function to validate the counts
    
    count_checker(ac_source_data_count.toInt,ac_count.toInt,"ALERT CASES - ")
    
 
    println("Alert Cases - Data records are given below\n")
    
        
    //  ALERT CASES CSV FILE PROCESSING ENDS
    //########################################################################################
    
     // MON ALERT CSV FILE PROCESSING STARTS
    
    println("Mon Alert data processing starts\n")
    //commnet 4
    val ma_input = spark.sparkContext.textFile("mon_alert.csv")
    //val ma_input = sc.textFile("mon_alert.csv")
    // Reading the header record which is coming as first row
    val ma_header = ma_input.first()
    //Remove the header record from data
    val ma_data = ma_input.filter(row => row != ma_header)
    println("Data records after header removal########\n")
    val ma_print = ma_data.collect()
    //ma_print.foreach(println)
    
    val ma_source_data_count = ma_data.count()
    //println(s"Number of data records before parsing: $ma_source_data_count\n")
    //Calling the method to split the file and process
    val monalert = ma_data.map(mapper_ma).toDS().cache()
    //val monalert = ma_data.map(mapper_ma).cache()
    //counting the number of records
    val ma_count = monalert.count()
   
    
    // Calling the count checker function to validate the counts
    
    count_checker(ma_source_data_count.toInt,ma_count.toInt,"MON ALERT - ")
    
 
    //println("MON ALERT - Data records are given below\n")
    
    //val ma_df = monalert.toDF()
    //ma_df.show()
    
   
    // MON ALERT CSV FILE PROCESSING ENDS
    //########################################################################################
    
    
        
     // TRANSACTIONS FILE PROCESSING STARTS
    
    println("Transactions data processing starts\n")
    //comment5
    val trxn_input = spark.sparkContext.textFile("transactions.csv")
    //val trxn_input = sc.textFile("transactions.csv")
    // Reading the header record which is coming as first row
    val trxn_header = trxn_input.first()
    //Remove the header record from data
    val trxn_data = trxn_input.filter(row => row != trxn_header)
    println("Data records after header removal########\n")
    val trxn_print = trxn_data.collect()
    trxn_print.foreach(println)
    val trxn_source_data_count = trxn_data.count()
    println (s"Number of records before parsing : $trxn_source_data_count\n")
    //Calling the method to split the file and process
    val transactions = trxn_data.map(mapper_trxn).toDS().cache()
    //val transactions = trxn_data.map(mapper_trxn).cache()
    //counting the number of records
    val trxn_count = transactions.count()
    println (s"Number of records after parsing : $trxn_count\n")
    // Calling the count checker function to validte the counts
    
    count_checker(trxn_source_data_count.toInt,trxn_count.toInt,"TRANSACTIONS - ")
    
   //println("TRANSACTIONS - Data records are given below\n")
    
   val transactions_df = transactions.toDF()
    
    // TRANSACTIONS CSV FILE PROCESSING ENDS
    //########################################################################################
       
    // CUSTOMERS FILE PROCESSING STARTS
    
    //println("Customers data processing starts\n")
   //comment2
     val cust_input = spark.sparkContext.textFile("customers.csv")
   //val cust_input = sc.textFile("customers.csv")
    // Reading the header record which is coming as first row
    val cust_header = cust_input.first()
    //Remove the header record from data
    val cust_data = cust_input.filter(row => row != cust_header)
    println("Data records after header removal########\n")
    val cust_print = cust_data.collect()
    cust_print.foreach(println)
    val cust_source_data_count = cust_data.count()
    //println (s"Number of records before parsing : $trxn_source_data_count\n")
    //Calling the method to split the file and process
    val customers = cust_data.map(mapper_cust).toDS().cache()
    //val customers = cust_data.map(mapper_cust).cache()
    //counting the number of records
    val cust_count = customers.count()
    //println (s"Number of records after parsing : $trxn_count\n")
    // Calling the count checker function to validte the counts
    
    count_checker(cust_source_data_count.toInt,cust_count.toInt,"CUSTOMERS - ")
    
   println("CUSTOMERS - Data records are given below\n")
    
   val customers_df = customers.toDF()
   
    
    // JOIN LOGIC BEGINS HERE
    //######################################################################################## 
    
   cases.registerTempTable("cases_table")
    ac.registerTempTable("ac_table")
    monalert.registerTempTable("monalert_table")
    transactions.registerTempTable("trxn_table")
    customers.registerTempTable("cust_table")
    // LOGIC FOR GETTING THE DISTINCT ORIG AND BENE KEYS FROM TRANSACTIONS
    
    val orig__key_txn = transactions_df.select($"originator_key")
    val bene_key_txn  = transactions_df.select($"beneficiary_key")
    val orig_bene_txn = orig__key_txn.unionAll(bene_key_txn).distinct()
    //println("Displaying the unique orig and bene keys from transactions")
    //orig_bene_txn.show
        
    //CREATES A TEMP TABLE FROM CASES,ALERT CASE AND MON ALERT TABLE
    val cases_alert = spark.sql("select cases.case_identifier,cases.case_description,ac.alert_key,ma.alert_identifier from cases_table cases join ac_table ac join monalert_table ma on (cases.case_identifier=ac.case_identifier) and ac.alert_key=ma.id where trim(ac.entity_name)='REAL ALERT' and ma.id is not null")
    cases_alert.registerTempTable("cases_alert_table")
    spark.sql("select * from cases_alert_table").show()
    
    println("transaction customer table join starts\n")
    val trxn_cust = spark.sql("select distinct txn.txn_id,customer_id,customer_name,address,phone_number,email_address from cust_table cust join trxn_table txn where cust.customer_id=txn.originator_key or cust.customer_id=txn.beneficiary_key")
    println("Displaying the data\n");
    trxn_cust.toDF().show()
    trxn_cust.registerTempTable("trxn_cust_table")
    
    
    //val cases_alert_count = spark.sql("select case_identifier as Case_Number,count(distinct alert_identifier) as Alert_Count from cases_alert_table group by case_identifier")
    //cases_alert_count.show()
    val cases_txn = spark.sql("select ca.case_identifier,ca.case_description,ca.alert_identifier,txn.txn_id,txn.txn_amount_orig from cases_alert_table ca join trxn_table txn on ca.alert_identifier=txn.alert_identifier") 
    
    cases_txn.show()
    cases_txn.registerTempTable("cases_txn_table")
    val cases_txn_cust = spark.sql("select a.case_identifier,a.case_description,a.alert_identifier,a.txn_id,a.txn_amount_orig,b.customer_id from cases_txn_table a join trxn_cust_table b on a.txn_id=b.txn_id")
    cases_txn_cust.toDF().show()
    cases_txn_cust.registerTempTable("cases_txn_cust_table")
    

    val cases_count_stats = spark.sql("select case_identifier as Case_Number,count(distinct alert_identifier) as Alert_Count,count(distinct txn_id) as Transaction_Count,sum(txn_amount_orig) as Transaction_Amount_Dollars,count(distinct customer_id) as Customer_Count  from cases_txn_cust_table group by case_identifier")
    cases_count_stats.show()
    println("Writing to elastic")
    
    cases_count_stats.saveToEs("case_summary/cs_type")
    println("write completed to elastic")
    
    //EsSpark.saveToEs(cases_count_stats,"abc/def")
    spark.stop()
  
  }
}
   
