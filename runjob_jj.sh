#set environemnt:
export SQLDBName=
export SQLDBUserName=
export SQLDBPassword=
export HiveDBName="localhost:3306"
echo "HiveDBName:" $HiveDBName
export HiveUserName=deachary
echo "HiveUserName:" $HiveUserName

export HivePassword=deachary
export mysqldatabase=test
export HiveDatabaseName=casemanager
echo "mysqldatabase:" $mysqldatabase
echo "HiveDatabaseName:" $HiveDatabaseName
export HiveTableNameCases=cases
export HiveTableNameAlertCases=alert_case
export HiveTableNameMonAlert=mon_alert
echo "HiveTableNameCases: " $HiveTableNameCases
echo "HiveTableNameAlertCases:" $HiveTableNameAlertCases
export hostname="10.51.88.17"
echo "hostname:" $hostname

echo" Deleting the old file if exists"

hadoop fs -rm -r /user/deachary/cases
hadoop fs -rm -r /user/deachary/alertcase
hadoop fs -rm -r /user/deachary/mon_alert

#Sqoop command for one time load
# Below drop steps are failing. Sqoop statements throws and error if the table exists

#hive -e 'drop table $HiveDatabaseName.$HiveTableNameCases';

# Cases table
sqoop import --connect jdbc:mysql://$HiveDBName/$mysqldatabase --username $HiveUserName --password $HivePassword --split-by case_key --columns case_key,case_identifier,case_description,priority,case_narrative,icro_number,number_of_alerts,oldest_alert_date,sar_sensitive,creation_timestamp,update_timestamp --table $HiveTableNameCases --fields-terminated-by "," --hive-import --create-hive-table --hive-table $HiveDatabaseName.$HiveTableNameCases;

# Alert Cases table
sqoop import --connect jdbc:mysql://$HiveDBName/$mysqldatabase --username $HiveUserName --password $HivePassword --split-by alert_key --columns case_identifier,alert_key,entity_name,alert_creation_timestamp --table $HiveTableNameAlertCases --fields-terminated-by "," --hive-import --create-hive-table --hive-table $HiveDatabaseName.$HiveTableNameAlertCases;

sqoop import --connect jdbc:mysql://$HiveDBName/$mysqldatabase --username $HiveUserName --password $HivePassword --split-by id --columns id,mon_base_alert_id,check_name,event_date,check_score,customer_id,customer_name,scc,sar_previously_filed,alert_score_date,workflow_workitem_id,alert_identifier --table $HiveTableNameMonAlert --fields-terminated-by "," --hive-import --create-hive-table --hive-table $HiveDatabaseName.$HiveTableNameMonAlert;

#Spark Code:

spark-shell --jars /tmp/elasticsearch-spark_2.10-2.3.3.jar --conf spark.es.nodes="$hostname"
import org.elasticsearch.spark._
import org.apache.spark.SparkContext
import org.elasticsearch.spark.sql._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

val hivectx = new HiveContext(sc)
# Step1 - Join the cases with alert_cases to bring the alert_key to cases.
val cases_alert = hivectx.sql("SELECT a.case_identifier,a.number_of_alerts,a.case_description,a.creation_timestamp,b.alert_key from casemanager.cases a join casemanager.alert_case b on (a.case_identifier=b.case_identifier) where (b.entity_name='Real Alert')");
cases_alert.registerTempTable("cases_alert_temp");

# Step2 - Filter the mon_alert for valid records.
val mon_alert = hivectx.sql("SELECT * from casemanager.mon_alert where id is not null");
mon_alert.registerTempTable("mon_alert_temp");

#Step3 - Join the cases_alert with mon_alert to add the alert details to case
val alert_mon = hivectx.sql("SELECT a.case_identifier,b.alert_identifier,b.check_name,b.alert_score_date from cases_alert_temp a join mon_alert_temp b on (a.alert_key=b.id)");
alert_mon.registerTempTable("alert_mon_temp");

#Step4 - Get the counts for alert for each case
val alert_cnt = hivectx.sql("SELECT case_identifier,count(distinct alert_identifier) as alert_count from alert_mon_temp group by case_identifier");
alert_cnt.registerTempTable("alert_cnt_temp");
#hivectx.sql("SELECT * FROM alert_cnt_temp").saveToEs("alert_cnt_temp/jj")


# Below set of spark code is working version

#val conf = new SparkConf().setAppName("casemanager")
#conf.set("es.index.auto.create", "true")

#val sc = new SparkContext(conf)
#val sqlContext = new org.apache.spark.sql.SQLContext(sc)
#val cases=sqlContext.read.parquet("/user/deachary/data.parquet")
#cases.registerTempTable("cases_table")
#sqlContext.sql("SELECT * FROM cases_table").saveToEs("casemanager_es/cases_es")


#val sqlctx = new SQLContext(sc)
#val sqlcontext = new org.apache.spark.sql.hive.HiveContext(sc)
#var df1 = sqlContext.sql("SELECT case_key,case_identifier,case_description,priority,case_narrative,icro_number,number_of_alerts,oldest_alert_date,sar_sensitive,creation_timestamp,update_timestamp from test.cases")
#df1.collect().foreach(println)
#val dataDF = df1.toDF()
#dataDF.write.parquet("data.parquet")


#val conf = new SparkConf().setAppName("WriteToES")
#conf.set("es.index.auto.create", "true")
#val sc = new SparkContext(conf)
#val sqlContext = new org.apache.spark.sql.SQLContext(sc)
#val sen_p=sqlContext.read.parquet("/home/deachary/data.parquet")
#sen_p.registerTempTable("sensor_ptable")
#sqlContext.sql("SELECT * FROM sensor_ptable").saveToEs("test/deba")
