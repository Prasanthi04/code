import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Application {

    private static final Logger LOG = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
        SparkConf conf = new SparkConf().setAppName("WebLogsAnalytics").setMaster("local").set("spark.cores.max", "10").set("spark.sql.caseSensitive", "true");
        SparkSession spark = SparkSession
                .builder()
                .appName("WEBLOG PROJECT")
                .config(conf)
                .getOrCreate();

        Dataset<Row> df = spark.read().option("delimiter", " ").csv("C:/2015_07_22_mktplace_shop_web_log_sample.log");
        spark.sqlContext().setConf("spark.sql.shuffle.partitions","10");

        //Question 2
        //Convert the string (of column 1, that is datetime) to datetime UNIX_Timestamp, find the delta time of each group after doing the groupby with IP, IPPort (which
        //is considered to be a session). Then the mean value of all the rows in the resultant dataset with deltattime will give the average session time.
        LOG.info("Finding the average session time....");
        Dataset<Row> dfNew = df.withColumn("datetime", (df.col("_c0").cast("timestamp")));

        Dataset<Row> dfNew1 = dfNew.selectExpr("_c0", "split(_c2, ':')[0] as IP", "split(_c2, ':')[1] as IPPort", "_c3", "_c4", "_c5", "_c6", "_c7", "_c8", "_c9", "_c10", "_c11", "_c12", "datetime")
                .withColumn("mxDT",new Column("datetime"));
        dfNew1.show();

        Dataset<Row> dfNewMax = dfNew1.groupBy("IP", "IPPort").
                agg(org.apache.spark.sql.functions.min("mxDT").
                        alias("minDT"),org.apache.spark.sql.functions.
                        max("mxDT").
                        alias("maxDT"));

        Dataset<Row> dfTimeDiff = dfNewMax.withColumn("deltatime",
                functions.unix_timestamp(new Column("maxDT")).$minus(functions.unix_timestamp(new Column("minDT"))));

        dfTimeDiff.agg(functions.mean("deltatime")).show();

        //QUESTION 1
        //Groupby IP and IPport , which is a session, and then aggregate the URLs per each session.
        LOG.info("Sessionizing the logs by IP....");
        dfNew1.groupBy("IP", "IPPort")
                .agg(functions.sum("_c11"))
                .show();


//       Question 3
        //Groupby by the IP, IPPORT (Ccnsidered as one session) that the user visited and count the distinct number of times the URL was visited
        LOG.info("Finding the unique/distinct URL visits per session by the user....");
        dfNew1.select("IP","IPPort", "_c11")
                 .groupBy("IP", "IPPort")
                 .agg(functions.countDistinct("_c11"))
                 .count();  //404391 times

          //Question 4
          //Sort the deltattime per session in descending order to know the IPs that has the longest session.
         LOG.info("Finding the longest session....");
         dfTimeDiff.select("IP", "deltatime")
                 .sort(new Column("deltatime").desc())
                 .show();
    }

}
