package processor;

import bean.TDLog;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.List;

public class Writer {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("test").setMaster("yarn");
        SparkContext sc = new SparkContext(conf);
        org.apache.spark.sql.SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
        String[] apps = {"taobao", "baidu", "qq"};
        for(int j = 0;j<8;j++) {
            List<TDLog> tdLogs = new ArrayList<>();
            for (int value = 1; value <= 5; value++) {
                TDLog log = new TDLog();
                log.setTime(j*8+value);

                log.setAppName(apps[(int) (log.getTime()%3)]);
                log.setDeviceId(value);
                log.setDeviceModel("apple X");
                log.setLat(j);
                log.setLng(j);
                tdLogs.add(log);
            }

            // Create a simple DataFrame, store into a partition directory
            Dataset<Row> squaresDF = sqlContext.createDataFrame(tdLogs, TDLog.class);
            squaresDF.write().mode("append").parquet("data/test_table/");
        }
    }
}
