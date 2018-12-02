package processor;

import bean.TDLog;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;
//processor.Writer
public class Writer {
    public static void main(String[] args){
        String parquetPath = "ods/td/";
        if(args.length>0){
            parquetPath+=args[0];
        }

        SparkSession spark = SparkSession
                .builder()
                //.master("local")
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

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
            Dataset<Row> squaresDF = spark.createDataFrame(tdLogs, TDLog.class);
            squaresDF.write().mode("append").parquet(parquetPath);
        }
    }
}
