package processor;

import bean.GpsData;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

import java.util.Collections;

public class App {

    private static final String[][] places = {
            {"小区","景区","政府机构","度假村"},
            {"酒店","火车站","机场","长途汽车站"},
            {"学校","医院","公园","体育中心"},
            {"商业街","工业园区","楼宇相关","广播电视中心"}};

    private static final String outputDir = "output/";
    private static final String APP = "app/";
    private static final String DEV = "dev/";
    private static final String POI = "poi/";

    public static void main(String[] args){
        String path = "data/test_table";
        long avgTime_MS = 1000*60*60*2;

        //spark://62.234.212.81:7077
        //local
        SparkConf conf = new SparkConf().setAppName("test").setMaster("yarn");
        SparkContext sc = new SparkContext(conf);
        org.apache.spark.sql.SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);

        Dataset<Row> df = sqlContext.read().parquet(path).toDF();
        df.createOrReplaceTempView("TDLogDataView");


        Dataset<String> modelsDf = df.select("deviceId","deviceModel").distinct().map(new MapFunction<Row, String>() {
            @Override
            public String call(Row row){
                return row.getLong(0) +" "+ row.getString(1);
            }
        }, Encoders.STRING());
        //modelsDf.javaRDD().saveAsTextFile(outputDir+DEV);
        modelsDf.write().mode("append").text(outputDir+DEV);


        Dataset<Row> appsDf = df.select("deviceId","appName");
        JavaRDD<String> appRdd = appsDf.distinct().sort("deviceId").javaRDD()
                .groupBy(row-> row.getLong(0)).flatMap(pair->{
                    String str = ""+pair._1;
                    for(Row g : pair._2()){
                       str += " " + g.getString(1);
                    }
                    return Collections.singletonList(str).iterator();
                });
        appRdd.saveAsTextFile(outputDir+APP);


        Dataset<Row> gpsDf = df.select("deviceId","time", "lat", "lng");
        Dataset<GpsData> stringsDS = gpsDf.map(
                (MapFunction<Row, GpsData>) row ->{
                    GpsData gpsData = new GpsData();

                    gpsData.setDeviceId(row.getLong(0));
                    gpsData.setTime(row.getLong(1)/avgTime_MS);
                    gpsData.setLocation(getLocation(row.getDouble(2), row.getDouble(3)));
                    return gpsData;
                },
                Encoders.bean(GpsData.class));

        JavaRDD<String> rowRdd = stringsDS.distinct().sort("deviceId", "time").javaRDD()
                .groupBy(gpsData-> gpsData.getDeviceId()).flatMap(pair->{
                    String str = ""+pair._1;
                    for(GpsData g : pair._2()){
                        str += " " + g.getLocation();
                    }
                    return Collections.singletonList(str).iterator();
        });

        rowRdd.saveAsTextFile(outputDir+POI);


    }

    public static String getLocation(double lat, double lng){
        int gridLen = 1;
        int ii = (int) (lat / gridLen);
        if(ii>3){
            ii = 3;
        }

        int jj = (int) (lng / gridLen);
        if(jj>3){
            jj = 3;
        }
        return places[ii][jj];
    }

}
