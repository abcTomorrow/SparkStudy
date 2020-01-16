package com.wojiushiwo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Created by myk
 * 2019/9/24 下午1:33
 */
public class SparkSQLDemo {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("abc").master("local[2]")
                .getOrCreate();

        Dataset<Row> df = sparkSession.read().json();
//        Column[] columns=new Column[]{Column.};
//        df.select()
    }
}
