package com.globant.app.Interface;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public interface RequestServiceI {
    void uploadData(SparkSession spark, String fileName);

    Dataset<Row> hiredEmployees(SparkSession spark);
}
