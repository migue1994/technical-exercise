package com.globant.app.Interface;

import org.apache.spark.sql.SparkSession;

public interface RequestServiceI {
    void uploadData(SparkSession spark);
}
