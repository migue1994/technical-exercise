package com.globant.app.Interface;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public interface AuxiliaryMethodsI {
    StructType getSchema(String fileName);

    Dataset<Row> readDF(SparkSession spark, String dataBase);

    Column dateBetweenCondition(String date1, String date2);
}
