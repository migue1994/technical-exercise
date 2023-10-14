package com.globant.app.Interface;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public interface RequestServiceI {
    void uploadData(SparkSession spark, String basePath, String fileName, String schemaType);

    Dataset<Row> hiredEmployees(Dataset<Row> hired_employees, Dataset<Row> jobs, Dataset<Row> departments);

    Dataset<Row> hiredEmployeesByDepartment(Dataset<Row> hired_employees, Dataset<Row> departments);
}
