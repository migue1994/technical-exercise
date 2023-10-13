package com.globant.app.Services;

import com.globant.app.Interface.RequestServiceI;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class RequestServiceImpl implements RequestServiceI {

    @Override
    public void uploadData(SparkSession spark, String fileName) {
        String path = String.format("C:\\Users\\Miguel\\Downloads\\data_challenge_files\\%s.csv", fileName);
        StructType schema = getSchema(fileName);

        spark.read()
                .schema(schema)
                .csv(path)
                .repartition(10)
                .write()
                .format("jdbc")
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("url", "jdbc:mysql://sql10.freesqldatabase.com:3306/sql10652684")
                .option("dbtable", fileName)
                .option("user", "sql10652684")
                .option("password", "Yt3AJBSqPk")
                .save();
    }

    @Override
    public Dataset<Row> hiredEmployees(SparkSession spark) {
        Dataset<Row> jobs = readDF(spark, "jobs");
        Dataset<Row> hired_employees = readDF(spark, "hired_employees");
        Dataset<Row> departments = readDF(spark, "departments");

        return hired_employees.alias("A")
                .join(
                        jobs.alias("B"),
                        col("A.job_id").equalTo(col("B.id")),
                        "inner")
                .drop(col("B.id")).alias("A")
                .join(
                        departments.alias("B"),
                        col("A.department_id").equalTo(col("B.id")),
                        "inner")
                .drop(col("B.id")).alias("A")
                .filter(substring(col("datetime"), 1, 4).equalTo("2021"))
                .groupBy(col("department"), col("job"))
                .agg(
                        sum(dateBetweenCondition("2020-12-31", "2021-03-31")).alias("Q1"),
                        sum(dateBetweenCondition("2021-03-31", "2021-06-30")).alias("Q2"),
                        sum(dateBetweenCondition("2021-06-30", "2021-09-30")).alias("Q3"),
                        sum(dateBetweenCondition("2021-09-30", "2021-12-31")).alias("Q4"))
                .orderBy(col("department").asc(), col("job").asc());
    }

    private StructType getSchema(String fileName){
        if (fileName.equals("jobs")){
            return DataTypes.createStructType(new StructField[] {
                    DataTypes.createStructField("id", DataTypes.IntegerType, false),
                    DataTypes.createStructField("job", DataTypes.StringType, true)
            });
        } else if (fileName.equals("departments")) {
            return DataTypes.createStructType(new StructField[] {
                    DataTypes.createStructField("id", DataTypes.IntegerType, false),
                    DataTypes.createStructField("department", DataTypes.StringType, true)
            });
        }else {
            return DataTypes.createStructType(new StructField[] {
                    DataTypes.createStructField("id", DataTypes.IntegerType, false),
                    DataTypes.createStructField("name", DataTypes.StringType, true),
                    DataTypes.createStructField("datetime", DataTypes.StringType, true),
                    DataTypes.createStructField("department_id", DataTypes.IntegerType, false),
                    DataTypes.createStructField("job_id", DataTypes.IntegerType, false)
            });
        }
    }

    private Dataset<Row> readDF(SparkSession spark, String dataBase){
        return spark.read()
                .format("jdbc")
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("url", "jdbc:mysql://sql10.freesqldatabase.com:3306/sql10652684")
                .option("dbtable", dataBase)
                .option("user", "sql10652684")
                .option("password", "Yt3AJBSqPk")
                .load();
    }

    private Column dateBetweenCondition(String date1, String date2){
        Column d1 = to_date(lit(date1), "yyyy-MM-dd");
        Column d2 = to_date(lit(date2), "yyyy-MM-dd");
        return when(col("datetime").between(d1, d2), 1).otherwise(0);
    }
}
