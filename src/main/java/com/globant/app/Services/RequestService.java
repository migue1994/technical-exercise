package com.globant.app.Services;
import org.apache.spark.sql.SparkSession;

public class RequestService {

    public void uploadData(SparkSession spark) {
        System.out.println("enntro");
        String path = "C:\\Users\\Miguel\\Downloads\\data_challenge_files\\hired_employees.csv";
        spark.read().csv(path)
                .limit(1000)
                .repartition(1)
                .write()
                .format("jdbc")
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("url", "jdbc:mysql://sql10.freesqldatabase.com:3306/sql10652684")
                .option("dbtable", "departments")
                .option("user", "sql10652684")
                .option("password", "Yt3AJBSqPk")
                .save();
    }
}
