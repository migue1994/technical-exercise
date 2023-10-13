package com.globant.app.Services;
import com.globant.app.Interface.RequestServiceI;
import org.apache.spark.sql.SparkSession;

public class RequestServiceImpl implements RequestServiceI {

    @Override
    public void uploadData(SparkSession spark) {
        System.out.println("enntro");
        String path = "C:\\Users\\Miguel\\Downloads\\data_challenge_files\\hired_employees.csv";
        spark.read().csv(path)
                .repartition(10)
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
