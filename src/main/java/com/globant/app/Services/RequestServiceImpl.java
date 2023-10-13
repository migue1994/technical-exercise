package com.globant.app.Services;
import com.globant.app.Interface.RequestServiceI;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

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

    public StructType getSchema(String fileName){
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
}
