package com.globant.app.Services;

import com.globant.app.Interface.AuxiliaryMethodsI;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;

public class AuxiliaryMethodsImpl implements AuxiliaryMethodsI {
    @Override
    public StructType getSchema(String fileName) {
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

    @Override
    public Dataset<Row> readDF(SparkSession spark, String dataBase) {
        return spark.read()
                .format("jdbc")
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("url", "jdbc:mysql://sql10.freesqldatabase.com:3306/sql10652684")
                .option("dbtable", dataBase)
                .option("user", "sql10652684")
                .option("password", "Yt3AJBSqPk")
                .load();
    }

    @Override
    public Column dateBetweenCondition(String date1, String date2) {
        Column d1 = to_date(lit(date1), "yyyy-MM-dd");
        Column d2 = to_date(lit(date2), "yyyy-MM-dd");
        return when(col("datetime").between(d1, d2), 1).otherwise(0);
    }
}
