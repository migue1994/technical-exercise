package com.globant.app.Services;

import com.globant.app.Interface.AuxiliaryMethodsI;
import com.globant.app.Interface.RequestServiceI;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class RequestServiceImpl implements RequestServiceI {

    private final AuxiliaryMethodsI auxiliaryMethodsI;

    public RequestServiceImpl(AuxiliaryMethodsI auxiliaryMethodsI) {
        this.auxiliaryMethodsI = auxiliaryMethodsI;
    }

    @Override
    public void uploadData(SparkSession spark, String basePath, String fileName, String schemaType) {
        String path = basePath + fileName + ".csv";
        StructType schema = auxiliaryMethodsI.getSchema(schemaType);

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
    public Dataset<Row> hiredEmployees(Dataset<Row> hired_employees, Dataset<Row> jobs, Dataset<Row> departments) {

        return hired_employees.alias("A")
                .filter(substring(col("datetime"), 1, 4).equalTo("2021"))
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
                .groupBy(col("department"), col("job"))
                .agg(
                        sum(auxiliaryMethodsI.dateBetweenCondition("2020-12-31", "2021-03-31")).alias("Q1"),
                        sum(auxiliaryMethodsI.dateBetweenCondition("2021-03-31", "2021-06-30")).alias("Q2"),
                        sum(auxiliaryMethodsI.dateBetweenCondition("2021-06-30", "2021-09-30")).alias("Q3"),
                        sum(auxiliaryMethodsI.dateBetweenCondition("2021-09-30", "2021-12-31")).alias("Q4"))
                .orderBy(col("department").asc(), col("job").asc());
    }

    @Override
    public Dataset<Row> hiredEmployeesByDepartment(Dataset<Row> hired_employees, Dataset<Row> departments) {

        Dataset<Row> result = hired_employees
                .filter(substring(col("datetime"), 1, 4).equalTo("2021"))
                .alias("A")
                .join(
                        departments.alias("B"),
                        col("A.department_id").equalTo(col("B.id")),
                        "inner")
                .groupBy(col("B.id"), col("department"))
                .agg(count(col("B.id")).alias("hired"));

        Double value = result.select(mean(col("hired").cast("Double"))).first().getDouble(0);

        return result.filter(col("hired").gt(lit(value)))
                .orderBy(col("hired").desc());
    }
}
