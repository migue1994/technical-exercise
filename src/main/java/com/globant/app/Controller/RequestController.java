package com.globant.app.Controller;

import com.globant.app.Interface.AuxiliaryMethodsI;
import com.globant.app.Interface.RequestServiceI;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static spark.Spark.get;
import static spark.Spark.post;

public class RequestController {

    private final RequestServiceI requestServiceI;
    private final SparkSession spark;
    private final AuxiliaryMethodsI auxiliaryMethodsI;

    public RequestController(
            SparkSession spark,
            RequestServiceI requestServiceI,
            AuxiliaryMethodsI auxiliaryMethodsI){
        this.requestServiceI = requestServiceI;
        this.spark = spark;
        this.auxiliaryMethodsI = auxiliaryMethodsI;
        mainApiRest();
    }

    private void mainApiRest(){
        get("/api/data/:fileName", (req, res) -> {
            try{
                String basePath = "src/test/resources/data/";
                requestServiceI.uploadData(spark, basePath, req.params(":fileName"), req.params(":fileName"));
                return String.format("The %s file has been uploaded correctly!", req.params(":fileName"));
            }catch (Exception e){
                return e;
            }
        });

        get("/api/data/query/sql", (req, res) -> {
            try{
                Dataset<Row> jobs = auxiliaryMethodsI.readDF(spark, "jobs");
                Dataset<Row> hired_employees = auxiliaryMethodsI.readDF(spark, "hired_employees");
                Dataset<Row> departments = auxiliaryMethodsI.readDF(spark, "departments");
                return requestServiceI.hiredEmployees(hired_employees, jobs, departments).toJSON().collectAsList();
            }catch (Exception e){;
                return e;
            }
        });

        get("/api/data/query/sql2", (req, res) -> {
            try{
                Dataset<Row> hired_employees = auxiliaryMethodsI.readDF(spark, "hired_employees");
                Dataset<Row> departments = auxiliaryMethodsI.readDF(spark, "departments");
                return requestServiceI.hiredEmployeesByDepartment(hired_employees, departments).toJSON().collectAsList();
            }catch (Exception e){
                return e;
            }
        });
    }
}
