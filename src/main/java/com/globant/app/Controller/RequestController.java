package com.globant.app.Controller;

import com.globant.app.Interface.RequestServiceI;
import org.apache.spark.sql.SparkSession;

import static spark.Spark.get;

public class RequestController {

    private final RequestServiceI requestServiceI;
    private final SparkSession spark;

    public RequestController(SparkSession spark, RequestServiceI requestServiceI){
        this.requestServiceI = requestServiceI;
        this.spark = spark;
        mainApiRest();
    }

    private void mainApiRest(){
        get("/api/data/:fileName", (req, res) -> {
            try{
                requestServiceI.uploadData(spark, req.params(":fileName"));
                return String.format("The file %s has been uploaded correctly!", req.params(":fileName"));
            }catch (Exception e){
                return e;
            }
        });

        get("/api/data/query/sql", (req, res) -> {
            try{
                return requestServiceI.hiredEmployees(spark).toJSON().collectAsList();
            }catch (Exception e){;
                return e;
            }
        });

        get("/api/data/query/sql2", (req, res) -> {
            try{
                res.status(201);
                return requestServiceI.hiredEmployeesByDepartment(spark).toJSON().collectAsList();
            }catch (Exception e){;
                return e;
            }
        });
    }
}
