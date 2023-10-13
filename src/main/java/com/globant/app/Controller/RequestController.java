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
                res.status(201);
                return "Success";
            }catch (Exception e){
                res.status(401);
                return "Failed";
            }
        });


    }
}
