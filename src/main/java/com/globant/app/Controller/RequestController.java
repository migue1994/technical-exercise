package com.globant.app.Controller;

import com.globant.app.Services.RequestService;
import org.apache.spark.sql.SparkSession;

import static spark.Spark.get;

public class RequestController {
    public RequestController(final RequestService requestService, SparkSession spark){
        get("/api/data/:name", (req, res) -> {
            try{
                requestService.uploadData(spark);
                res.status(201);
                return "Success";
            }catch (Exception e){
                return res;
            }
        });
    }
}
