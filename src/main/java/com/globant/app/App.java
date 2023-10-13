package com.globant.app;

import com.globant.app.Controller.RequestController;
import com.globant.app.Services.RequestService;
import org.apache.spark.sql.SparkSession;

import static spark.Spark.*;

public class App 
{
    private static RequestService RequestServiceI;
    public static void main( String[] args )
    {
        SparkSession spark = SparkSession.builder().master("local[*]").appName("globant").getOrCreate();

        new RequestController(new RequestService(), spark);
    }
}
