package com.globant.app;

import org.apache.spark.sql.SparkSession;

import static spark.Spark.*;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        SparkSession spark = SparkSession.builder().master("local[*]").appName("globant").getOrCreate();

        get("/api/data/:name", (request, response) -> {
            return "Hello, " + request.params(":name");
        });
    }
}
