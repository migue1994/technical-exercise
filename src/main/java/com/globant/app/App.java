package com.globant.app;

import com.globant.app.Controller.RequestController;
import com.globant.app.Services.RequestServiceImpl;
import org.apache.spark.sql.SparkSession;

public class App 
{
    public static void main( String[] args )
    {
        SparkSession spark = SparkSession.builder().master("local[*]").appName("globant").getOrCreate();
        new RequestController(spark, new RequestServiceImpl());
    }
}
