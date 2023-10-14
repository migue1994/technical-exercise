package com.globant.app;

import com.globant.app.Services.AuxiliaryMethodsImpl;
import com.globant.app.Controller.RequestController;
import com.globant.app.Services.RequestServiceImpl;
import org.apache.spark.sql.SparkSession;

public class App 
{
    public static void main( String[] args )
    {
        SparkSession spark = SparkSession.builder().master("local[*]")
                .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
                .appName("globant")
                .getOrCreate();

        AuxiliaryMethodsImpl auxiliaryMethodsImpl = new AuxiliaryMethodsImpl();

        new RequestController(spark, new RequestServiceImpl(auxiliaryMethodsImpl));
    }
}
