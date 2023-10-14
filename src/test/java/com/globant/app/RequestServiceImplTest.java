package com.globant.app;

import com.globant.app.Interface.AuxiliaryMethodsI;
import com.globant.app.Interface.RequestServiceI;
import com.globant.app.Services.AuxiliaryMethodsImpl;
import com.globant.app.Services.RequestServiceImpl;
import junit.framework.TestCase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class RequestServiceImplTest extends TestCase {
    private final AuxiliaryMethodsI auxiliaryMethodsI = new AuxiliaryMethodsImpl();
    private final RequestServiceI requestService = new RequestServiceImpl(auxiliaryMethodsI);
    private final SparkSession spark = SparkSession.builder().master("local[*]").getOrCreate();


    public void testUploadData(){
        String basePath = "src/test/resources/testData/";
        requestService.uploadData(spark, basePath, "readDataTest", "hired_employees");
        Dataset<Row> df = spark.read().format("jdbc")
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("url", "jdbc:mysql://sql10.freesqldatabase.com:3306/sql10652684")
                .option("dbtable", "readDataTest")
                .option("user", "sql10652684")
                .option("password", "Yt3AJBSqPk")
                .load();

        assertEquals(df.count(), 10);
        assertEquals(df.columns().length, 5);
    }

    public void testHiredEmployees(){
        Dataset<Row> jobs = auxiliaryMethodsI.readDF(spark, "jobsTest");
        Dataset<Row> hired_employees = auxiliaryMethodsI.readDF(spark, "readDataTest");
        Dataset<Row> departments = auxiliaryMethodsI.readDF(spark, "departmentsTest");

        Dataset<Row> result = requestService.hiredEmployees(hired_employees, jobs, departments);

        assertEquals(result.count(), 1);
        assertEquals(result.columns().length, 6);
    }

    public  void testHiredEmployeesByDepartment(){
        Dataset<Row> hired_employees = auxiliaryMethodsI.readDF(spark, "readDataTest");
        Dataset<Row> departments = auxiliaryMethodsI.readDF(spark, "departmentsTest");

        Dataset<Row> result = requestService.hiredEmployeesByDepartment(hired_employees, departments);

        assertEquals(result.count(), 1);
    }

}
