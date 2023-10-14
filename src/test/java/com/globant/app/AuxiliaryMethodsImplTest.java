package com.globant.app;

import com.globant.app.Interface.AuxiliaryMethodsI;
import com.globant.app.Services.AuxiliaryMethodsImpl;
import junit.framework.TestCase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import static org.junit.Assert.assertArrayEquals;
import static org.apache.spark.sql.functions.*;

public class AuxiliaryMethodsImplTest extends TestCase {
    private final AuxiliaryMethodsI auxiliaryMethodsI = new AuxiliaryMethodsImpl();
    private final SparkSession spark = SparkSession.builder().master("local[*]").getOrCreate();

    public void testGetSchema(){
        StructType schema = auxiliaryMethodsI.getSchema("jobs");

        Dataset<Row> df = spark.read().schema(schema).csv("src/test/resources/testData/jobsTest.csv");

        String[] columns = new String[]{"id", "job"};

        assertArrayEquals(columns, df.columns());
    }

    public void testReadDF(){
        Dataset<Row> df = auxiliaryMethodsI.readDF(spark, "departmentsTest");

        assertEquals(df.count(), 12);
        assertEquals(df.columns().length, 2);
    }

    public void testDateBetweenCondition(){
        Dataset<Row> df = auxiliaryMethodsI.readDF(spark, "readDataTest");

        int numRows = df
                .select(sum(auxiliaryMethodsI.dateBetweenCondition("2020-12-31", "2021-03-31")).cast("int")).first().getInt(0);

        assertEquals(numRows, 10);
    }
}
