package com.company;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructType;

import static javafx.beans.binding.Bindings.select;

public class Main {



    public static void log(String s){
        System.out.print(s);
    }
    public static void main(String[] args) {
	// write your code here
        SparkSession spark = SparkSession.builder().
                master("local").
                appName("Spark Session Example").
                getOrCreate();

        Dataset<Row> peopleDFCsv = spark.read()
                .format("csv").option("sep", ",")
                .option("inferSchema", "true")
                .option("header", "true")
                .load("debugInfo_1533200012911fxd.csv");
        //	System.out.println(peopleDFCsv.schema());

        Dataset<Row> program1 = peopleDFCsv.select("program2").distinct();
        program1.show();
        //peopleDFCsv.select("program1").where("program1").show();
     /*   program1.foreach(row ->
        {
            String conditionExpr = "program1=" + row.getString(0);
            peopleDFCsv.select("program1").where(conditionExpr).show();
        });*/
    }
}
