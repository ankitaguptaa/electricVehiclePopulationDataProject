package org.singularityTechnologies;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.time.LocalDate;

import static org.apache.spark.sql.functions.*;
public class FactTable {
    public static void main(String[] args) {
        // Create SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("DataFrame Operations")
                .master("local[*]")
                .getOrCreate();
        FactTable  fct= new FactTable();
        Dataset<Row> employee = fct.readEmployee(spark);
        Dataset<Row> employeePersonal =fct.readEmployeePersonal(spark);

        Dataset<Row> fullName= employee.join(employeePersonal,"employee_id")
                .withColumn("employee_full_name",concat(expr("first_name"),lit(" "),expr("last_name")))
                .drop("first_name","last_name")
                ;

        int maxSalary = fullName.agg(max("salary")).first().getInt(0);
        int year = LocalDate.now().getYear();
        fullName.withColumn("salary_difference",lit(maxSalary).minus(col("salary")))
                .withColumn("age",lit(year).minus(year(to_date(col("DOB"),"dd/mm/yyyy"))))
                .show();

    }

    private  Dataset<Row> readEmployee(SparkSession spark) {
        Dataset<Row> employee =  spark.read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(getClass().getResource("/data/employee.csv").toString());
        return employee;
    }
    private  Dataset<Row> readEmployeePersonal(SparkSession spark) {
        Dataset<Row> employeePersonal =  spark.read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(getClass().getResource("/data/employee_personal.csv").toString());
        return employeePersonal;
    }
}