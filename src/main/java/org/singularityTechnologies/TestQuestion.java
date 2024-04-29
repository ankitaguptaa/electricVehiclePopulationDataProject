package org.singularityTechnologies;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;


import static org.apache.spark.sql.functions.*;
public class TestQuestion {
        public static void main(String[] args) {
            // Create SparkSession
            SparkSession spark = SparkSession.builder()
                    .appName("DataFrame Operations")
                    .master("local[*]")
                    .getOrCreate();

            Dataset<Row> df1 = spark.createDataFrame(
                    java.util.Arrays.asList(
                            RowFactory.create(1, "John"),
                            RowFactory.create(2, "Jane"),
                            RowFactory.create(3, "Doe"),
                            RowFactory.create(4, "Pascal")
                    ),
                    DataTypes.createStructType(new StructField[] {
                            DataTypes.createStructField("id", DataTypes.IntegerType, false),
                            DataTypes.createStructField("name", DataTypes.StringType, false)
                    })
            );

            Dataset<Row> df2 = spark.createDataFrame(
                    java.util.Arrays.asList(
                            RowFactory.create(1, "Singapore", "Address1", "City1", 100),
                            RowFactory.create(2, "Singapore", "Address2", "City2", 200),
                            RowFactory.create(3, "Malaysia", "Address3", "City3", 150),
                            RowFactory.create(4, "Singapore", "Address4", "City2", 300)
                    ),
                    DataTypes.createStructType(new StructField[] {
                            DataTypes.createStructField("id", DataTypes.IntegerType, false),
                            DataTypes.createStructField("country", DataTypes.StringType, false),
                            DataTypes.createStructField("address", DataTypes.StringType, false),
                            DataTypes.createStructField("city", DataTypes.StringType, false),
                            DataTypes.createStructField("count", DataTypes.IntegerType, false)
                    })
            );

            // i) Join two dataframes and filter where the country is Singapore
            Dataset<Row> joinedDF = df1.join(df2, "id").filter(col("country").equalTo("Singapore"));
            Dataset<Row> countDF =  joinedDF.groupBy("city")
                    .sum("count")
                    .orderBy(col("sum(count)").desc());

            Row topRow = countDF.select("city").first();

            System.out.println(topRow);
            // ii) Pivot the output dataframe
            //Dataset<Row> pivotedDF = joinedDF.groupBy("country", "city").pivot("name").sum("count");

            // iii) Showcase the city of Singapore first whose count of people is highest on top


            // Show the final dataframe
            //pivotedDF.show();

            // Stop SparkSession
            spark.stop();
        }
}
