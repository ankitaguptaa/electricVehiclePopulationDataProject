package org.singularityTechnologies;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
    import static org.apache.spark.sql.functions.*;


public class ElectricVehicle {


    SparkSession spark;

    Dataset<Row> fileReadDS = null;

    public ElectricVehicle() {
        this.spark = SparkSession.builder()
                .appName("Read CSV Example")
                .master("local[*]")
                .config("spark.default.parallelism", "3") // Use all available cores
                .getOrCreate();

    }

    public void getTopThreeUsedVehiclesSQL() {
        final Logger logger = Logger.getLogger(ElectricVehicle.class);

        SparkSession spark = SparkSession.builder()
                .appName("Read CSV Example")
                .master("local[*]") // Use all available cores
                .getOrCreate();
        Dataset<Row> df = getElectricVehicalFromFile(spark);
        df.createOrReplaceTempView("vehicles");
        Dataset<Row> res = spark.sql("select " +
                "city,make " +
                "from " +
                "vehicles " +
                "group by city,make");
        res.show();
    }

    private Dataset<Row> getElectricVehicalFromFile(SparkSession spark) {
        if (fileReadDS == null) {
            fileReadDS = spark.read()
                    .format("csv")
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .load(getClass().getResource("/data/Electric_Vehicle_Population_Data.csv").toString());
        }
        return fileReadDS;
    }

    public void getTopThreeUsedVehiclesDS() {

        Dataset<Row> ds = getElectricVehicalFromFile(spark);
        Dataset<Row> result = ds.select("city", "make")
                .groupBy("city", "make")
                .count()
                .orderBy(col("count").desc());
       //   ds.select( ds.col("Electric Range")*2).show();
        //result.show();

        // Expression to check column
        //Dataset<Row> result2 = ds.withColumn("ERHigh",expr("`Electric Range` < 100"));

        //Concatenate columns and create new column
//        ds.withColumn("NewColumn",concat(expr("County"),expr("City")))
//        .select("County","City","NewColumn")
//        .show();

        //distinct
        ds.select("make")
                .where(col("make").isNotNull())
                .distinct()
                .show();

    }

    @Override
    protected void finalize() throws Throwable {
        spark.stop();
        super.finalize();
    }


}
