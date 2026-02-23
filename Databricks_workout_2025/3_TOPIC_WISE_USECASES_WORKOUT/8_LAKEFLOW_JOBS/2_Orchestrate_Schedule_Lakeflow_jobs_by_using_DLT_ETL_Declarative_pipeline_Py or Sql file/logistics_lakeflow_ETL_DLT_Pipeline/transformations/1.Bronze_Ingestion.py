from pyspark import pipelines as dp

@dp.table(name = 'bronze_staff_data1')
def bronze_staff_data():
    base_path = spark.conf.get("base_path")
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format","csv")
        .option("inferColumnTypes","true")
        .option("schemaEvolutionMode","addNewColumns")
        .load(f"{base_path}/staff/")
    )

@dp.table(name="bronze_geotag_data1")
def bronze_geotag_data():
    base_path = spark.conf.get("base_path")
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("inferColumnTypes", "true")
            .load(f"{base_path}/geotag/")
    )


@dp.table(name="bronze_shipments_data1")
def bronze_shipments_data():
    base_path = spark.conf.get("base_path")
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("inferColumnTypes", "true")
            .option("multiLine", "true")
            .load(f"{base_path}/shipment/")
            .select(
                "shipment_id",
                "order_id",
                "source_city",
                "destination_city",
                "shipment_status",
                "cargo_type",
                "vehicle_type",
                "payment_mode",
                "shipment_weight_kg",
                "shipment_cost",
                "shipment_date"
            )
    )
