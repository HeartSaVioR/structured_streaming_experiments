# -*- coding: utf-8 -*-

def setup_catalog(spark_session_builder, options):
    catalog_name = options["iceberg-catalog-name"]
    catalog_class = options["iceberg-catalog-class"]
    catalog_type = options["iceberg-catalog-type"]

    if catalog_type == "hadoop":
        catalog_warehouse = options["iceberg-catalog-warehouse"]
        return spark_session_builder \
            .config("spark.sql.catalog.%s" % catalog_name, catalog_class) \
            .config("spark.sql.catalog.%s.type" % catalog_name, catalog_type) \
            .config("spark.sql.catalog.%s.warehouse" % catalog_name, catalog_warehouse)
    else:
        # catalog_type == "hive"
        catalog_hms_uri = options["iceberg-catalog-uri"]
        return spark_session_builder \
            .config("spark.sql.catalog.%s" % catalog_name, catalog_class) \
            .config("spark.sql.catalog.%s.type" % catalog_name, catalog_type) \
            .config("spark.sql.catalog.%s.uri" % catalog_name, catalog_hms_uri)
