package org.ecommerce.analyticsservice.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.ecommerce.analyticsservice.constants.BaseTable;

import java.util.List;

public interface ExtractOperationUtil {

    // ingest from Data Source
    Dataset<Row> ingest(Class<? extends BaseTable> table);

    // ingest from Data Source
    Dataset<Row> ingest(String tableName);

    // ingest from Data Source or Data Warehouse
    Dataset<Row> ingest(String tableName, boolean fromDataWarehouse);

    Dataset<Row> ingest(StructType schema, List<Row> rows);
}
