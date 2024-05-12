package org.ecommerce.analyticsservice.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.ecommerce.analyticsservice.constants.BaseTable;

public interface LoadOperationUtil {
    void save(Class<? extends BaseTable> table, Dataset<Row> df);
}