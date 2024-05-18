package org.ecommerce.analyticsservice.utils.impl;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.ecommerce.analyticsservice.constants.table.BaseTable;
import org.ecommerce.analyticsservice.utils.ExtractOperationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Properties;

@Component
@RequiredArgsConstructor
public class ExtractOperationUtilImpl implements ExtractOperationUtil {

    @Value("${database.source.url}")
    private String dataSourceUrl;

    @Value("${database.source.username}")
    private String dataSourceUser;

    @Value("${database.source.password}")
    private String dataSourcePassword;

    @Value("${database.destination.url}")
    private String destinationUrl;

    @Value("${database.destination.username}")
    private String desDatabaseUser;

    @Value("${database.destination.password}")
    private String desDatabasePassword;

    private final SparkSession sparkSession;
    private static final Logger logger = LoggerFactory.getLogger(ExtractOperationUtilImpl.class);


    @Override
    public Dataset<Row> ingest(Class<? extends BaseTable> table) {
        Properties properties = new Properties();
        properties.setProperty("user", dataSourceUser);
        properties.setProperty("password", dataSourcePassword);
        String srcTableName = "";
        try {
            srcTableName = table.getField("SRC_NAME").get(null).toString();
        } catch (NoSuchFieldException | IllegalAccessException e) {
            logger.error("Error when extracting from database: {}", e.getMessage());
        }
        return sparkSession.read().jdbc(dataSourceUrl, srcTableName, properties);
    }

    @Override
    public Dataset<Row> ingest(String tableName) {
        return this.ingest(tableName, false);
    }

    @Override
    public Dataset<Row> ingest(String tableName, boolean fromDataWarehouse) {
        Properties properties = new Properties();
        String databaseUrl;
        if (fromDataWarehouse) {
            properties.setProperty("user", desDatabaseUser);
            properties.setProperty("password", desDatabasePassword);
            databaseUrl = destinationUrl;
        } else {
            properties.setProperty("user", dataSourceUser);
            properties.setProperty("password", dataSourcePassword);
            databaseUrl = dataSourceUrl;
        }
        return sparkSession.read().jdbc(databaseUrl, tableName, properties);
    }

    @Override
    public Dataset<Row> ingest(StructType schema, List<Row> rows) {
        return sparkSession.createDataFrame(rows, schema);
    }
}
