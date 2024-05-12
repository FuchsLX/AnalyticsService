package org.ecommerce.analyticsservice.utils.impl;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.ecommerce.analyticsservice.constants.BaseTable;
import org.ecommerce.analyticsservice.utils.ExtractOperationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.lang.reflect.Field;
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

    private final SparkSession sparkSession;
    private static final Logger logger = LoggerFactory.getLogger(ExtractOperationUtilImpl.class);

    @Override
    public Dataset<Row> ingest(Class<? extends BaseTable> table) {
        Properties properties = new Properties();
        properties.setProperty("user", dataSourceUser);
        properties.setProperty("password", dataSourcePassword);
        String srcTableName = "";
        try {
            Field srcNameField= table.getField("SRC_NAME");
            srcTableName = srcNameField.get(null).toString();
        } catch (NoSuchFieldException | IllegalAccessException e) {
            logger.error("Error when extracting from database: {}", e.getMessage());
        }
        return sparkSession.read().jdbc(dataSourceUrl, srcTableName, properties);
    }
}
