package org.ecommerce.analyticsservice.utils;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.ecommerce.analyticsservice.constants.BaseTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.lang.reflect.Field;
import java.util.Properties;

@Component
@RequiredArgsConstructor
public class IngestOperationUtil {

    @Value("${database.source.url}")
    private String dataSourceUrl;

    private final SparkSession sparkSession;
    private static final Logger logger = LoggerFactory.getLogger(IngestOperationUtil.class);

    public Dataset<Row> extract(Class<? extends BaseTable> table) {
        Properties properties = new Properties();
        properties.setProperty("user", "postgres");
        properties.setProperty("password", "123456789");
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
