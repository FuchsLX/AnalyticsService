package org.ecommerce.analyticsservice.utils;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.ecommerce.analyticsservice.constants.BaseTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.lang.reflect.Field;

@Component
@RequiredArgsConstructor
public class LoadOperationUtil {

    @Value("${database.destination.url}")
    private String destinationUrl;

    private static final Logger logger = LoggerFactory.getLogger(LoadOperationUtil.class);

    public void load(Class<? extends BaseTable> table, Dataset<Row> df) {
        String desTableName = "";
        try {
            Field desNameField = table.getField("DES_NAME");
            desTableName = desNameField.get(null).toString();
        } catch (NoSuchFieldException e) {
            logger.error("Could not find DES_NAME field in table {}", table.getName());
        } catch (IllegalAccessException e) {
            logger.error("Error when saving dataframe: {}", e.getMessage());
        }

        df.write()
                .mode(SaveMode.Overwrite)
                .option("dbtable", desTableName)
                .option("url", destinationUrl)
                .option("driver", "org.postgresql.Driver")
                .option("user", "postgres")
                .option("password", "123456789")
                .format("jdbc")
                .save();
    }
}
