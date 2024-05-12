package org.ecommerce.analyticsservice.utils.impl;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.ecommerce.analyticsservice.constants.BaseTable;
import org.ecommerce.analyticsservice.utils.LoadOperationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.lang.reflect.Field;

@Component
@RequiredArgsConstructor
public class LoadOperationUtilImpl implements LoadOperationUtil {

    @Value("${database.destination.url}")
    private String destinationUrl;

    @Value("${database.destination.username}")
    private String desDatabaseUser;

    @Value("${database.destination.password}")
    private String desDatabasePassword;

    private static final Logger logger = LoggerFactory.getLogger(LoadOperationUtilImpl.class);

    @Override
    public void save(Class<? extends BaseTable> table, Dataset<Row> df) {
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
                .option("user", desDatabaseUser)
                .option("password", desDatabasePassword)
                .format("jdbc")
                .save();
    }
}
