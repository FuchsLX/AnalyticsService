package org.ecommerce.analyticsservice.utils.impl;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.ecommerce.analyticsservice.utils.LoadOperationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;


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
    public void save(String tableName, Dataset<Row> df) {
        df.write()
                .mode(SaveMode.Overwrite)
                .option("dbtable", tableName)
                .option("url", destinationUrl)
                .option("driver", "org.postgresql.Driver")
                .option("user", desDatabaseUser)
                .option("password", desDatabasePassword)
                .format("jdbc")
                .save();
        logger.debug("LOADED dataset: {} in the Data warehouse", tableName);
    }
}
