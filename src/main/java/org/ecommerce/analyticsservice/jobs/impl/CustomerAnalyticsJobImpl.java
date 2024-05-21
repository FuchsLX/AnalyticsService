package org.ecommerce.analyticsservice.jobs.impl;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.ecommerce.analyticsservice.constants.table.CustomerMetaTable;
import org.ecommerce.analyticsservice.constants.table.CustomerTable;
import org.ecommerce.analyticsservice.constants.table.OrderTable;
import org.ecommerce.analyticsservice.constants.table.RoleTable;
import org.ecommerce.analyticsservice.jobs.CustomerAnalyticsJob;
import org.ecommerce.analyticsservice.utils.ExtractOperationUtil;
import org.ecommerce.analyticsservice.utils.LoadOperationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import static org.apache.spark.sql.functions.*;

@Component
@RequiredArgsConstructor
public class CustomerAnalyticsJobImpl implements CustomerAnalyticsJob {

    private final ExtractOperationUtil extractOperationUtil;
    private final LoadOperationUtil loadOperationUtil;

    private static final Logger logger = LoggerFactory.getLogger(CustomerAnalyticsJobImpl.class);

    @Override
    public void start() {
        Dataset<Row> userDf = extractOperationUtil.ingest(CustomerTable.class);
        Dataset<Row> customerMetaDf = extractOperationUtil.ingest(CustomerMetaTable.class);
        Dataset<Row> roleDf = extractOperationUtil.ingest(RoleTable.class);
        Dataset<Row> orderDf = extractOperationUtil.ingest(OrderTable.class);


        userDf = userDf.repartition(10);
        customerMetaDf = customerMetaDf.repartition(10);
        roleDf = roleDf.repartition(10);
        orderDf = orderDf.repartition(10);


        userDf = userDf
                .drop("is_account_non_expired")
                .drop("is_account_non_locked")
                .drop("is_credentials_non_expired")
                .drop("password");

        customerMetaDf = customerMetaDf
                .withColumnRenamed("id", "customer_meta_id")
                .withColumnRenamed("user_id", "customer_id")
                .withColumnRenamed("user_meta_gender", "gender")
                .withColumn("full_name", concat_ws(" ", col("first_name"), col("middle_name"), col("last_name")))
                .drop("first_name")
                .drop("middle_name")
                .drop("last_name");

        roleDf = roleDf.drop("description");

        orderDf = orderDf
                .filter(col("status").equalTo("COMPLETED"))
                .groupBy(col("customer_id"))
                .agg(count(col("id")).as("order_count"));

        userDf = userDf
                .join(roleDf, userDf.col("role_id").equalTo(roleDf.col("id")), "inner")
                .filter(roleDf.col("name").equalTo("CUSTOMER"))
                .drop(roleDf.col("id"), roleDf.col("name"), userDf.col("role_id"));

        userDf = userDf
                .join(customerMetaDf, userDf.col("id").equalTo(customerMetaDf.col("customer_id")), "left_outer")
                .drop(customerMetaDf.col("customer_id"));

        userDf = userDf
                .join(orderDf, userDf.col("id").equalTo(orderDf.col("customer_id")), "left_outer")
                .drop(orderDf.col("customer_id"));

        loadOperationUtil.save(CustomerTable.CUSTOMER_WH, userDf);
    }
}
