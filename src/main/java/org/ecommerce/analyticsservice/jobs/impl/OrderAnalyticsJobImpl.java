package org.ecommerce.analyticsservice.jobs.impl;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.ecommerce.analyticsservice.constants.table.OrderItemTable;
import org.ecommerce.analyticsservice.constants.table.OrderTable;
import org.ecommerce.analyticsservice.jobs.OrderAnalyticsJob;
import org.ecommerce.analyticsservice.utils.ExtractOperationUtil;
import org.ecommerce.analyticsservice.utils.LoadOperationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;

@Component
@RequiredArgsConstructor
public class OrderAnalyticsJobImpl implements OrderAnalyticsJob {

    private final ExtractOperationUtil extractOperationUtil;
    private final LoadOperationUtil loadOperationUtil;

    private static final Logger logger = LoggerFactory.getLogger(OrderAnalyticsJobImpl.class);

    @Override
    public void start() {
        Dataset<Row> orderDf = extractOperationUtil.ingest(OrderTable.class);
        Dataset<Row> orderItemDf = extractOperationUtil.ingest(OrderItemTable.class);
        logger.debug("EXTRACTED `orders` table and `order_items` table in {}", OrderAnalyticsJob.class.getName());

        orderDf = orderDf.repartition(10);
        orderItemDf = orderItemDf.repartition(10);

        // PREPARING, CLEANING
        orderDf = orderDf
                .drop("item_discount")
                .drop("created_at")
                .drop("updated_at")
                .withColumnRenamed("sub_total", "total_price_before_discount")
                .withColumnRenamed("total", "total_price_after_discount")
                .withColumnRenamed("id", "order_id")
                .withColumnRenamed("status", "order_status")
                .withColumn("full_name", concat_ws(" ", col("first_name"), col("middle_name"), col("last_name")))
                .drop("first_name")
                .drop("middle_name")
                .drop("last_name");

        orderItemDf = orderItemDf
                .drop("create_at")
                .drop("updated_at")
                .drop("discount")
                .withColumnRenamed("price", "order_item_price")
                .withColumnRenamed("id", "order_item_id");

        long originalOrderRecordCount = orderDf.count();

        // TRANSFORM
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("order_status", DataTypes.StringType, false),
                DataTypes.createStructField("quantity", DataTypes.LongType, false)
        });

        List<Row> rows = Arrays.asList(
                RowFactory.create("COMPLETED", orderDf.agg(count(when(col("order_status").equalTo("COMPLETED"), true))).first().getLong(0)),
                RowFactory.create("PROCESSING", orderDf.agg(count(when(col("order_status").equalTo("PROCESSING"), true))).first().getLong(0)),
                RowFactory.create("DELIVERED", orderDf.agg(count(when(col("order_status").equalTo("DELIVERED"), true))).first().getLong(0)),
                RowFactory.create("CANCELLED", orderDf.agg(count(when(col("order_status").equalTo("CANCELLED"), true))).first().getLong(0)),
                RowFactory.create("TOTAL", originalOrderRecordCount)
        );

        Dataset<Row> orderStatusDf = extractOperationUtil.ingest(schema, rows);
        Dataset<Row> totalPriceWithOrderStatusDf = orderDf
                .groupBy(col("order_status"))
                .agg(
                        sum(col("total_price_after_discount")).as("total_price_after_discount"),
                        sum(col("total_price_before_discount")).as("total_price_before_discount")
                );
        orderStatusDf = orderStatusDf
                .join(totalPriceWithOrderStatusDf, orderStatusDf.col("order_status").equalTo(totalPriceWithOrderStatusDf.col("order_status")), "inner")
                .drop(totalPriceWithOrderStatusDf.col("order_status"));
        logger.debug("TRANSFORMED `{}` dataframe: ", OrderTable.ORDER_WITH_STATUS_WH);
        orderStatusDf.show(true);

        // LOAD
        loadOperationUtil.save(OrderTable.ORDER_WITH_STATUS_WH, orderStatusDf);
        logger.debug("SAVED `{}` table !", OrderTable.ORDER_WITH_STATUS_WH);
    }
}