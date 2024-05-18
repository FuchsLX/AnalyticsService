package org.ecommerce.analyticsservice.jobs.impl;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.ecommerce.analyticsservice.constants.table.*;
import org.ecommerce.analyticsservice.jobs.ProductAnalyticsJob;
import org.ecommerce.analyticsservice.utils.ExtractOperationUtil;
import org.ecommerce.analyticsservice.utils.LoadOperationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import static org.apache.spark.sql.functions.*;

@Component
@RequiredArgsConstructor
public class ProductAnalyticsJobImpl implements ProductAnalyticsJob {

    private final ExtractOperationUtil extractOperationUtil;
    private final LoadOperationUtil loadOperationUtil;

    private static final Logger logger = LoggerFactory.getLogger(ProductAnalyticsJobImpl.class);

    @Override
    public void start() {
        Dataset<Row> productDf = extractOperationUtil.ingest(ProductTable.class);
        Dataset<Row> categoryDf = extractOperationUtil.ingest(CategoryTable.class);
        Dataset<Row> productCategoryDf = extractOperationUtil.ingest(ProductCategoryTable.class);
        Dataset<Row> ratingDf = extractOperationUtil.ingest(RatingTable.class);
        Dataset<Row> tagDf = extractOperationUtil.ingest(TagTable.class);
        Dataset<Row> productTagDf = extractOperationUtil.ingest(ProductTagTable.class);
        Dataset<Row> orderDf = extractOperationUtil.ingest(OrderTable.class);
        Dataset<Row> orderItemDf = extractOperationUtil.ingest(OrderItemTable.class);
        logger.debug("EXTRACTED dataframes in {} !!!", ProductAnalyticsJob.class.getName());

        productDf = productDf.repartition(10);
        categoryDf = categoryDf.repartition(10);
        productCategoryDf = productCategoryDf.repartition(10);
        ratingDf = ratingDf.repartition(10);
        tagDf = tagDf.repartition(10);
        productTagDf = productTagDf.repartition(10);
        orderDf = orderDf.repartition(10);
        orderItemDf = orderItemDf.repartition(10);

        // PREPARING, CLEANING
        productDf = productDf.drop("user_id")
                .drop("created_at")
                .drop("updated_at")
                .drop("content")
                .drop("meta_title")
                .drop("sku")
                .drop("slug")
                .drop("image_link")
                .drop("summary")
                .drop("starts_at")
                .drop("ends_at")
                .drop("published_at")
                .drop("shop")
                .withColumnRenamed("id", "product_id")
                .withColumnRenamed("title", "product_name");

        ratingDf = ratingDf.drop("created_at")
                .drop("updated_at")
                .drop("content")
                .drop("customer_id")
                .withColumnRenamed("id", "rating_id");

        categoryDf = categoryDf
                .drop("created_at")
                .drop("updated_at")
                .drop("slug")
                .drop("meta_title")
                .drop("content")
                .withColumnRenamed("id", "category_id")
                .withColumnRenamed("title", "category_name")
                .withColumnRenamed("parent_id", "category_parent_id");

        productCategoryDf = productCategoryDf
                .join(categoryDf, categoryDf.col("category_id").equalTo(productCategoryDf.col("category_id")), "inner")
                .drop(productCategoryDf.col("category_id"));


        tagDf = tagDf.drop("created_at")
                .drop("updated_at")
                .drop("content")
                .drop("meta_title")
                .drop("slug")
                .withColumnRenamed("id", "tag_id")
                .withColumnRenamed("title", "tag_name");

        productTagDf = productTagDf
                .join(tagDf, tagDf.col("tag_id").equalTo(productTagDf.col("tag_id")), "inner")
                .drop(productTagDf.col("tag_id"));

        orderDf = orderDf
                .drop("item_discount")
                .drop("created_at")
                .drop("updated_at")
                .drop("address")
                .drop("first_name")
                .drop("last_name")
                .drop("middle_name")
                .drop("last_name")
                .drop("mobile")
                .withColumnRenamed("id", "order_id")
                .withColumnRenamed("status", "order_status")
                .withColumnRenamed("sub_total", "total_price_before_discount")
                .withColumnRenamed("total", "total_price_after_discount");

        orderItemDf = orderItemDf
                .drop("discount")
                .drop("created_at")
                .drop("updated_at")
                .withColumnRenamed("id", "order_item_id")
                .withColumnRenamed("price", "order_item_price")
                .withColumnRenamed("quantity", "order_item_quantity");

        this.productWithOrderTask(productDf, orderDf, orderItemDf);
        this.productWithTagAndCategoryTask(productDf, productCategoryDf, productTagDf);
        this.ratingAnalyticsTask(productDf, ratingDf);

    }

    private void productWithOrderTask(Dataset<Row> productDf, Dataset<Row> orderDf, Dataset<Row> orderItemDf) {
        orderItemDf = orderItemDf
                .join(orderDf, orderItemDf.col("order_id").equalTo(orderDf.col("order_id")), "inner")
                .drop(orderItemDf.col("order_id"));

        Dataset<Row> repeatBuyersDf = orderItemDf
                .filter(col("order_status").equalTo("COMPLETED"))
                .groupBy(col("product_id"), col("customer_id"))
                .agg(count(col("order_item_id")).as("order_count"))
                .filter(col("order_count").gt(1))
                .groupBy(col("product_id"))
                .agg(count(col("customer_id")).as("repeat_buyers_count"));

        orderItemDf = orderItemDf
                .groupBy(col("product_id"))
                .agg(
                        count(when(col("order_status").equalTo("COMPLETED"), true)).as("completed_quantity"),
                        count(when(col("order_status").equalTo("CANCELLED"), true)).as("cancelled_quantity"),
                        count(when(col("order_status").equalTo("DELIVERED"), true)).as("delivered_quantity"),
                        count(when(col("order_status").equalTo("PROCESSING"), true)).as("processing_quantity"),
                        sum(when(col("order_status").equalTo("COMPLETED"), col("total_price_after_discount")).otherwise(0)).as("completed_total_price"),
                        sum(when(col("order_status").equalTo("CANCELLED"), col("total_price_after_discount")).otherwise(0)).as("cancelled_total_price"),
                        sum(when(col("order_status").equalTo("DELIVERED"), col("total_price_after_discount")).otherwise(0)).as("delivered_total_price"),
                        sum(when(col("order_status").equalTo("PROCESSING"), col("total_price_after_discount")).otherwise(0)).as("processing_total_price"),
                        count_distinct(when(col("order_status").equalTo("COMPLETED"), col("customer_id"))).as("unique_buyers_count")
                );

        if (repeatBuyersDf.count() == 0) {
            orderItemDf = orderItemDf
                    .withColumn("repeat_buyers_count", lit(0));
        } else {
            orderItemDf = orderItemDf
                    .join(repeatBuyersDf, repeatBuyersDf.col("product_id").equalTo(orderItemDf.col("product_id")), "left_outer")
                    .drop(repeatBuyersDf.col("product_id"));
        }
        orderItemDf = orderItemDf.withColumn(
                "reorder_rate",
                        when(col("unique_buyers_count").equalTo(0), lit(null))
                                .otherwise(col("repeat_buyers_count").divide(col("unique_buyers_count").multiply(100)))
        );

        productDf = productDf
                .join(orderItemDf, orderItemDf.col("product_id").equalTo(productDf.col("product_id")), "inner")
                .drop(orderItemDf.col("product_id"));
        logger.debug("TRANSFORMED `{}` table !!!", ProductTable.PRODUCT_ORDER_WH);
        productDf.printSchema();
        productDf.show(5, false);
        loadOperationUtil.save(ProductTable.PRODUCT_ORDER_WH, productDf);
        logger.debug("SAVED `{}` table !!!", ProductTable.PRODUCT_ORDER_WH);
    }

    private void productWithTagAndCategoryTask(Dataset<Row> productDf, Dataset<Row> productCategoryDf, Dataset<Row> productTagDf) {
        productDf = productDf
                .join(productCategoryDf, productCategoryDf.col("product_id").equalTo(productDf.col("product_id")), "left_outer")
                .join(productTagDf, productTagDf.col("product_id").equalTo(productDf.col("product_id")), "left_outer")
                .drop(productCategoryDf.col("product_id"))
                .drop(productTagDf.col("product_id"))
                .drop(productDf.col("discount"))
                .drop(productDf.col("price"))
                .drop(productDf.col("quantity"))
                .groupBy(productDf.col("product_id"), productDf.col("product_name"))
                .agg(
                        collect_list(col("category_id")).as("category_ids"),
                        collect_list(col("category_name")).as("category_names"),
                        collect_list(col("tag_id")).as("tag_ids"),
                        collect_list(col("tag_name")).as("tag_names")
                );
        logger.debug("TRANSFORMED `{}` table !!", ProductTable.PRODUCT_CATEGORY_TAG_WH);
        productDf.printSchema();
        productDf.show(5, false);
        loadOperationUtil.save(ProductTable.PRODUCT_CATEGORY_TAG_WH, productDf);
        logger.debug("SAVED `{}` table !!!", ProductTable.PRODUCT_CATEGORY_TAG_WH);
    }

    private void ratingAnalyticsTask(Dataset<Row> productDf, Dataset<Row> ratingDf) {
        Dataset<Row> ratingAggDf = ratingDf
                .groupBy(col("product_id"))
                .agg(
                        count(when(col("rating").equalTo(1), true)).alias("one_star_rating_count"),
                        count(when(col("rating").equalTo(2), true)).alias("two_star_rating_count"),
                        count(when(col("rating").equalTo(3), true)).alias("three_star_rating_count"),
                        count(when(col("rating").equalTo(4), true)).alias("four_star_rating_count"),
                        count(when(col("rating").equalTo(5), true)).alias("five_star_rating_count"),
                        avg(col("rating")).alias("rating_avg"),
                        count(col("rating_id")).alias("rating_count")
                );

        productDf = productDf
                .select(col("product_id"), col("product_name"))
                .join(ratingAggDf, productDf.col("product_id").equalTo(ratingDf.col("product_id")), "left")
                .drop(ratingDf.col("product_id"));
        logger.debug("TRANSFORMED `{}` table", ProductTable.PRODUCT_RATING_WH);
        productDf.printSchema();
        productDf.show(5, false);
        loadOperationUtil.save(ProductTable.PRODUCT_RATING_WH, productDf);
        logger.debug("SAVED `{}` table !!!", ProductTable.PRODUCT_RATING_WH);
    }
}