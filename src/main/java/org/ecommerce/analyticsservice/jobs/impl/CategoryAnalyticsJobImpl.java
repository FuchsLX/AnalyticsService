package org.ecommerce.analyticsservice.jobs.impl;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.ecommerce.analyticsservice.constants.*;
import org.ecommerce.analyticsservice.jobs.CategoryAnalyticsJob;
import org.ecommerce.analyticsservice.models.Category;
import org.ecommerce.analyticsservice.mapper.CategoryMapper;
import org.ecommerce.analyticsservice.utils.ExtractOperationUtil;
import org.ecommerce.analyticsservice.utils.LoadOperationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import scala.collection.JavaConverters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.*;

@Component
@RequiredArgsConstructor
public class CategoryAnalyticsJobImpl implements CategoryAnalyticsJob {

    private final ExtractOperationUtil extractOperationUtil;
    private final LoadOperationUtil loadOperationUtil;

    private static final Logger logger = LoggerFactory.getLogger(CategoryAnalyticsJobImpl.class);

    @Override
    public void start() {
        Dataset<Row> categoryDf = extractOperationUtil.ingest(CategoryTable.class);
        Dataset<Row> productCategoryDf = extractOperationUtil.ingest(ProductCategoryTable.class);
        Dataset<Row> orderDf = extractOperationUtil.ingest(OrderTable.class);
        Dataset<Row> orderItemDf = extractOperationUtil.ingest(OrderItemTable.class);
        Dataset<Row> ratingDf = extractOperationUtil.ingest(RatingTable.class);
        logger.debug("EXTRACTED dataframes in {}", CategoryAnalyticsJob.class.getName());

        categoryDf = categoryDf.repartition(10);
        productCategoryDf = productCategoryDf.repartition(10);
        orderDf = orderDf.repartition(10);
        orderItemDf = orderItemDf.repartition(10);
        ratingDf = ratingDf.repartition(10);
        logger.debug("REPARTITION dataframes in {}", CategoryAnalyticsJob.class.getName());

        categoryDf = categoryDf
                .drop("created_at")
                .drop("updated_at")
                .drop("slug")
                .drop("meta_title")
                .drop("content")
                .withColumnRenamed("id", "category_id")
                .withColumnRenamed("title", "category_name")
                .withColumnRenamed("parent_id", "category_parent_id");

        categoryDf = this.joinSpec(categoryDf, productCategoryDf);

        ratingDf = ratingDf.drop("created_at")
                .drop("updated_at")
                .drop("content")
                .drop("customer_id")
                .withColumnRenamed("id", "rating_id");

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

        orderItemDf = orderItemDf
                .join(orderDf, orderItemDf.col("order_id").equalTo(orderDf.col("order_id")), "inner")
                .drop(orderItemDf.col("order_id"));

        this.categoryProductCounterTask(categoryDf);
        this.categoryRatingTask(categoryDf, ratingDf);
        this.categoryWithOrderTask(categoryDf, orderItemDf);
    }

    private void categoryWithOrderTask(Dataset<Row> categoryDf, Dataset<Row> orderItemDf) {
        categoryDf = categoryDf
                .join(orderItemDf, orderItemDf.col("product_id").equalTo(categoryDf.col("product_id")), "left_outer")
                .drop(orderItemDf.col("product_id"));

        Dataset<Row> repeatBuyersDf = orderItemDf
                .filter(col("order_status").equalTo("COMPLETED"))
                .groupBy(col("product_id"), col("customer_id"))
                .agg(count(col("order_item_id")).as("order_count"))
                .filter(col("order_count").gt(1))
                .groupBy(col("product_id"))
                .agg(count(col("customer_id")).as("repeat_buyers_count"));

        if (repeatBuyersDf.count() == 0) {
            categoryDf = categoryDf
                    .withColumn(
                            "repeat_buyers_count",
                            when(col("product_id").isNotNull(), lit(0))
                                    .otherwise(null)
                    );
        } else {
            categoryDf = categoryDf
                    .join(repeatBuyersDf, repeatBuyersDf.col("product_id").equalTo(categoryDf.col("product_id")), "left_outer")
                    .drop(repeatBuyersDf.col("product_id"));
        }

        categoryDf = categoryDf
                .groupBy(col("category_id"), col("category_name"))
                .agg(
                        count(when(col("order_status").equalTo("COMPLETED"), true)).as("completed_quantity"),
                        count(when(col("order_status").equalTo("CANCELLED"), true)).as("cancelled_quantity"),
                        count(when(col("order_status").equalTo("DELIVERED"), true)).as("delivered_quantity"),
                        count(when(col("order_status").equalTo("PROCESSING"), true)).as("processing_quantity"),
                        sum(when(col("order_status").equalTo("COMPLETED"), col("total_price_after_discount")).otherwise(0)).as("completed_total_price"),
                        sum(when(col("order_status").equalTo("CANCELLED"), col("total_price_after_discount")).otherwise(0)).as("cancelled_total_price"),
                        sum(when(col("order_status").equalTo("DELIVERED"), col("total_price_after_discount")).otherwise(0)).as("delivered_total_price"),
                        sum(when(col("order_status").equalTo("PROCESSING"), col("total_price_after_discount")).otherwise(0)).as("processing_total_price"),
                        count_distinct(when(col("order_status").equalTo("COMPLETED"), col("customer_id"))).as("unique_buyers_count"),
                        sum(col("repeat_buyers_count")).as("repeat_buyers_count")
                );

        categoryDf = categoryDf.withColumn(
                "reorder_rate",
                when(col("unique_buyers_count").equalTo(0), lit(null))
                        .otherwise(col("repeat_buyers_count").divide(col("unique_buyers_count")).multiply(100))
        );
        logger.debug("TRANSFORMED `{}` table !!!", CategoryTable.CATEGORY_ORDER_WH);
        categoryDf.printSchema();
        categoryDf.show(5, false);
        loadOperationUtil.save(CategoryTable.CATEGORY_ORDER_WH, categoryDf);
        logger.debug("SAVED `{}` table !!!", CategoryTable.CATEGORY_ORDER_WH);
    }

    private void categoryProductCounterTask(Dataset<Row> categoryDf) {
        categoryDf = categoryDf
                .groupBy(col("category_id"), col("category_name"))
                .agg(count(col("product_id")).as("product_count"));
        logger.debug("TRANSFORMED `{}` table !!!", CategoryTable.CATEGORY_PRODUCT_COUNTER_WH);
        categoryDf.printSchema();
        categoryDf.show(5, false);
        loadOperationUtil.save(CategoryTable.CATEGORY_PRODUCT_COUNTER_WH, categoryDf);
        logger.debug("SAVED `{}` table !!!", CategoryTable.CATEGORY_PRODUCT_COUNTER_WH);
    }

    private void categoryRatingTask(Dataset<Row> categoryDf, Dataset<Row> ratingDf) {
        categoryDf = categoryDf
                .join(ratingDf, ratingDf.col("product_id").equalTo(categoryDf.col("product_id")), "left_outer")
                .groupBy(col("category_id"), col("category_name"))
                .agg(
                        count(when(col("rating").equalTo(1), true)).alias("one_star_rating_count"),
                        count(when(col("rating").equalTo(2), true)).alias("two_star_rating_count"),
                        count(when(col("rating").equalTo(3), true)).alias("three_star_rating_count"),
                        count(when(col("rating").equalTo(4), true)).alias("four_star_rating_count"),
                        count(when(col("rating").equalTo(5), true)).alias("five_star_rating_count"),
                        avg(col("rating")).alias("rating_avg"),
                        count(col("rating_id")).alias("rating_count")
                );
        logger.debug("TRANSFORMED `{}` table !!!", CategoryTable.CATEGORY_RATING_WH);
        categoryDf.printSchema();
        categoryDf.show(5, false);
        loadOperationUtil.save(CategoryTable.CATEGORY_RATING_WH, categoryDf);
        logger.debug("SAVED `{}` table !!!", CategoryTable.CATEGORY_RATING_WH);
    }

    private Dataset<Row> joinSpec(Dataset<Row> categoryDf, Dataset<Row> productCategoryDf) {
        StructType productCategorySchema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("product_id", DataTypes.StringType, false),
                DataTypes.createStructField("category_id", DataTypes.StringType, false),
        });

        productCategoryDf = productCategoryDf
                .groupBy(col("product_id"))
                .agg(collect_list(col("category_id")).as("categories_id"));
        productCategoryDf.printSchema();
        productCategoryDf.show(false);

        Map<String, List<String>> productCategoriesMap = new HashMap<>();
        for (Row row: productCategoryDf.collectAsList()) {
            productCategoriesMap.put(
                    row.getAs("product_id"),
                    JavaConverters.seqAsJavaListConverter(row.<String>getSeq(1)).asJava()
            );
        }

        List<Category> categoriesList = categoryDf.map(new CategoryMapper(), Encoders.bean(Category.class)).collectAsList();
        Map<String, Category> categoriesMap = new HashMap<>();
        categoriesList.forEach(c -> categoriesMap.put(c.getId(), c));

        productCategoriesMap.forEach((productId, categoriesId) -> {
            categoriesId.forEach(cId -> {
                String parentId = categoriesMap.get(cId).getParentId();
                while (parentId != null) {
                    if (!categoriesId.contains(parentId)) categoriesId.add(parentId);
                    parentId = categoriesMap.get(parentId).getParentId();
                }
            });
        });

        List<Row> productCategoryRows = new ArrayList<>();
        productCategoriesMap.forEach((productId, categoriesId) -> {
            categoriesId.forEach(cId -> {
                productCategoryRows.add(RowFactory.create(productId, cId));
            });
        });
        productCategoryDf = extractOperationUtil.ingest(productCategorySchema, productCategoryRows);
        productCategoryDf.show(false);
        return categoryDf
                .join(productCategoryDf, productCategoryDf.col("category_id").equalTo(categoryDf.col("category_id")), "left_outer")
                .drop(productCategoryDf.col("category_id"));
    }
}