package org.ecommerce.analyticsservice.services;

import com.google.common.collect.Lists;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.ecommerce.analyticsservice.constants.*;
import org.ecommerce.analyticsservice.models.Category;
import org.ecommerce.analyticsservice.models.Rating;
import org.ecommerce.analyticsservice.services.mapper.CategoryMapper;
import org.ecommerce.analyticsservice.utils.ExtractOperationUtil;
import org.ecommerce.analyticsservice.utils.LoadOperationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import javax.swing.*;
import java.util.*;

import static org.apache.spark.sql.functions.*;

@Service
@RequiredArgsConstructor
public class ProductAnalyticsService {
    private final ExtractOperationUtil extractOperation;
    private final LoadOperationUtil loadOperation;

    private static final Logger logger = LoggerFactory.getLogger(ProductAnalyticsService.class);

    public Rating getRating(String productId) {
        Dataset<Row> ratingDf = extractOperation.ingest(RatingTable.class);
        ratingDf = ratingDf.repartition(10);
        ratingDf.show(false);
        ratingDf = ratingDf
                .drop("created_at", "updated_at", "customer_id", "content")
                .filter(col("product_id").equalTo(productId));
        ratingDf.show(false);
        if (ratingDf.count() == 0) return Rating.builder().productId(productId)
                .oneStarCount(0).twoStarCount(0).threeStarCount(0).fourStarCount(0).fiveStarCount(0)
                .recordCount(0).average(0)
                .build();
        return Rating.builder()
                .productId(productId)
                .oneStarCount(ratingDf.agg(count(when(col("rating").equalTo(1), true))).first().getLong(0))
                .twoStarCount(ratingDf.agg(count(when(col("rating").equalTo(2), true))).first().getLong(0))
                .threeStarCount(ratingDf.agg(count(when(col("rating").equalTo(3), true))).first().getLong(0))
                .fourStarCount(ratingDf.agg(count(when(col("rating").equalTo(4), true))).first().getLong(0))
                .fiveStarCount(ratingDf.agg(count(when(col("rating").equalTo(5), true))).first().getLong(0))
                .average(ratingDf.agg(avg("rating")).first().getDouble(0))
                .recordCount(ratingDf.count())
                .build();
    }


    public void test() {
        Dataset<Row> productDf = extractOperation.ingest(ProductTable.class);
        Dataset<Row> categoryDf = extractOperation.ingest(CategoryTable.class);
        Dataset<Row> ratingDf = extractOperation.ingest(RatingTable.class);
        Dataset<Row> tagDf = extractOperation.ingest(TagTable.class);
        Dataset<Row> productTagDf = extractOperation.ingest(ProductTagTable.class);
        Dataset<Row> productCategoryDf = extractOperation.ingest(ProductCategoryTable.class);

        productDf = productDf.repartition(10);
        categoryDf = categoryDf.repartition(10);
        ratingDf = ratingDf.repartition(10);
        tagDf = tagDf.repartition(10);
        productTagDf = productTagDf.repartition(10);
        productCategoryDf = productCategoryDf.repartition(10);

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
                .withColumnRenamed("id", "product_id")
                .withColumnRenamed("title", "product_title");

        ratingDf = ratingDf.drop("created_at")
                .drop("updated_at")
                .drop("content")
                .drop("customer_id")
                .withColumnRenamed("id", "rating_id");

        Dataset<Row> ratingAggDf = ratingDf
                .groupBy(col("product_id"))
                .agg(
                        count(when(col("rating").equalTo(1), true)).alias("one_start_rating_count"),
                        count(when(col("rating").equalTo(2), true)).alias("two_start_rating_count"),
                        count(when(col("rating").equalTo(3), true)).alias("three_start_rating_count"),
                        count(when(col("rating").equalTo(4), true)).alias("four_start_rating_count"),
                        count(when(col("rating").equalTo(5), true)).alias("five_start_rating_count"),
                        avg(col("rating")).alias("rating_avg"),
                        count(col("rating_id")).alias("rating_count")
                );

        categoryDf = categoryDf.drop("created_at")
                .drop("updated_at")
                .drop("slug")
                .drop("meta_title")
                .drop("content")
                .withColumnRenamed("title", "category_title")
                .withColumnRenamed("parent_id", "category_parent_id");

        tagDf = tagDf.drop("created_at")
                .drop("updated_at")
                .drop("content")
                .drop("meta_title")
                .drop("slug")
                .withColumnRenamed("title", "tag_title");

        productTagDf = productTagDf
                .join(tagDf, productTagDf.col("tag_id").equalTo(tagDf.col("id")), "inner")
                .drop(tagDf.col("id"));
        productCategoryDf = productCategoryDf
                .join(categoryDf, productCategoryDf.col("category_id").equalTo(categoryDf.col("id")), "inner")
                .drop(categoryDf.col("id"));
        productDf = productDf
                .join(productTagDf, productTagDf.col("product_id").equalTo(productDf.col("product_id")), "left")
                .join(productCategoryDf, productCategoryDf.col("product_id").equalTo(productDf.col("product_id")), "left")
                .join(ratingAggDf, ratingAggDf.col("product_id").equalTo(productDf.col("product_id")), "left")
                .drop(productTagDf.col("product_id"))
                .drop(productCategoryDf.col("product_id"))
                .drop(ratingAggDf.col("product_id"));


        productDf.printSchema();
        productDf.show(false);
        System.out.println("productDf has " + productDf.count() + " records.");
        loadOperation.save(ProductTable.class, productDf);
    }
}