package org.ecommerce.analyticsservice.services;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.ecommerce.analyticsservice.constants.CategoryTable;
import org.ecommerce.analyticsservice.constants.ProductCategoryTable;
import org.ecommerce.analyticsservice.models.Category;
import org.ecommerce.analyticsservice.services.mapper.CategoryMapper;
import org.ecommerce.analyticsservice.utils.ExtractOperationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import scala.collection.JavaConverters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.*;

@Service
@RequiredArgsConstructor
public class CategoryAnalyticsService {
    private final ExtractOperationUtil extractOperation;
    private final SparkSession spark;

    private static final Logger logger = LoggerFactory.getLogger(CategoryAnalyticsService.class);

    public void categoryTest() {
        Dataset<Row> categoryDf = extractOperation.ingest(CategoryTable.class);
        Dataset<Row> productCategoryDf = extractOperation.ingest(ProductCategoryTable.class);

        categoryDf = categoryDf.repartition(10);
        productCategoryDf = productCategoryDf.repartition(10);

        categoryDf = categoryDf.drop("created_at")
                .drop("updated_at")
                .drop("meta_title")
                .drop("slug")
                .drop("content")
                .withColumnRenamed("title", "category_name");

        categoryDf = this.joinSpec(categoryDf, productCategoryDf);
        Dataset<Row> categoryAggDf = categoryDf
                .groupBy(col("id"), col("category_name"))
                .agg(count(col("product_id")).as("product_count"));
        Dataset<Row> parentCategoryDf = categoryDf
                .drop("category_name")
                .drop("product_id")
                .distinct();
        categoryAggDf = categoryAggDf
                .join(parentCategoryDf, parentCategoryDf.col("id").equalTo(categoryAggDf.col("id")), "inner")
                .drop(parentCategoryDf.col("id"));
        categoryDf.printSchema();
        categoryAggDf.show(false);
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
        productCategoryDf = spark.createDataFrame(productCategoryRows, productCategorySchema);
        productCategoryDf.show(false);
        return categoryDf
                .join(productCategoryDf, productCategoryDf.col("category_id").equalTo(categoryDf.col("id")), "left_outer")
                .drop(productCategoryDf.col("category_id"));
    }
}
