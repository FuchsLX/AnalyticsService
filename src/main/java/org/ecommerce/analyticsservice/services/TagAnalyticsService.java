package org.ecommerce.analyticsservice.services;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.ecommerce.analyticsservice.constants.ProductTagTable;
import org.ecommerce.analyticsservice.constants.TagTable;
import org.ecommerce.analyticsservice.utils.ExtractOperationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;

@Service
@RequiredArgsConstructor
public class TagAnalyticsService {
    private final ExtractOperationUtil extractOperation;
    private final SparkSession spark;

    private static final Logger logger = LoggerFactory.getLogger(TagAnalyticsService.class);

    public void tagTest() {
        Dataset<Row> tagDf = extractOperation.ingest(TagTable.class);
        Dataset<Row> productTagDf = extractOperation.ingest(ProductTagTable.class);

        tagDf = tagDf.repartition(10);
        productTagDf = productTagDf.repartition(10);

        tagDf = tagDf.drop("created_at")
                .drop("updated_at")
                .drop("content")
                .drop("meta_title")
                .drop("slug")
                .withColumnRenamed("title", "tag_name");

        tagDf = tagDf.join(productTagDf, productTagDf.col("tag_id").equalTo(tagDf.col("id")), "left_outer")
                .drop(productTagDf.col("tag_id"));
        Dataset<Row> tagAggDf = tagDf
                .groupBy(col("id"), col("tag_name"))
                .agg(count(col("product_id")).as("products_count"));
        tagAggDf.show(false);
    }


}
