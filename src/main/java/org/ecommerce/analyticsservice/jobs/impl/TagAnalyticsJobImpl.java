package org.ecommerce.analyticsservice.jobs.impl;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.ecommerce.analyticsservice.constants.table.ProductTagTable;
import org.ecommerce.analyticsservice.constants.table.TagTable;
import org.ecommerce.analyticsservice.jobs.TagAnalyticsJob;
import org.ecommerce.analyticsservice.utils.ExtractOperationUtil;
import org.ecommerce.analyticsservice.utils.LoadOperationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import static org.apache.spark.sql.functions.*;

@Component
@RequiredArgsConstructor
public class TagAnalyticsJobImpl implements TagAnalyticsJob {

    private final ExtractOperationUtil extractOperationUtil;
    private final LoadOperationUtil loadOperationUtil;

    private static final Logger logger = LoggerFactory.getLogger(TagAnalyticsJobImpl.class);

    @Override
    public void start() {
        Dataset<Row> tagDf = extractOperationUtil.ingest(TagTable.class);
        Dataset<Row> productTagDf = extractOperationUtil.ingest(ProductTagTable.class);
        logger.debug("EXTRACTED dataframes in {}", TagAnalyticsJob.class.getName());

        tagDf = tagDf.repartition(10);
        productTagDf = productTagDf.repartition(10);

        tagDf = tagDf.drop("created_at")
                .drop("updated_at")
                .drop("content")
                .drop("meta_title")
                .drop("slug")
                .withColumnRenamed("id", "tag_id")
                .withColumnRenamed("title", "tag_name");

        tagDf = tagDf
                .join(productTagDf, tagDf.col("tag_id").equalTo(productTagDf.col("tag_id")), "left_outer")
                .drop(productTagDf.col("tag_id"));

        this.tagProductCounterTask(tagDf);
    }

    private void tagProductCounterTask(Dataset<Row> tagDf) {
        tagDf = tagDf
                .groupBy(col("tag_id"), col("tag_name"))
                .agg(count(col("product_id")).as("product_count"));
        logger.debug("TRANSFORMED `{}` table !!!", TagTable.TAG_PRODUCT_COUNTER_WH);
        tagDf.printSchema();
        tagDf.show(5, false);
        loadOperationUtil.save(TagTable.TAG_PRODUCT_COUNTER_WH, tagDf);
        logger.debug("SAVED `{}` table !!!", TagTable.TAG_PRODUCT_COUNTER_WH);
    }
}
