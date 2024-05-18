package org.ecommerce.analyticsservice.services.impl;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.ecommerce.analyticsservice.constants.table.CategoryTable;
import org.ecommerce.analyticsservice.dtos.*;
import org.ecommerce.analyticsservice.exceptions.NotFoundException;
import org.ecommerce.analyticsservice.mapper.*;
import org.ecommerce.analyticsservice.services.CategoryAnalyticsService;
import org.ecommerce.analyticsservice.utils.ExtractOperationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;

import static org.apache.spark.sql.functions.col;

@Service
@RequiredArgsConstructor
public class CategoryAnalyticsServiceImpl implements CategoryAnalyticsService {
    private final ExtractOperationUtil extractOperationUtil;
    private static final Logger logger = LoggerFactory.getLogger(CategoryAnalyticsServiceImpl.class);

    @Override
    public CategoryRatingDto getCategoryRatingByCategoryId(String categoryId) throws NotFoundException {
        Dataset<Row> df = this.extractCategoryRatingDf()
                .filter(col("category_id").equalTo(categoryId));
        if (df.isEmpty()) throw new NotFoundException(String.format("No such category with id: %s", categoryId));
        Row row = df.first();
        if (row.getAs("rating_count") == null) {
            return CategoryRatingDto.builder()
                    .categoryId(categoryId)
                    .categoryName(row.getAs("category_name"))
                    .oneStarRatingCount(0L)
                    .twoStarRatingCount(0L)
                    .threeStarRatingCount(0L)
                    .fourStarRatingCount(0L)
                    .fiveStarRatingCount(0L)
                    .ratingCount(0L)
                    .ratingAvg((double) 0)
                    .build();
        }
        return CategoryRatingDto.builder()
                .categoryId(categoryId)
                .categoryName(row.getAs("category_name"))
                .oneStarRatingCount(row.getAs("one_star_rating_count"))
                .twoStarRatingCount(row.getAs("two_star_rating_count"))
                .threeStarRatingCount(row.getAs("three_star_rating_count"))
                .fourStarRatingCount(row.getAs("four_star_rating_count"))
                .fiveStarRatingCount(row.getAs("five_star_rating_count"))
                .ratingCount(row.getAs("rating_count"))
                .ratingAvg(row.getAs("rating_avg"))
                .build();
    }

    @Override
    public List<CategoryRatingDto> getTopHighestRatedCategory(int numRecords) {
        return this.extractCategoryRatingDf()
                .orderBy(col("rating_avg").desc())
                .limit(numRecords)
                .map(new CategoryRatingDtoMapper(), Encoders.bean(CategoryRatingDto.class))
                .collectAsList();
    }

    @Override
    public List<CategoryRatingDto> getTopLowestRatedCategory(int numRecords) {
        return this.extractCategoryRatingDf()
                .filter(col("rating_count").gt(0))
                .orderBy(col("rating_avg").asc())
                .limit(numRecords)
                .map(new CategoryRatingDtoMapper(), Encoders.bean(CategoryRatingDto.class))
                .collectAsList();
    }

    @Override
    public CategoryProductCountDto getCategoryProductCountByCategoryId(String categoryId) throws NotFoundException {
        Dataset<Row> df = this.extractCategoryProductCntDf()
                .filter(col("category_id").equalTo(categoryId));
        if (df.isEmpty()) throw new NotFoundException(String.format("No such category with id: %s", categoryId));
        return CategoryProductCountDto.builder()
                .categoryId(categoryId)
                .categoryName(df.first().getAs("category_name"))
                .productCount(df.first().getAs("product_count"))
                .build();
    }

    @Override
    public List<CategoryProductCountDto> getTopHighestCategoryProductCount(int numRecords) {
        return this.extractCategoryProductCntDf()
                .orderBy(col("product_count").desc())
                .limit(numRecords)
                .map(new CategoryProductCountDtoMapper(), Encoders.bean(CategoryProductCountDto.class))
                .collectAsList();
    }

    @Override
    public List<CategoryProductCountDto> getAllCategoryProductCount() {
        return this.extractCategoryProductCntDf()
                .map(new CategoryProductCountDtoMapper(), Encoders.bean(CategoryProductCountDto.class))
                .collectAsList();
    }

    @Override
    public CategoryWithOrderStatusCntDto getCategoryWithOSCntByCategoryId(String categoryId) throws NotFoundException {
        var df = this.extractCategoryWithOSCntDf()
                .filter(col("category_id").equalTo(categoryId));
        if (df.isEmpty()) throw new NotFoundException(String.format("No such category with id: %s", categoryId));
        return CategoryWithOrderStatusCntDto.builder()
                .categoryId(categoryId)
                .categoryName(df.first().getAs("category_name"))
                .processingCount(df.first().getAs("processing_quantity"))
                .deliveredCount(df.first().getAs("delivered_quantity"))
                .completedCount(df.first().getAs("completed_quantity"))
                .cancelledCount(df.first().getAs("cancelled_quantity"))
                .build();
    }

    @Override
    public List<CategoryWithOrderStatusCntDto> getTopHighestCompletedCategoryOSCnt(int numRecords) {
        return this.extractCategoryWithOSCntDf()
                .orderBy(col("completed_quantity").desc())
                .limit(numRecords)
                .map(new CategoryWithOrderStatusCntDtoMapper(), Encoders.bean(CategoryWithOrderStatusCntDto.class))
                .collectAsList();
    }

    @Override
    public CategoryWithOrderStatusRevDto getCategoryWithOSErvByCategoryId(String categoryId) throws NotFoundException {
        var df = this.extractCategoryWithOSRevDf()
                .filter(col("category_id").equalTo(categoryId));
        if (df.isEmpty()) throw new NotFoundException(String.format("No such category with id: %s", categoryId));
        return CategoryWithOrderStatusRevDto.builder()
                .categoryId(categoryId)
                .categoryName(df.first().getAs("category_name"))
                .completedTotalPrice(df.first().getAs("completed_total_price"))
                .processingTotalPrice(df.first().getAs("processing_total_price"))
                .deliveredTotalPrice(df.first().getAs("delivered_total_price"))
                .cancelledTotalPrice(df.first().getAs("cancelled_total_price"))
                .build();
    }

    @Override
    public List<CategoryWithOrderStatusRevDto> getTopHighestCompletedCategoryOSRev(int numRecords) {
        return this.extractCategoryWithOSRevDf()
                .orderBy(col("completed_total_price").desc())
                .limit(numRecords)
                .map(new CategoryWithOrderStatusRevDtoMapper(), Encoders.bean(CategoryWithOrderStatusRevDto.class))
                .collectAsList();
    }

    @Override
    public CategoryWithBuyerCntDto getCategoryWithBuyerCntByCategoryId(String categoryId) throws NotFoundException {
        Dataset<Row> df = this.extractCategoryWithBuyerCntDf()
                .filter(col("category_id").equalTo(categoryId));
        if (df.isEmpty()) throw new NotFoundException(String.format("No such category with id: %s", categoryId));
        return CategoryWithBuyerCntDto.builder()
                .categoryId(categoryId)
                .categoryName(df.first().getAs("category_name"))
                .uniqueBuyersCount(df.first().getAs("unique_buyers_count"))
                .repeatBuyersCount(df.first().getAs("repeat_buyers_count"))
                .reorderRate(df.first().getAs("reorder_rate"))
                .build();
    }

    @Override
    public List<CategoryWithBuyerCntDto> getTopHighestReorderRateCategory(int numRecords) {
        return this.extractCategoryWithBuyerCntDf()
                .orderBy(col("reorder_rate").desc())
                .limit(numRecords)
                .map(new CategoryWithBuyerCntDtoMapper(), Encoders.bean(CategoryWithBuyerCntDto.class))
                .collectAsList();
    }

    private Dataset<Row> extractCategoryWithOSCntDf() {
        return this.extractCategoryOrderDf()
                .select(
                        col("category_id"), col("category_name"),
                        col("completed_quantity"), col("processing_quantity"),
                        col("cancelled_quantity"), col("delivered_quantity")
                );
    }

    private Dataset<Row> extractCategoryWithOSRevDf() {
        return this.extractCategoryOrderDf()
                .select(
                        col("category_id"), col("category_name"),
                        col("completed_total_price"), col("processing_total_price"),
                        col("cancelled_total_price"), col("delivered_total_price")
                );
    }

    private Dataset<Row> extractCategoryWithBuyerCntDf() {
        return this.extractCategoryOrderDf()
                .select(
                        col("category_id"), col("category_name"),
                        col("unique_buyers_count"), col("repeat_buyers_count"),
                        col("reorder_rate")
                );
    }

    private Dataset<Row> extractCategoryRatingDf() {
        return extractOperationUtil.ingest(CategoryTable.CATEGORY_RATING_WH, true);
    }

    private Dataset<Row> extractCategoryProductCntDf() {
        return extractOperationUtil.ingest(CategoryTable.CATEGORY_PRODUCT_COUNTER_WH, true);
    }

    private Dataset<Row> extractCategoryOrderDf() {
        return extractOperationUtil.ingest(CategoryTable.CATEGORY_ORDER_WH, true);
    }
}
