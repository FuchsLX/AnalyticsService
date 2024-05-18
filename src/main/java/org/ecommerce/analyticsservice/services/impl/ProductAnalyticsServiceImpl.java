package org.ecommerce.analyticsservice.services.impl;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.ecommerce.analyticsservice.constants.table.ProductTable;
import org.ecommerce.analyticsservice.dtos.*;
import org.ecommerce.analyticsservice.exceptions.NotFoundException;
import org.ecommerce.analyticsservice.mapper.*;
import org.ecommerce.analyticsservice.services.ProductAnalyticsService;
import org.ecommerce.analyticsservice.utils.ExtractOperationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.List;

import static org.apache.spark.sql.functions.*;

@Service
@RequiredArgsConstructor
public class ProductAnalyticsServiceImpl implements ProductAnalyticsService {

    private final ExtractOperationUtil extractOperationUtil;
    private static final Logger logger = LoggerFactory.getLogger(ProductAnalyticsServiceImpl.class);

    @Override
    public ProductRatingDto getRatingByProductId(String productId) throws NotFoundException {
        Dataset<Row> ratingDf =  this.extractProductRatingDf()
                .filter(col("product_id").equalTo(productId));
        if (ratingDf.isEmpty()) {
            throw new NotFoundException("No such product");
        } else {
            Row ratingRow = ratingDf.first();
            if (ratingRow.getAs("rating_count") == null) {
                return ProductRatingDto.builder()
                        .productId(productId)
                        .productName(ratingRow.getAs("product_name"))
                        .oneStarRatingCount(0L)
                        .twoStarRatingCount(0L)
                        .threeStarRatingCount(0L)
                        .fourStarRatingCount(0L)
                        .fiveStarRatingCount(0L)
                        .ratingCount(0L)
                        .ratingAvg((double) 0)
                        .build();
            } else {
                return ProductRatingDto.builder()
                        .productId(productId)
                        .productName(ratingRow.getAs("product_name"))
                        .oneStarRatingCount(ratingRow.getAs("one_star_rating_count"))
                        .twoStarRatingCount(ratingRow.getAs("two_star_rating_count"))
                        .threeStarRatingCount(ratingRow.getAs("three_star_rating_count"))
                        .fourStarRatingCount(ratingRow.getAs("four_star_rating_count"))
                        .fiveStarRatingCount(ratingRow.getAs("five_star_rating_count"))
                        .ratingCount(ratingRow.getAs("rating_count"))
                        .ratingAvg(ratingRow.getAs("rating_avg"))
                        .build();
            }
        }
    }

    @Override
    public List<ProductRatingDto> getTopHighestRatedProducts(int numRecords) {
        return this.extractProductRatingDf()
                .orderBy(col("rating_avg").desc())
                .limit(numRecords)
                .map(new ProductRatingDtoMapper(), Encoders.bean(ProductRatingDto.class))
                .collectAsList();
    }

    @Override
    public List<ProductRatingDto> getTopLowestRatedProducts(int numRecords) {
        return this.extractProductRatingDf()
                .filter(col("rating_count").gt(0))
                .orderBy(col("rating_avg").asc())
                .limit(numRecords)
                .map(new ProductRatingDtoMapper(), Encoders.bean(ProductRatingDto.class))
                .collectAsList();
    }

    @Override
    @SuppressWarnings("unchecked")
    public ProductWithCategoryTagDto getProductWithCategoryTagByProductId(String productId) throws NotFoundException{
        Dataset<Row> productCTDf = this.extractProductWithCategoryTagDf()
                .filter(col("product_id").equalTo(productId));
        if (productCTDf.isEmpty()) throw new NotFoundException(String.format("No such product with id: %s", productId));
        Row pctRow = productCTDf.first();
        return ProductWithCategoryTagDto.builder()
                .productId(productId)
                .productName(pctRow.getAs("product_name"))
                .categoryIds(JavaConverters.seqAsJavaListConverter((Seq<String>) pctRow.getAs("category_ids")).asJava())
                .categoryNames(JavaConverters.seqAsJavaListConverter((Seq<String>) pctRow.getAs("category_names")).asJava())
                .tagIds(JavaConverters.seqAsJavaListConverter((Seq<String>) pctRow.getAs("tag_ids")).asJava())
                .tagNames(JavaConverters.seqAsJavaListConverter((Seq<String>) pctRow.getAs("tag_names")).asJava())
                .build();
    }

    @Override
    public ProductStdDto getProductStdDtoByProductId(String productId) throws NotFoundException {
        Dataset<Row> df = this.extractProductStdDf()
                .filter(col("product_id").equalTo(productId));
        if (df.isEmpty()) throw new NotFoundException(String.format("No such product with id: %s", productId));
        return ProductStdDto.builder()
                .productId(productId)
                .productName(df.first().getAs("product_name"))
                .price(df.first().getAs("price"))
                .quantity(df.first().getAs("quantity"))
                .discount(df.first().getAs("discount"))
                .build();
    }

    @Override
    public List<ProductStdDto> getAllProductStd() {
        return this.extractProductStdDf()
                .map(new ProductStdDtoMapper(), Encoders.bean(ProductStdDto.class))
                .collectAsList();
    }

    @Override
    public ProductWithOrderStatusCountDto getProductWithOrderStatusCountByProductId(String productId) throws NotFoundException {
        Dataset<Row> df = this.extractProductWithOrderStatusCount()
                .filter(col("product_id").equalTo(productId));
        if (df.isEmpty()) throw new NotFoundException(String.format("No such product with id: %s", productId));
        return ProductWithOrderStatusCountDto.builder()
                .productId(productId)
                .productName(df.first().getAs("product_name"))
                .processingCount(df.first().getAs("processing_quantity"))
                .deliveredCount(df.first().getAs("delivered_quantity"))
                .completedCount(df.first().getAs("completed_quantity"))
                .cancelledCount(df.first().getAs("cancelled_quantity"))
                .build();
    }

    @Override
    public List<ProductWithOrderStatusCountDto> getTopHighestCompletedCountProduct(int numRecords) {
        return this.extractProductWithOrderStatusCount()
                .orderBy(col("completed_quantity").desc())
                .limit(numRecords)
                .map(new ProductWithOrderStatusCountDtoMapper(), Encoders.bean(ProductWithOrderStatusCountDto.class))
                .collectAsList();
    }

    @Override
    public ProductWithOrderStatusRevDto getProductWithOrderStatusRevByProductId(String productId) throws NotFoundException {
        Dataset<Row> df = this.extractProductWithOrderStatusRevDf()
                .filter(col("product_id").equalTo(productId));
        if (df.isEmpty()) throw new NotFoundException(String.format("No such product with id: %s", productId));
        return ProductWithOrderStatusRevDto.builder()
                .productId(productId)
                .productName(df.first().getAs("product_name"))
                .completedTotalPrice(df.first().getAs("completed_total_price"))
                .processingTotalPrice(df.first().getAs("processing_total_price"))
                .deliveredTotalPrice(df.first().getAs("delivered_total_price"))
                .cancelledTotalPrice(df.first().getAs("cancelled_total_price"))
                .build();
    }

    @Override
    public List<ProductWithOrderStatusRevDto> getTopHighestRevenueProduct(int numRecords) {
        return this.extractProductWithOrderStatusRevDf()
                .orderBy(col("completed_total_price").desc())
                .limit(numRecords)
                .map(new ProductWithOrderStatusTotalPriceDtoMapper(), Encoders.bean(ProductWithOrderStatusRevDto.class))
                .collectAsList();
    }

    @Override
    public ProductWithBuyerDto getProductWithBuyerByProductId(String productId) throws NotFoundException {
        Dataset<Row> df = this.extractProductWithBuyerCount()
                .filter(col("product_id").equalTo(productId));
        if (df.isEmpty()) throw new NotFoundException(String.format("No such product with id: %s", productId));
        return ProductWithBuyerDto.builder()
                .productId(productId)
                .productName(df.first().getAs("product_name"))
                .uniqueBuyersCount(df.first().getAs("unique_buyers_count"))
                .repeatBuyersCount(df.first().getAs("repeat_buyers_count"))
                .reorderRate(df.first().getAs("reorder_rate"))
                .build();
    }

    @Override
    public List<ProductWithBuyerDto> getTopHighestReorderRateProduct(int numRecords) {
        return this.extractProductWithBuyerCount()
                .orderBy(col("reorder_rate").desc())
                .limit(numRecords)
                .map(new ProductWithBuyerDtoMapper(), Encoders.bean(ProductWithBuyerDto.class))
                .collectAsList();
    }

    @Override
    public List<ProductWithCategoryTagDto> getAllProductWithCategoryTag() {
        return this.extractProductWithCategoryTagDf()
                .map(new ProductWithCategoryTagDtoMapper(), Encoders.bean(ProductWithCategoryTagDto.class))
                .collectAsList();
    }

    private Dataset<Row> extractProductStdDf() {
        return this.extractProductWithOrderDf()
                .select(col("product_id"), col("product_name"), col("price"), col("quantity"), col("discount"));
    }

    private Dataset<Row> extractProductWithBuyerCount() {
        return this.extractProductWithOrderDf()
                .select(
                        col("product_id"), col("product_name"),
                        col("unique_buyers_count"), col("repeat_buyers_count"),
                        col("reorder_rate")
                );
    }

    private Dataset<Row> extractProductWithOrderStatusRevDf() {
        return this.extractProductWithOrderDf()
                .select(
                        col("product_id"), col("product_name"),
                        col("completed_total_price"), col("processing_total_price"),
                        col("cancelled_total_price"), col("delivered_total_price")
                );
    }

    private Dataset<Row> extractProductWithOrderStatusCount() {
        return this.extractProductWithOrderDf()
                .select(
                        col("product_id"), col("product_name"),
                        col("completed_quantity"), col("processing_quantity"),
                        col("cancelled_quantity"), col("delivered_quantity")
                );
    }

    private Dataset<Row> extractProductWithOrderDf() {
        return extractOperationUtil.ingest(ProductTable.PRODUCT_ORDER_WH, true);
    }

    private Dataset<Row> extractProductRatingDf() {
        return extractOperationUtil.ingest(ProductTable.PRODUCT_RATING_WH, true);
    }

    private Dataset<Row> extractProductWithCategoryTagDf() {
        return extractOperationUtil.ingest(ProductTable.PRODUCT_CATEGORY_TAG_WH, true);
    }
}
