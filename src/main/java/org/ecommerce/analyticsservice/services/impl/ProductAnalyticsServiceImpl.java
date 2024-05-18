package org.ecommerce.analyticsservice.services.impl;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.ecommerce.analyticsservice.constants.ProductTable;
import org.ecommerce.analyticsservice.dtos.RatingDto;
import org.ecommerce.analyticsservice.exceptions.NotFoundException;
import org.ecommerce.analyticsservice.services.ProductAnalyticsService;
import org.ecommerce.analyticsservice.utils.ExtractOperationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import static org.apache.spark.sql.functions.*;

@Service
@RequiredArgsConstructor
public class ProductAnalyticsServiceImpl implements ProductAnalyticsService {

    private final ExtractOperationUtil extractOperationUtil;
    private static final Logger logger = LoggerFactory.getLogger(ProductAnalyticsServiceImpl.class);

    @Override
    public RatingDto getRatingByProductId(String productId) throws NotFoundException {
        Dataset<Row> ratingDf =  extractOperationUtil.ingest(ProductTable.PRODUCT_RATING_WH, true)
                .filter(col("product_id").equalTo(productId));
        if (ratingDf.isEmpty()) {
            throw new NotFoundException("No such product");
        } else {
            Row ratingRow = ratingDf.first();
            if (ratingRow.getAs("rating_count") == null) {
                return RatingDto.builder()
                        .productId(productId)
                        .productName(ratingRow.getAs("product_name"))
                        .oneStarRatingCount(0)
                        .twoStarRatingCount(0)
                        .threeStarRatingCount(0)
                        .fourStarRatingCount(0)
                        .fiveStarRatingCount(0)
                        .ratingCount(0)
                        .ratingAvg(0)
                        .build();
            } else {
                return RatingDto.builder()
                        .productName(productId)
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
}
