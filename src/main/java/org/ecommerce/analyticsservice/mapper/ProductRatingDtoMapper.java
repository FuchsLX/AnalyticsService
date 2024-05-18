package org.ecommerce.analyticsservice.mapper;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.ecommerce.analyticsservice.dtos.ProductRatingDto;

public final class ProductRatingDtoMapper implements MapFunction<Row, ProductRatingDto> {

    @Override
    public ProductRatingDto call(Row value) throws Exception {
        return ProductRatingDto.builder()
                .productId(value.getAs("product_id"))
                .productName(value.getAs("product_name"))
                .ratingAvg( value.getAs("rating_avg"))
                .ratingCount(value.getAs("rating_count"))
                .oneStarRatingCount(value.getAs("one_star_rating_count"))
                .twoStarRatingCount(value.getAs("two_star_rating_count"))
                .threeStarRatingCount(value.getAs("three_star_rating_count"))
                .fourStarRatingCount(value.getAs("four_star_rating_count"))
                .fiveStarRatingCount(value.getAs("five_star_rating_count"))
                .build();
    }
}
