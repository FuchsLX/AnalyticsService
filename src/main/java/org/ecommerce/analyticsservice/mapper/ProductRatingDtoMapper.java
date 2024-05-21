package org.ecommerce.analyticsservice.mapper;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.ecommerce.analyticsservice.dtos.ProductRatingDto;

public final class ProductRatingDtoMapper implements MapFunction<Row, ProductRatingDto> {

    @Override
    public ProductRatingDto call(Row row) throws Exception {
        String productName = row.getAs("product_name");
        if (productName.length() > 20) productName = productName.substring(0, 20) + "...";
        return ProductRatingDto.builder()
                .productId(row.getAs("product_id"))
                .productName(productName)
                .ratingAvg( row.getAs("rating_avg"))
                .ratingCount(row.getAs("rating_count"))
                .oneStarRatingCount(row.getAs("one_star_rating_count"))
                .twoStarRatingCount(row.getAs("two_star_rating_count"))
                .threeStarRatingCount(row.getAs("three_star_rating_count"))
                .fourStarRatingCount(row.getAs("four_star_rating_count"))
                .fiveStarRatingCount(row.getAs("five_star_rating_count"))
                .build();
    }
}
