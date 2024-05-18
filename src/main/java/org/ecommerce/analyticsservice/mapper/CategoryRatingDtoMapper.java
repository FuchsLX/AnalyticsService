package org.ecommerce.analyticsservice.mapper;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.ecommerce.analyticsservice.dtos.CategoryRatingDto;

public final class CategoryRatingDtoMapper implements MapFunction<Row, CategoryRatingDto> {

    @Override
    public CategoryRatingDto call(Row row) throws Exception {
        return CategoryRatingDto.builder()
                .categoryId(row.getAs("category_id"))
                .categoryName(row.getAs("category_name"))
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
