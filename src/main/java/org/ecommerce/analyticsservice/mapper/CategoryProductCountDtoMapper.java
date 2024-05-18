package org.ecommerce.analyticsservice.mapper;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.ecommerce.analyticsservice.dtos.CategoryProductCountDto;

public final class CategoryProductCountDtoMapper implements MapFunction<Row, CategoryProductCountDto> {
    @Override
    public CategoryProductCountDto call(Row row) throws Exception {
        return CategoryProductCountDto.builder()
                .categoryId(row.getAs("category_id"))
                .categoryName(row.getAs("category_name"))
                .productCount(row.getAs("product_count"))
                .build();
    }
}
