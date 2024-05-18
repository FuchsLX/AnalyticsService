package org.ecommerce.analyticsservice.mapper;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.ecommerce.analyticsservice.dtos.CategoryWithBuyerCntDto;

public final class CategoryWithBuyerCntDtoMapper implements MapFunction<Row, CategoryWithBuyerCntDto> {

    @Override
    public CategoryWithBuyerCntDto call(Row row) throws Exception {
        return CategoryWithBuyerCntDto.builder()
                .categoryId(row.getAs("category_id"))
                .categoryName(row.getAs("category_name"))
                .uniqueBuyersCount(row.getAs("unique_buyers_count"))
                .repeatBuyersCount(row.getAs("repeat_buyers_count"))
                .reorderRate(row.getAs("reorder_rate"))
                .build();
    }
}
