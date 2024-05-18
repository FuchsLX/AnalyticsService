package org.ecommerce.analyticsservice.mapper;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.ecommerce.analyticsservice.dtos.TagProductCounterDto;

public final class TagProductCounterDtoMapper implements MapFunction<Row, TagProductCounterDto> {

    @Override
    public TagProductCounterDto call(Row row) throws Exception {
        return TagProductCounterDto.builder()
                .tagId(row.getAs("tag_id"))
                .tagName(row.getAs("tag_name"))
                .productCount(row.getAs("product_count"))
                .build();
    }
}
