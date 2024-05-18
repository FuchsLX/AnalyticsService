package org.ecommerce.analyticsservice.mapper;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.ecommerce.analyticsservice.dtos.ProductWithCategoryTagDto;
import scala.collection.JavaConverters;
import scala.collection.Seq;

public final class ProductWithCategoryTagDtoMapper implements MapFunction<Row, ProductWithCategoryTagDto> {

    @Override
    @SuppressWarnings("unchecked")
    public ProductWithCategoryTagDto call(Row row) throws Exception {
        return ProductWithCategoryTagDto.builder()
                .productId(row.getAs("product_id"))
                .productName(row.getAs("product_name"))
                .categoryIds(JavaConverters.seqAsJavaListConverter((Seq<String>) row.getAs("category_ids")).asJava())
                .categoryNames(JavaConverters.seqAsJavaListConverter((Seq<String>) row.getAs("category_names")).asJava())
                .tagIds(JavaConverters.seqAsJavaListConverter((Seq<String>) row.getAs("tag_ids")).asJava())
                .tagNames(JavaConverters.seqAsJavaListConverter((Seq<String>) row.getAs("tag_names")).asJava())
                .build();
    }
}
