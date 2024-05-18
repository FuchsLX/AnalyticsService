package org.ecommerce.analyticsservice.dtos;

import lombok.*;

import java.util.List;

@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public final class ProductWithCategoryTagDto {
    private String productId;
    private String productName;
    private List<String> categoryIds;
    private List<String> categoryNames;
    private List<String> tagIds;
    private List<String> tagNames;
}
