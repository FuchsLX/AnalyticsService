package org.ecommerce.analyticsservice.dtos;

import lombok.*;

@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public final class CategoryProductCountDto {
    private String categoryId;
    private String categoryName;
    private long productCount;
}
