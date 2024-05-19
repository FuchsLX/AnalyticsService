package org.ecommerce.analyticsservice.dtos;

import lombok.*;

@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public final class CategoryWithBuyerCntDto {
    private String categoryId;
    private String categoryName;
    private Long uniqueBuyersCount;
    private Long repeatBuyersCount;
    private Double reorderRate;
}
