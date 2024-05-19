package org.ecommerce.analyticsservice.dtos;

import lombok.*;

@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public final class ProductWithBuyerDto {
    private String productId;
    private String productName;
    private Long uniqueBuyersCount;
    private Long repeatBuyersCount;
    private Double reorderRate;
}
