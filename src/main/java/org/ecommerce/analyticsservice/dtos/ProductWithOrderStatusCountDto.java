package org.ecommerce.analyticsservice.dtos;

import lombok.*;

@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public final class ProductWithOrderStatusCountDto {
    private String productId;
    private String productName;
    private long completedCount;
    private long cancelledCount;
    private long processingCount;
    private long deliveredCount;
}
