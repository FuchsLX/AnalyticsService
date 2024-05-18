package org.ecommerce.analyticsservice.dtos;

import lombok.*;

@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public final class CategoryWithOrderStatusCntDto {
    private String categoryId;
    private String categoryName;
    private long completedCount;
    private long cancelledCount;
    private long processingCount;
    private long deliveredCount;
}
