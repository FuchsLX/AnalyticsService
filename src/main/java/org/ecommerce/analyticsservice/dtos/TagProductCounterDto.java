package org.ecommerce.analyticsservice.dtos;

import lombok.*;

@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public final class TagProductCounterDto {
    private String tagId;
    private String tagName;
    private long productCount;
}
