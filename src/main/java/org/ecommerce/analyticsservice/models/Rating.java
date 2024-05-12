package org.ecommerce.analyticsservice.models;

import lombok.*;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Rating {
    private String productId;
    private long oneStarCount;
    private long twoStarCount;
    private long threeStarCount;
    private long fourStarCount;
    private long fiveStarCount;
    private long recordCount;
    private double average;
}
