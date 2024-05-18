package org.ecommerce.analyticsservice.dtos;

import lombok.*;

@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class RatingDto {
    private String productId;
    private String productName;
    private double ratingAvg;
    private long ratingCount;
    private long oneStarRatingCount;
    private long twoStarRatingCount;
    private long threeStarRatingCount;
    private long fourStarRatingCount;
    private long fiveStarRatingCount;
}
