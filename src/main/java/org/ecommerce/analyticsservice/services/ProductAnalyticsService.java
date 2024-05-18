package org.ecommerce.analyticsservice.services;

import org.ecommerce.analyticsservice.dtos.RatingDto;
import org.ecommerce.analyticsservice.exceptions.NotFoundException;

public interface ProductAnalyticsService {

    RatingDto getRatingByProductId(String productId) throws NotFoundException;
}
