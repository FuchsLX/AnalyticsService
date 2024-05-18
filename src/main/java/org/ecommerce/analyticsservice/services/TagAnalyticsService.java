package org.ecommerce.analyticsservice.services;

import org.ecommerce.analyticsservice.dtos.TagProductCounterDto;

import java.util.List;

public interface TagAnalyticsService {
    List<TagProductCounterDto> getTagProductCount();
}
