package org.ecommerce.analyticsservice.controller;

import lombok.RequiredArgsConstructor;
import org.ecommerce.analyticsservice.dtos.TagProductCounterDto;
import org.ecommerce.analyticsservice.services.TagAnalyticsService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

import static org.ecommerce.analyticsservice.constants.router.TagAnalyticsRouter.*;

@RestController
@RequiredArgsConstructor
public class TagAnalyticsController {
    private final TagAnalyticsService tagAnalyticsService;

    @GetMapping(TAG_PRODUCT_COUNTER_ROUTER)
    public ResponseEntity<List<TagProductCounterDto>> getTagProductCount() {
        return ResponseEntity.ok(tagAnalyticsService.getTagProductCount());
    }
}
