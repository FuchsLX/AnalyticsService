package org.ecommerce.analyticsservice.controller;

import lombok.RequiredArgsConstructor;
import org.ecommerce.analyticsservice.dtos.CustomerCountDto;
import org.ecommerce.analyticsservice.services.CustomerAnalyticsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import static org.ecommerce.analyticsservice.constants.router.CustomerAnalyticsRouter.*;

@RestController
@RequiredArgsConstructor
public class CustomerAnalyticsController {
    private final CustomerAnalyticsService customerAnalyticsService;
    private static final Logger logger = LoggerFactory.getLogger(CustomerAnalyticsController.class);

    @GetMapping(GET_CUSTOMER_COUNT)
    public ResponseEntity<CustomerCountDto> getCustomerCount() {
        return ResponseEntity.ok(customerAnalyticsService.getCustomerCountAnalytics());
    }
}
