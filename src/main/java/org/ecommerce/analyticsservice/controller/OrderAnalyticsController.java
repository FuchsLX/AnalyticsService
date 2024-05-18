package org.ecommerce.analyticsservice.controller;

import lombok.RequiredArgsConstructor;
import org.ecommerce.analyticsservice.dtos.OrderStatusCounterDto;
import org.ecommerce.analyticsservice.dtos.OrderStatusRevenueDto;
import org.ecommerce.analyticsservice.services.OrderAnalyticsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

import static org.ecommerce.analyticsservice.constants.router.OrderAnalyticsRouter.*;

@RestController
@RequiredArgsConstructor
public class OrderAnalyticsController {

    private final OrderAnalyticsService orderAnalyticsService;
    private static final Logger logger = LoggerFactory.getLogger(OrderAnalyticsController.class);

    @GetMapping(ORDER_STATUS_COUNT_ROUTER)
    public ResponseEntity<List<OrderStatusCounterDto>> getOrderStatusCounters() {
        logger.debug("REQUEST: `getOrderStatusCounters()`...");
        return ResponseEntity.ok(orderAnalyticsService.getOrderStatusCounters());
    }

    @GetMapping(ORDER_STATUS_REVENUE_ROUTER)
    public ResponseEntity<List<OrderStatusRevenueDto>> getOrderStatusRevenue() {
        logger.debug("REQUEST: `getOrderStatusRevenue()`...");
        return ResponseEntity.ok(orderAnalyticsService.getOrderStatusRevenue());
    }
}
