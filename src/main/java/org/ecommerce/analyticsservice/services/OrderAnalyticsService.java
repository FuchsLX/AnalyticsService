package org.ecommerce.analyticsservice.services;

import org.ecommerce.analyticsservice.dtos.OrderStatusCounterDto;
import org.ecommerce.analyticsservice.dtos.OrderStatusRevenueDto;

import java.util.List;

public interface OrderAnalyticsService {
    List<OrderStatusCounterDto> getOrderStatusCounters();

    List<OrderStatusRevenueDto> getOrderStatusRevenue();
}
