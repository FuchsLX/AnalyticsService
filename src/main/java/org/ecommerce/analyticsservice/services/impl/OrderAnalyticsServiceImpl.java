package org.ecommerce.analyticsservice.services.impl;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.ecommerce.analyticsservice.constants.table.OrderTable;
import org.ecommerce.analyticsservice.dtos.OrderStatusCounterDto;
import org.ecommerce.analyticsservice.dtos.OrderStatusRevenueDto;
import org.ecommerce.analyticsservice.mapper.OrderStatusCounterDtoMapper;
import org.ecommerce.analyticsservice.mapper.OrderStatusRevenueDtoMapper;
import org.ecommerce.analyticsservice.services.OrderAnalyticsService;
import org.ecommerce.analyticsservice.utils.ExtractOperationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;

import static org.apache.spark.sql.functions.col;

@Service
@RequiredArgsConstructor
public class OrderAnalyticsServiceImpl implements OrderAnalyticsService {
    private final ExtractOperationUtil extractOperationUtil;

    private static final Logger logger = LoggerFactory.getLogger(OrderAnalyticsServiceImpl.class);

    @Override
    public List<OrderStatusCounterDto> getOrderStatusCounters() {
        return extractOperationUtil.ingest(OrderTable.ORDER_WITH_STATUS_WH, true)
                .select(col("order_status"), col("quantity"))
                .map(new OrderStatusCounterDtoMapper(), Encoders.bean(OrderStatusCounterDto.class)).collectAsList();
    }

    @Override
    public List<OrderStatusRevenueDto> getOrderStatusRevenue() {
        return extractOperationUtil.ingest(OrderTable.ORDER_WITH_STATUS_WH, true)
                .select(col("order_status"), col("total_price_after_discount"), col("total_price_before_discount"))
                .map(new OrderStatusRevenueDtoMapper(), Encoders.bean(OrderStatusRevenueDto.class)).collectAsList();
    }
}
