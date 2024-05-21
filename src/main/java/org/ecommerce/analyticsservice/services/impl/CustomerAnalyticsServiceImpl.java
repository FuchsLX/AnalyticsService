package org.ecommerce.analyticsservice.services.impl;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.ecommerce.analyticsservice.constants.table.CustomerTable;
import org.ecommerce.analyticsservice.dtos.CustomerCountDto;
import org.ecommerce.analyticsservice.services.CustomerAnalyticsService;
import org.ecommerce.analyticsservice.utils.ExtractOperationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import static org.apache.spark.sql.functions.*;

@Service
@RequiredArgsConstructor
public class CustomerAnalyticsServiceImpl implements CustomerAnalyticsService {
    private final ExtractOperationUtil extractOperationUtil;
    private static final Logger logger = LoggerFactory.getLogger(CustomerAnalyticsServiceImpl.class);

    @Override
    public CustomerCountDto getCustomerCountAnalytics() {
        Dataset<Row> df = this.extractCustomerDf()
                .select(col("id"), col("is_enabled"), col("customer_meta_id"), col("order_count"));
        long totalCustomer = df.count();
        df = df.filter(col("is_enabled").equalTo("true"));
        long totalCustomerEnabled = df.count();
        df = df.filter(col("customer_meta_id").isNotNull());
        long totalCustomersUpdatedProfile = df.count();
        df = df.filter(col("order_count").isNotNull());
        long totalCustomersBoughtAtLeastOnce = df.count();
        df = df.filter(col("order_count").gt(1));
        long totalRepeatCustomers = df.count();
        return CustomerCountDto.builder()
                .totalCustomer(totalCustomer)
                .totalCustomerEnabled(totalCustomerEnabled)
                .totalCustomersUpdatedProfile(totalCustomersUpdatedProfile)
                .totalCustomersBoughtAtLeastOnce(totalCustomersBoughtAtLeastOnce)
                .totalRepeatCustomers(totalRepeatCustomers)
                .build();
    }

    private Dataset<Row> extractCustomerDf() {
        return extractOperationUtil.ingest(CustomerTable.CUSTOMER_WH, true);
    }
}
