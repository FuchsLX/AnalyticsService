package org.ecommerce.analyticsservice.services;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.ecommerce.analyticsservice.constants.OrderItemTable;
import org.ecommerce.analyticsservice.constants.OrderTable;
import org.ecommerce.analyticsservice.constants.TransactionTable;
import org.ecommerce.analyticsservice.utils.ExtractOperationUtil;
import org.ecommerce.analyticsservice.utils.LoadOperationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;


@Service
@RequiredArgsConstructor
public class OrderAnalyticService {
    private final ExtractOperationUtil extractOperation;
    private final LoadOperationUtil loadOperation;

    private static final Logger logger = LoggerFactory.getLogger(OrderAnalyticService.class);

    public void test() {
        Dataset<Row> ordersDf = extractOperation.ingest(OrderTable.class);
        Dataset<Row> orderItemDf = extractOperation.ingest(OrderItemTable.class);
        Dataset<Row> transactionDf = extractOperation.ingest(TransactionTable.class);

    }
}
