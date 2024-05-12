package org.ecommerce.analyticsservice.controller;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.ecommerce.analyticsservice.models.Customer;
import org.ecommerce.analyticsservice.services.ProductAnalyticsService;
import org.ecommerce.analyticsservice.services.TestCustomerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

@Controller
@RequiredArgsConstructor
public class DemoController {

    private final SparkSession sparkSession;
    private static final Logger logger = LoggerFactory.getLogger(DemoController.class);
    private final TestCustomerService customerService;
    private final ProductAnalyticsService productAnalyticsService;

    @GetMapping("/test")
    public ResponseEntity<String> test() {
        logger.info("test GET method");
        productAnalyticsService.categoryTest();
        return ResponseEntity.ok("Hello World");
    }

    @GetMapping("/test-spark")
    public ResponseEntity<List<Customer>> testSpark() {
        logger.info("test spark GET method");
        logger.info("Using Apache Spark with v{}", sparkSession.version());

//        Properties prop = new Properties();
//        prop.setProperty("user", "postgres");
//        prop.setProperty("password", "123456789");
//        Dataset<Row> df = sparkSession.read().jdbc("jdbc:postgresql://localhost:5432/web7_2024", "users", prop);
//        df.show(7, 70);
//        logger.info("Dataframe's schema:");
//        df.printSchema();
        return ResponseEntity.ok(customerService.ingestCustomer());

//        List<Customer> customers = new ArrayList<>(Arrays.asList(
//                Customer.builder().id("123456789").address("test1@Gmail.com").build(),
//                Customer.builder().id("123456789").address("test2@Gmail.com").build(),
//                Customer.builder().id("123456789").address("test3@Gmail.com").build()
//        ));
//        return ResponseEntity.ok(customers);
    }
}
