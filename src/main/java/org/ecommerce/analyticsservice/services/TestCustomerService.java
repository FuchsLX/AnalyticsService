package org.ecommerce.analyticsservice.services;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.*;
import org.ecommerce.analyticsservice.constants.CustomerTable;
import org.ecommerce.analyticsservice.constants.RoleTable;
import org.ecommerce.analyticsservice.constants.UserMetaTable;
import org.ecommerce.analyticsservice.services.filter.RoleFilter;
import org.ecommerce.analyticsservice.services.mapper.CustomerMapper;
import org.ecommerce.analyticsservice.models.Customer;
import org.ecommerce.analyticsservice.utils.ExtractOperationUtil;
import org.ecommerce.analyticsservice.utils.LoadOperationUtil;
import org.ecommerce.analyticsservice.utils.impl.ExtractOperationUtilImpl;
import org.ecommerce.analyticsservice.utils.impl.LoadOperationUtilImpl;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
public class TestCustomerService {

    private final ExtractOperationUtil extractOperation;
    private final LoadOperationUtil loadOperation;

    public List<Customer> ingestCustomer() {
        Dataset<Row> customerDf = extractOperation.ingest(CustomerTable.class);
        Dataset<Row> roleDf = extractOperation.ingest(RoleTable.class);
        Dataset<Row> customerDetail = extractOperation.ingest(UserMetaTable.class);

        customerDf = customerDf
                .join(roleDf, customerDf.col("role_id").equalTo(roleDf.col("id")), "inner")
                .join(customerDetail, customerDetail.col("user_id").equalTo(customerDf.col("id")), "inner")
                .filter(new RoleFilter.CustomerRoleFilter())
                .drop(customerDetail.col("id"))
                .drop(customerDetail.col("user_id"))
                .drop(roleDf.col("description"))
                .drop(roleDf.col("id"))
                .drop(roleDf.col("name"))
                .drop(customerDf.col("role_id"))
                .drop(customerDf.col("first_name"))
                .drop(customerDf.col("last_name"))
                .withColumnRenamed("first_name", "firstName")
                .withColumnRenamed("last_name", "lastName")
                .withColumnRenamed("mobile", "phoneNumber")
                .withColumnRenamed("user_meta_gender", "gender");

        loadOperation.save(CustomerTable.class, customerDf);
        return customerDf.map(new CustomerMapper(), Encoders.bean(Customer.class)).collectAsList();
    }
}
