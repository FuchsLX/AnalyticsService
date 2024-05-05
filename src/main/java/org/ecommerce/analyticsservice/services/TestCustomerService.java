package org.ecommerce.analyticsservice.services;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.*;
import org.ecommerce.analyticsservice.constants.CustomerTable;
import org.ecommerce.analyticsservice.constants.RoleTable;
import org.ecommerce.analyticsservice.constants.UserMetaTable;
import org.ecommerce.analyticsservice.filter.RoleFilter;
import org.ecommerce.analyticsservice.mapper.CustomerMapper;
import org.ecommerce.analyticsservice.models.Customer;
import org.ecommerce.analyticsservice.utils.IngestOperationUtil;
import org.ecommerce.analyticsservice.utils.LoadOperationUtil;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Properties;

@Service
@RequiredArgsConstructor
public class TestCustomerService {


    private final IngestOperationUtil ingestOperation;
    private final LoadOperationUtil loadOperation;

    public List<Customer> ingestCustomer() {
        Dataset<Row> customerDf = ingestOperation.extract(CustomerTable.class);
        Dataset<Row> roleDf = ingestOperation.extract(RoleTable.class);
        Dataset<Row> customerDetail = ingestOperation.extract(UserMetaTable.class);

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

        loadOperation.load(CustomerTable.class, customerDf);
        return customerDf.map(new CustomerMapper(), Encoders.bean(Customer.class)).collectAsList();
    }

    public void testGenerics() {
//        operations.ingest()
    }
}
