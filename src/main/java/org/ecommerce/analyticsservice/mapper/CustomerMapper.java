package org.ecommerce.analyticsservice.mapper;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.ecommerce.analyticsservice.models.Customer;

public class CustomerMapper implements MapFunction<Row, Customer> {

    @Override
    public Customer call(Row row) throws Exception {
        Customer cus = new Customer();
        cus.setId(row.getAs("id"));
        cus.setEmail(row.getAs("email"));
        cus.setFirstName(row.getAs("firstName"));
        cus.setLastName(row.getAs("lastName"));
        cus.setAddress(row.getAs("address"));
        cus.setMobileNumber(row.getAs("phoneNumber"));
        cus.setGender(row.getAs("gender"));
        return cus;
    }

}
