package org.ecommerce.analyticsservice.services.impl;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Encoders;
import org.ecommerce.analyticsservice.constants.table.TagTable;
import org.ecommerce.analyticsservice.dtos.TagProductCounterDto;
import org.ecommerce.analyticsservice.mapper.TagProductCounterDtoMapper;
import org.ecommerce.analyticsservice.services.TagAnalyticsService;
import org.ecommerce.analyticsservice.utils.ExtractOperationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
public class TagAnalyticsServiceImpl implements TagAnalyticsService {
    private final ExtractOperationUtil extractOperationUtil;
    private static final Logger logger = LoggerFactory.getLogger(TagAnalyticsServiceImpl.class);

    @Override
    public List<TagProductCounterDto> getTagProductCount() {
        return extractOperationUtil.ingest(TagTable.TAG_PRODUCT_COUNTER_WH, true)
                .map(new TagProductCounterDtoMapper(), Encoders.bean(TagProductCounterDto.class))
                .collectAsList();
    }
}
