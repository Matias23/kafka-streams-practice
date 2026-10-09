package com.mricotta.kafkalab.entity;

import java.math.BigDecimal;
import java.time.Instant;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.DateFormat;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;
import org.springframework.data.elasticsearch.annotations.InnerField;
import org.springframework.data.elasticsearch.annotations.MultiField;
import org.springframework.data.elasticsearch.annotations.WriteTypeHint;

// One document per order in the "orders" index; the annotations define the index mapping.
// No type hint: documents hold only business fields, not the Java class name.
@Document(indexName = OrderDocument.INDEX, writeTypeHint = WriteTypeHint.FALSE)
public record OrderDocument(
        // used as the document _id: indexing the same order again overwrites it instead of duplicating it
        @Id @Field(type = FieldType.Keyword) String orderId,
        // keyword: stored as a single exact value, for filters and aggregations
        @Field(type = FieldType.Keyword) String customerId,
        // text: analyzed into lowercase words for full-text search; the keyword sub-field keeps the exact value
        @MultiField(
                mainField = @Field(type = FieldType.Text),
                otherFields = @InnerField(suffix = "keyword", type = FieldType.Keyword))
        String product,
        @Field(type = FieldType.Double) BigDecimal amount,
        @Field(type = FieldType.Date, format = DateFormat.date_time) Instant createdAt) {

    public static final String INDEX = "orders";
}
