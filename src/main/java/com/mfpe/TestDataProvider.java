package com.mfpe;

import com.github.javafaker.Faker;
import io.micronaut.scheduling.annotation.Scheduled;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.HasAttributeNodeSelector;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.util.HashMap;
import java.util.UUID;

@Singleton
public class TestDataProvider {

    private static final Logger LOG = LoggerFactory.getLogger(TestDataProvider.class);
    private static final Faker faker = new Faker();
    private final RestHighLevelClient client;

    public TestDataProvider(RestHighLevelClient client) {
        this.client = client;
    }

    @Scheduled(fixedDelay = "10s")
    void insertDocument(){
        var document = new HashMap<>();
        document.put("first_name", faker.name().firstName());
        document.put("last_name", faker.name().lastName());

        IndexRequest indexRequest = new IndexRequest()
                .index("mn-es-idx")
                .id(UUID.randomUUID().toString())
                .source(document, XContentType.JSON);

        client.indexAsync(indexRequest, RequestOptions.DEFAULT, new ActionListener<>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                LOG.debug("Added document {} with id {}", document, indexResponse.getId());
            }

            @Override
            public void onFailure(Exception e) {
                LOG.error("Failed to insert document:", e);
            }
        });
    }



}