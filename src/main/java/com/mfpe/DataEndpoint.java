package com.mfpe;

import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.PathVariable;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.asyncsearch.AsyncSearchResponse;
import org.elasticsearch.client.asyncsearch.SubmitAsyncSearchRequest;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Controller("/data")
public class DataEndpoint {

    private static final Logger LOG = LoggerFactory.getLogger(DataEndpoint.class);

    private final RestHighLevelClient client;

    public DataEndpoint(RestHighLevelClient client) {
        this.client = client;
    }

    @ExecuteOn(TaskExecutors.IO)
    @Get("/document/{id}")
    public String byId(@PathVariable("id") String id) throws IOException {
        var response = client.get(new GetRequest(Constants.INDEX, id), RequestOptions.DEFAULT);
        LOG.debug("Response /document/ {} => {}", id, response.getSourceAsString());
        return response.getSourceAsString();
    }

    @Get("/document-async/{id}")
    public CompletableFuture<String> byIdAsync(@PathVariable("id") String id) {
        final CompletableFuture<String> whenDone = new CompletableFuture<>();
        client.getAsync(new GetRequest(Constants.INDEX, id), RequestOptions.DEFAULT, new ActionListener<>() {
            @Override
            public void onResponse(GetResponse response) {
                LOG.debug("Response /document-async/ {} => {}", id, response.getSourceAsString());
                whenDone.complete(response.getSourceAsString());
            }

            @Override
            public void onFailure(Exception e) {
                whenDone.completeExceptionally(e);
            }
        });
        return whenDone;
    }

    @Get("/document-async/firstname/{search}")
    public CompletableFuture<String> byFirstnameAsync(@PathVariable("search") String search) {
        var whenDone = new CompletableFuture<String>();
        SearchSourceBuilder source = new SearchSourceBuilder()
                .query(QueryBuilders.matchQuery("first_name", search));
        client.asyncSearch().submitAsync(new SubmitAsyncSearchRequest(source, Constants.INDEX), RequestOptions.DEFAULT, new ActionListener<>() {
            @Override
            public void onResponse(AsyncSearchResponse searchResponse) {
                var hits = searchResponse.getSearchResponse().getHits().getHits();
                List<String> response  = Stream.of(hits).map(SearchHit::getSourceAsString).collect(Collectors.toList());
                LOG.debug("Response /document-async/firstname {} => {}", search, response);
                whenDone.complete(response.toString());
            }

            @Override
            public void onFailure(Exception e) {
                whenDone.completeExceptionally(e);
            }
        });
        return whenDone;
    }

}
