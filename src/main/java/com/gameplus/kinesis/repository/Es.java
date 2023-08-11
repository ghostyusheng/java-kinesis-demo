package com.gameplus.kinesis.repository;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.gameplus.kinesis.config.Config;
import org.apache.http.HttpHost;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class Es {
    private static final Logger log = LogManager.getLogger(Es.class);
    static RestHighLevelClient conn = null;
    public static Es instance;

    public static Es initConn () {
        if (conn != null) {
            return instance;
        }
        RequestConfig requestConfig = RequestConfig.custom()
                .setSocketTimeout(Integer.MAX_VALUE)
                .setConnectTimeout(Integer.MAX_VALUE)
                .build();

        RestClientBuilder builder = RestClient.builder(new HttpHost(Config.ES_HOST, Integer.parseInt(Config.ES_PORT), "http"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        CredentialsProvider credentialsProvider;
                        return httpClientBuilder.setDefaultRequestConfig(requestConfig)
                                .setMaxConnTotal(100)
                                .setMaxConnPerRoute(1000) ;//此处为多并发设置  ;
                    }
                });
        RestHighLevelClient conn = new RestHighLevelClient(builder);
        Es.conn = conn;
        Es es = new Es();
        Es.instance = es;
        return es;
    }

    public static Es upsert(String index, String id, HashMap doc) throws IOException {
        IndexRequest request = new IndexRequest(index);
        request.id(id);
        request.source(doc);
        request.sourceAsMap();
        conn.index(request, RequestOptions.DEFAULT);
        log.info(String.format("upsert %s => id: %s ok!", index, id));
        return Es.instance;
    }

    public static Es update(String index, String id, HashMap<String, JSONArray> map) throws IOException {
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index(index);
        updateRequest.id(id);
        XContentBuilder json = jsonBuilder().startObject();
        for(Map.Entry<String, JSONArray> entry : map.entrySet()) {
            String key = entry.getKey();
            JSONArray value = entry.getValue();
            json = json.field(key, value);
        }
        updateRequest.doc(
                json
                .endObject()
        );
        UpdateResponse response = conn.update(updateRequest, RequestOptions.DEFAULT);

        log.info(String.format("update %s => id: %s ok!", index, id));
        return Es.instance;
    }

    public static Es delete(String index, String id) throws IOException {
        DeleteRequest deleteDocumentRequest = new DeleteRequest(index, id);
        DeleteResponse deleteResponse = conn.delete(deleteDocumentRequest, RequestOptions.DEFAULT);
        return Es.instance;
    }

    public static String getById(String index, String id) throws IOException {
        GetRequest getRequest = new GetRequest(index, id);
        GetResponse response = conn.get(getRequest, RequestOptions.DEFAULT);

        return response.getSourceAsString();
    }

    public static JSONArray getByKey(String index, String key, String val, int size) throws IOException {
        SearchRequest searchRequest = new SearchRequest(index);

        QueryBuilder matchQueryBuilder = QueryBuilders.termQuery(key, val);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(matchQueryBuilder).size(size);
        searchRequest.source(sourceBuilder);
        SearchResponse response = conn.search(searchRequest,RequestOptions.DEFAULT);

        JSONObject res = (JSONObject) JSONObject.parse(response.toString());
        JSONObject hits = (JSONObject)res.get("hits");
        if (hits == null) {
            return null;
        }
        JSONArray data = (JSONArray) hits.get("hits");
        JSONArray arr = new JSONArray();
        for (Object _val : data) {
            JSONObject __val = (JSONObject) _val;
            arr.add(__val.get("_source"));
        }
        return arr;
    }
}
