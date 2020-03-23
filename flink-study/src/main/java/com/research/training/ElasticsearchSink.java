package com.research.training;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.HashMap;
import java.util.Map;

/**
 * @fileName: ElasticsearchSink.java
 * @description: ElasticsearchSink.java类说明
 * @author: by echo huang
 * @date: 2020-03-01 14:49
 */
public class ElasticsearchSink implements ElasticsearchSinkFunction<Tuple3<String, String, Long>> {

    @Override
    public void process(Tuple3<String, String, Long> input, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
        requestIndexer.add(createIndexRequest(input));
    }

    private IndexRequest createIndexRequest(Tuple3<String, String, Long> input) {
        Map<String, Object> json = new HashMap<>();
        json.put("time", input.f0);
        json.put("domain", input.f1);
        json.put("triffic_count", input.f2);

        return Requests.indexRequest()
                .index("my_index")
                .type("_doc")
                .source(json);
    }
}
