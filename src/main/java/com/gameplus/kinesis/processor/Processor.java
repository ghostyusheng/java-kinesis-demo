package com.gameplus.kinesis.processor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.gameplus.kinesis.stream.MarketOrder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class Processor {
    private static final Logger log = LogManager.getLogger(Processor.class);

    public void processOneRecord(ConsumerRecord record) {

        try {
            String s = record.value().toString();
            JSONObject obj = (JSONObject) JSON.parse(s);
            JSONObject metadata = (JSONObject) obj.get("metadata");
            if (metadata == null) {
                return;
            }
            String operation = metadata.getString("operation");
            if (!(operation.equals("insert") || operation.equals("update") || operation.equals("load") || operation.equals("delete"))) {
                log.error("ERROR operation " + operation);
                return;
            }

            String index = metadata.getString("table-name");
            JSONObject data = (JSONObject) obj.get("data");
            String id = data.get("id").toString();
            log.info(String.format("==> [%s] %s %s", index, operation, id));
            MarketOrder.handler(index, data, operation);
        } catch (Exception e) {
            log.error("process 数据出错 e = {} ,  data =  {}", e, record.value());
        }

    }
}