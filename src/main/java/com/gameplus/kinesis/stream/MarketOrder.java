package com.gameplus.kinesis.stream;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.gameplus.kinesis.repository.Es;
import com.gameplus.kinesis.service.Nft;
import com.gameplus.kinesis.utils.CustomDate;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MarketOrder {
    private static final Logger log = LogManager.getLogger(MarketOrder.class);
    static String table = "t_market_order";
    static Es es = Es.initConn();
    static String[] landFields = new String[]{"id", "game_name", "nft_name", "maker_user_id", "taker_user_id", "nft_id", "game_id", "token_id", "num", "maker", "taker", "unit_price", "base_price", "hash", "coin_id", "coin", "listing_time", "expire_time", "nft_image", "status", "trans_serial_no"};

    public static void handler(String index, JSONObject data, String operation) throws Exception {
        if (!index.equals(table)) {
            return;
        }
        String id = data.get("id").toString();
        String create_time = CustomDate.normalize(data.get("create_time").toString(), "yyyy-MM-dd'T'HH:mm:ss'Z'");
        String modify_time = CustomDate.normalize(data.get("modify_time").toString(), "yyyy-MM-dd'T'HH:mm:ss'Z'");
        String nft_name = (String) data.getOrDefault("nft_name", "");
        String nft_id = (String) data.getOrDefault("nft_id", "");
        String token_id = String.valueOf(data.getOrDefault("token_id", ""));
        String game_id = String.valueOf(data.getOrDefault("game_id", ""));
        String collection_id = data.get("collection_id").toString();
        BigDecimal base_price1 = new BigDecimal(data.get("base_price").toString());
        BigDecimal unit_price1 = new BigDecimal(data.get("unit_price").toString());
        BigDecimal ruler = new BigDecimal("1000000000000000000");
        String unit_price = unit_price1.divide(ruler).toPlainString();
        String base_price = base_price1.divide(ruler).toPlainString();
        if (operation.equals("delete")) {
            log.info("ready to delete " + id);
            Es.delete(index, id);
            return;
        }
        HashMap map = new HashMap<>();
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            String field = entry.getKey();
            String value = entry.getValue().toString();
            boolean res = Arrays.asList(landFields).contains(field);
            if (!res) {
                continue;
            }
            map.put(field, value);
        }

        Map attr = Nft.getNftAttrsByNftId(nft_id);
        map.put("attr", attr);

        map.put("create_time", create_time);
        map.put("modify_time", modify_time);
        map.put("nft_name_token_id", nft_name + " " + token_id);
        map.put("game_id", game_id);
        map.put("base_price", base_price);
        map.put("base_price_str", base_price);
        map.put("unit_price", unit_price);
        map.put("unit_price_str", unit_price);
        map.put("id", id);
        map.put("collection_id", collection_id);
        String label_ids = Nft.getNftLabelIdsByNftId(nft_id);
        log.info("id = {} , label_ids = {} ", id, label_ids);
        if (label_ids == null) {
            MarketOrder.es.upsert(index, id, map);
            return;
        }
        if (label_ids == null) {
            label_ids = "";
        }
        String[] ids = label_ids.split(",");
        List<String> ids_list = Arrays.asList(ids);
        JSONArray ids_arr = JSONArray.parseArray(JSON.toJSONString(ids_list));
        map.put("nft_label_ids", ids_arr);
        MarketOrder.es.upsert(index, id, map);
    }
}
