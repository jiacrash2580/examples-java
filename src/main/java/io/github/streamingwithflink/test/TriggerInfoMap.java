package io.github.streamingwithflink.test;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class TriggerInfoMap extends RichFlatMapFunction<Tuple2<String, Long>, Tuple3<String, Integer, Long>> {
    private JedisTools jedisTools;
    public static final long oneMinuteMillis = 60 * 1000;

    @Override
    public void flatMap(Tuple2<String, Long> value, Collector<Tuple3<String, Integer, Long>> out) throws Exception {
        List<Integer> fqList = getFqByCode(jedisTools, value.f0);
        if (fqList.isEmpty())
            return;
        // 对各频率各触发时间，进行slot计算与去重
        for (int j = 0, jsize = fqList.size(); j < jsize; j++) {
            Integer fq = fqList.get(j);
            //减16小时与时区东8区偏移量有关
            // code,fq,slot
            out.collect(Tuple3.of(value.f0, fq, (value.f1 - 16 * 60 * oneMinuteMillis) / (fq * oneMinuteMillis)));
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        jedisTools = new JedisTools();
        jedisTools.setUrl("172.16.10.22:6639");
        jedisTools.init();
    }

    @Override
    public void close() throws Exception {
        jedisTools.destroyPool();
        jedisTools = null;
    }

    public static final String CODE_TO_FQ_PREFIX = "CODE_TO_FQ:_:";

    public static List<Integer> getFqByCode(JedisTools jedisTools, String code) {
        Jedis jedis = null;
        try {
            jedis = (Jedis) jedisTools.getResource();
            Set<String> fqSet = jedis.smembers(CODE_TO_FQ_PREFIX + code);
            List<Integer> result = new ArrayList<>();
            if (!fqSet.isEmpty()) {
                for (String fqStr : fqSet) {
                    result.add(Integer.valueOf(fqStr));
                }
                Collections.sort(result);
            }
            return result;
        } finally {
            JedisTools.close(jedis);
        }
    }
}
