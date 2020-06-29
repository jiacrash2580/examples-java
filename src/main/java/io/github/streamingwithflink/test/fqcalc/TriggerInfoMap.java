package io.github.streamingwithflink.test.fqcalc;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.RedisPipeline;

import java.util.*;

public class TriggerInfoMap extends RichFlatMapFunction<Tuple2<String, Long>, List<Tuple3<String, Integer, Long>>> {
    private JedisTools jedisTools;
    public static final long oneMinuteMillis = 60 * 1000;

    public static final String FQ_PROCESS_SLOT = "FQ_PROCESS_SLOT";

    @Override
    public void flatMap(Tuple2<String, Long> value, Collector<List<Tuple3<String, Integer, Long>>> out) throws Exception {
        List<Integer> fqList = getFqByCode(jedisTools, value.f0);
        if (fqList.isEmpty()) {
            return;
        }
        List<Tuple3<String, Integer, Long>> triggerInfo = new ArrayList<>();
        // 对各频率各触发时间，进行slot计算与去重
        for (int j = 0, jsize = fqList.size(); j < jsize; j++) {
            Integer fq = fqList.get(j);
            //减16小时与时区东8区偏移量有关
            // code,fq,slot
            triggerInfo.add(Tuple3.of(value.f0, fq, (value.f1 - 16 * 60 * oneMinuteMillis) / (fq * oneMinuteMillis)));
        }
        Jedis jedis = null;
        try {
            jedis = (Jedis) jedisTools.getResource();
            // 获取各频率触发通道中，是否已存在一样的触发信息
            RedisPipeline pipeline = JedisTools.getPipeline(jedis);
            int pipeCount = 0;
            List<Boolean> ismemberInfo = new ArrayList<Boolean>();
            for (Tuple3<String, Integer, Long> t3 : triggerInfo) {
                String slotV = t3.f0 + "_" + t3.f1.toString() + "_" + t3.f2.toString();
                pipeline.sismember(FQ_PROCESS_SLOT, slotV);
                pipeCount++;
                if (pipeCount >= JedisTools.pipeLineMax) {
                    ismemberInfo.addAll(JedisTools.pipelineSyncAndReturnAll(pipeline));
                }
            }
            if (pipeCount > 0) {
                ismemberInfo.addAll(JedisTools.pipelineSyncAndReturnAll(pipeline));
            }
            // 不存在的触发信息进行新增
            pipeCount = 0;
            int pipeIndex = 0;
            Iterator<Tuple3<String, Integer, Long>> it = triggerInfo.iterator();
            while (it.hasNext()) {
                Tuple3<String, Integer, Long> t3 = it.next();
                if (ismemberInfo.get(pipeIndex)) {
                    it.remove();
                } else {
                    // score等于null的，说明需要新增信息，非null的不用增加，因为不需要修改score，score会影响降频处理顺序
                    String slotV = t3.f0 + "_" + t3.f1.toString() + "_" + t3.f2.toString();
                    pipeline.sadd(FQ_PROCESS_SLOT, slotV);
                    pipeCount++;
                    if (pipeCount >= JedisTools.pipeLineMax) {
                        JedisTools.pipelineSync(pipeline);
                    }
                }
            }
            if (pipeCount > 0) {
                JedisTools.pipelineSync(pipeline);
            }
        } finally {
            JedisTools.close(jedis);
        }
        if (triggerInfo.isEmpty()) {
            return;
        }
        out.collect(triggerInfo);
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
