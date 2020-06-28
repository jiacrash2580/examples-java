package io.github.streamingwithflink.test.fqcalc;

import com.google.gson.internal.LazilyParsedNumber;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.kairosdb.client.builder.DataPoint;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;

public class CalculateFQMap extends RichFlatMapFunction<Tuple3<String, Integer, Long>, Tuple3<String, Long, Double>> {
    public static final int CVT_DIV_DEFAULT_SCALE = 10;
    public static final String SEP = ":_:";
    private KairosDBOperator kairosDBOperator;

    @Override
    public void flatMap(Tuple3<String, Integer, Long> value, Collector<Tuple3<String, Long, Double>> out) throws Exception {
        //f1是频率，f2是slot
        // 根据code和slot做平均值计算处理，然后存入kairosdb，之所以要加16小时，是因为时区东8区的偏移量修正
        Date startTime = new Date(value.f1 * value.f2 * TriggerInfoMap.oneMinuteMillis + (16 * 60 * TriggerInfoMap.oneMinuteMillis));
        // 开始和结束时间是左闭右开，因为时间周期是从0秒开始，59秒结束
        Date endTime = new Date(startTime.getTime() + value.f1 * TriggerInfoMap.oneMinuteMillis - 1);
        List<DataPoint> dpList = kairosDBOperator.queryKDB(value.f0, startTime, endTime, null);
        if (dpList != null && !dpList.isEmpty()) {
            BigDecimal v = new BigDecimal(0);
            for (DataPoint dp : dpList) {
                v = v.add(BigDecimal.valueOf(((LazilyParsedNumber) dp.getValue()).doubleValue()));
            }
            v = v.divide(BigDecimal.valueOf(dpList.size()), CVT_DIV_DEFAULT_SCALE, BigDecimal.ROUND_HALF_EVEN);
            String fqCode = value.f0.concat(SEP).concat(value.f1.toString());
            long midTime = startTime.getTime() / 2 + (endTime.getTime() + 1) / 2;
            out.collect(Tuple3.of(fqCode, midTime, v.doubleValue()));
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        kairosDBOperator = new KairosDBOperator();
        kairosDBOperator.setKairosdbUrl("http://172.16.10.22:8080");
        kairosDBOperator.setKairosDB_min_batch_wait(500);
        kairosDBOperator.init();
    }

    @Override
    public void close() throws Exception {
        kairosDBOperator.destroy();
        kairosDBOperator = null;
    }
}
