package io.github.streamingwithflink.test.fqcalc;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

public class FQSource extends RichSourceFunction<Tuple2<String, Long>> {
    private static final Logger logger = LoggerFactory.getLogger(FQSource.class);
    private static final FastDateFormat fdf = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
    private static final Calendar cal1 = Calendar.getInstance(), cal2 = Calendar.getInstance(), cal3 = Calendar.getInstance(), cal4 = Calendar.getInstance();
    private static final Map<String, Integer> codeInfo = new HashMap<>();

    static {
        //电瞬时负荷是1分钟频率的数据
        codeInfo.put("YDFH_SSFH", 1);
    }

    private boolean running = true;

    @Override
    public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
        for (String code : codeInfo.keySet()) {
            int offset = codeInfo.get(code);

            //准备模拟时间段
            cal1.setTime(fdf.parse("2020-06-21 00:00:00"));
            cal2.setTime(fdf.parse("2020-07-21 23:59:00"));
            int i = 0;
            for (; cal1.before(cal2); cal1.add(Calendar.MINUTE, offset)) {
                Map<String, String> data = new HashMap<>();
                data.put("TARGET", code);
                data.put("TIME", Long.toString(cal1.getTimeInMillis()));
                ctx.collect(Tuple2.of(code, cal1.getTimeInMillis()));
                i++;
                if (i >= 10000) {
                    while (running) {
                        Thread.sleep(1000);
                    }
                    return;
                }
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
