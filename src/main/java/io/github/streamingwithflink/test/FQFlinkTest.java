/*
 * Copyright 2015 Fabian Hueske / Vasia Kalavri
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.streamingwithflink.test;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FQFlinkTest {

    private static final Logger logger = LoggerFactory.getLogger(FQFlinkTest.class);

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        FastDateFormat fdf = FastDateFormat.getInstance("HH:mm:ss");

        env.addSource(new FQSource())
                .flatMap(new TriggerInfoMap())
                .flatMap(new CalculateFQMap())
                .setParallelism(16)
                .addSink(new RichSinkFunction<Tuple3<String, Long, Double>>() {
                    @Override
                    public void invoke(Tuple3<String, Long, Double> value, Context context) throws Exception {
                        logger.info("sink code:{} time:{} value:{}", value.f0, fdf.format(value.f1), value.f2);
                    }
                });

        // execute application
        env.execute("计算降频任务");
    }

}
