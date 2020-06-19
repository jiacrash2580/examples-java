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
package io.github.streamingwithflink.chapter1;

import io.github.streamingwithflink.util.SensorReading;
import io.github.streamingwithflink.util.TestAverager;
import io.github.streamingwithflink.util.TestSource;
import io.github.streamingwithflink.util.TestTimeAssigner;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class TestSensorReadings {

    private static final Logger logger = LoggerFactory.getLogger(TestSensorReadings.class);

    /**
     * main() defines and executes the DataStream program.
     *
     * @param args program arguments
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // use event time for the application
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1 * 1000);

        // ingest sensor stream
        DataStream<SensorReading> sensorData = env
                // SensorSource generates random temperature readings
                .addSource(new TestSource())
                .setParallelism(2)
                .assignTimestampsAndWatermarks(new TestTimeAssigner());

        DataStream<SensorReading> avgTemp = sensorData.keyBy(r -> r.id)
                .timeWindow(Time.seconds(5))
                .apply(new TestAverager());

        FastDateFormat fdf = FastDateFormat.getInstance("HH:mm:ss");

        // print result stream to standard out
        avgTemp.addSink(new RichSinkFunction<SensorReading>() {
            @Override
            public void invoke(SensorReading value, Context context) throws Exception {
                logger.info(fdf.format(context.timestamp()) + " < " + value.toString() + " > " + fdf.format(context.currentProcessingTime()));
            }
        });

        // execute application
        env.execute("Compute average sensor temperature");
    }

}
