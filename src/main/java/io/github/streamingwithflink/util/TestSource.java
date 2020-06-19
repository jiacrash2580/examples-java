package io.github.streamingwithflink.util;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Calendar;

public class TestSource extends RichParallelSourceFunction<SensorReading> {

    private boolean running = true;
    private double curT = 0;

    @Override
    public void run(SourceContext<SensorReading> ctx) throws Exception {
        curT = 100 * getRuntimeContext().getIndexOfThisSubtask();
        while (running) {
            Calendar cal = Calendar.getInstance();
            ctx.collect(new SensorReading("task-1", cal.getTimeInMillis(), curT++));
            ctx.collect(new SensorReading("task-2", cal.getTimeInMillis(), curT++));
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
