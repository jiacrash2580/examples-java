package io.github.streamingwithflink.test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.Aggregator;
import org.kairosdb.client.builder.AggregatorFactory;
import org.kairosdb.client.builder.DataPoint;
import org.kairosdb.client.builder.Metric;
import org.kairosdb.client.builder.MetricBuilder;
import org.kairosdb.client.builder.QueryBuilder;
import org.kairosdb.client.builder.QueryMetric;
import org.kairosdb.client.builder.TimeUnit;
import org.kairosdb.client.response.GetResponse;
import org.kairosdb.client.response.Query;
import org.kairosdb.client.response.QueryResponse;
import org.kairosdb.client.response.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("rawtypes")
public class KairosDBOperator
{
	private static Logger logger = LoggerFactory.getLogger(KairosDBOperator.class);

	private HttpClient client = null;

	private String kairosdbUrl = null;

	public void setKairosdbUrl(String kairosdbUrl) {
		this.kairosdbUrl = kairosdbUrl;
	}

	/**
	 * KairosDB写入最小等待延迟（因为KairosDB是队列异步批量写入方式，所以有写入延迟，该时间后可读到写入数据）
	 */
	private int kairosDB_min_batch_wait = 500;

	public int getKairosDB_min_batch_wait() {
		return kairosDB_min_batch_wait;
	}

	public void setKairosDB_min_batch_wait(int kairosDB_min_batch_wait) {
		this.kairosDB_min_batch_wait = kairosDB_min_batch_wait;
	}

	public void init() throws Exception {
		this.client = new HttpClient(kairosdbUrl);
	}

	public class KairosDataPoint
	{
		public static final String time = "time";
		public static final String value = "value";
	}

	public void destroy() {
		if (client != null)
		{
			try
			{
				client.shutdown();
			} catch (Exception e)
			{
				logger.error(e.getMessage(), e);
			}
		}
	}

	public void flushDB() {
		try
		{
			GetResponse gr = client.getMetricNames();
			for (String mtn : gr.getResults())
			{
				client.deleteMetric(mtn);
			}
		} catch (Exception e)
		{
			logger.error(e.getMessage(), e);
		}
	}

	public void addKDB(String metric, List<Map> dataPoints, Map<String, String> tagMap) throws Exception {
		MetricBuilder builder = MetricBuilder.getInstance();
		Metric mt = builder.addMetric(metric);
		for (String tagKey : tagMap.keySet())
		{
			mt.addTag(tagKey, tagMap.get(tagKey));
		}
		for (Map dataPoint : dataPoints)
		{
			mt.addDataPoint((long) dataPoint.get(KairosDataPoint.time), dataPoint.get(KairosDataPoint.value));
		}
		client.pushMetrics(builder);
	}

	public void remKDB(String metric, long time) throws Exception {
		Date dataTime = new Date(time);
		QueryBuilder builder = QueryBuilder.getInstance();
		builder.setStart(dataTime);
		builder.setEnd(dataTime);
		builder.addMetric(metric);
		client.delete(builder);
	}

	public List<Integer> queryFqMetricNames(String metric) throws Exception {
		GetResponse res = client.getMetricNames();
		List<String> all = res.getResults();
		List<Integer> ret = new ArrayList<Integer>();
		for (String each : all)
		{
			if (StringUtils.startsWithIgnoreCase(each, metric + ":_:"))
			{
				ret.add(Integer.valueOf(StringUtils.substringAfterLast(each, ":_:")));
			}
		}
		return ret;
	}

	public void deleteMetric(String metric) throws Exception {
		client.deleteMetric(metric);
	}

	public List<DataPoint> queryKDB(String metric, int start, TimeUnit startUnit, Date endTime, Aggregator aggregator) {
		List<DataPoint> result = new ArrayList<DataPoint>();
		try
		{
			if (StringUtils.isBlank(metric))
			{
				logger.error("metric is blank, please check !!!");
				return result;
			}
			if (start < 1)
			{
				logger.error("start time must > 0 !!!");
				return result;
			}
			QueryBuilder builder = QueryBuilder.getInstance();
			builder.setStart(start, startUnit);
			if (endTime != null)
			{
				builder.setEnd(endTime);
			}
			QueryMetric qm = builder.addMetric(metric);
			if (aggregator != null)
			{
				qm.addAggregator(aggregator);
			}
			List<Query> res = client.query(builder).getQueries();
			if (!res.isEmpty())
			{
				List<Result> results = res.get(0).getResults();
				if (!results.isEmpty())
				{
					result = results.get(0).getDataPoints();
				}
			}
		} catch (Exception e)
		{
			logger.error(e.getMessage(), e);
		}
		return result;
	}

	public List<DataPoint> queryKDB(String metric, Date startTime, Date endTime, List<Aggregator> aggrList) {
		List<DataPoint> result = new ArrayList<DataPoint>();
		try
		{
			if (StringUtils.isBlank(metric))
			{
				logger.error("metric is blank, please check !!!");
				return result;
			}
			if (startTime == null)
			{
				logger.error("start time must != null !!!");
				return result;
			}
			QueryBuilder builder = QueryBuilder.getInstance();
			builder.setStart(startTime);
			if (endTime != null)
			{
				builder.setEnd(endTime);
			}
			QueryMetric qm = builder.addMetric(metric);
			if (aggrList != null && !aggrList.isEmpty())
			{
				for (Aggregator aggr : aggrList)
				{
					qm.addAggregator(aggr);
				}
			}
			List<Query> res = client.query(builder).getQueries();
			if (!res.isEmpty())
			{
				List<Result> results = res.get(0).getResults();
				if (!results.isEmpty())
				{
					result = results.get(0).getDataPoints();
				}
			}
		} catch (Exception e)
		{
			logger.error(e.getMessage(), e);
		}
		return result;
	}

	public DataPoint queryNextPoint(String metric, Date startTime) {
		DataPoint result = null;
		try
		{
			if (StringUtils.isBlank(metric))
			{
				logger.error("metric is blank, please check !!!");
				return result;
			}
			if (startTime == null)
			{
				logger.error("start time must != null !!!");
				return result;
			}
			QueryBuilder builder = QueryBuilder.getInstance();
			// 由于kairosdb查询时左右闭区间，所以防止next数据点查重，使用时间点后1ms作为查询的开始时间
			builder.setStart(new Date(startTime.getTime() + 1)).addMetric(metric).setLimit(1);
			QueryResponse res = client.query(builder);
			List<DataPoint> results = res.getQueries().get(0).getResults().get(0).getDataPoints();
			if (results != null && !results.isEmpty())
			{
				result = results.get(0);
			}
		} catch (Exception e)
		{
			logger.error(e.getMessage(), e);
		}
		return result;
	}

	public DataPoint queryFirstPoint(String metric, Date startTime, Date endTime) {
		DataPoint result = null;
		try
		{
			if (StringUtils.isBlank(metric))
			{
				logger.error("metric is blank, please check !!!");
				return result;
			}
			if (startTime == null || endTime == null)
			{
				logger.error("start or end time must != null !!!");
				return result;
			}
			QueryBuilder builder = QueryBuilder.getInstance();
			// 由于kairosdb查询时左右闭区间，所以防止next数据点查重，使用时间点后1ms作为查询的开始时间
			builder.setStart(new Date(startTime.getTime() + 1)).setEnd(new Date(endTime.getTime())).addMetric(metric).setLimit(1);
			QueryResponse res = client.query(builder);
			List<DataPoint> results = res.getQueries().get(0).getResults().get(0).getDataPoints();
			if (results != null && !results.isEmpty())
			{
				result = results.get(0);
			}
		} catch (Exception e)
		{
			logger.error(e.getMessage(), e);
		}
		return result;
	}
	
	public DataPoint queryLastPoint(String metric, Date startTime, Date endTime) {
		DataPoint result = null;
		try
		{
			if (StringUtils.isBlank(metric))
			{
				logger.error("metric is blank, please check !!!");
				return result;
			}
			if (startTime == null || endTime == null)
			{
				logger.error("start or end time must != null !!!");
				return result;
			}
			QueryBuilder builder = QueryBuilder.getInstance();
			// 由于kairosdb查询时左右闭区间，所以防止next数据点查重，使用时间点后1ms作为查询的开始时间
			int interval = (int) (endTime.getTime() / 1000 - startTime.getTime() / 1000) / 60 + 1;
			builder.setStart(new Date(startTime.getTime() + 1)).setEnd(new Date(endTime.getTime())).addMetric(metric)
					.addAggregator(AggregatorFactory.createLastAggregator(interval, TimeUnit.MINUTES));
			QueryResponse res = client.query(builder);
			if (res.getQueries().isEmpty())
			{
				logger.error("queryKDB error={}", res.getErrors());
			} else
			{
				List<DataPoint> results = res.getQueries().get(0).getResults().get(0).getDataPoints();
				if (results != null && !results.isEmpty())
				{
					result = results.get(0);
				}
			}
		} catch (Exception e)
		{
			logger.error(e.getMessage(), e);
		}
		return result;
	}
}
