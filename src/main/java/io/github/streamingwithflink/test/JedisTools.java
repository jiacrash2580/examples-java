package io.github.streamingwithflink.test;

import java.io.Closeable;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.RedisPipeline;

public class JedisTools
{
	private static Logger logger = LoggerFactory.getLogger(JedisTools.class);
	public static final int pipeLineMax = 512;

	private int jedisType = -1;
	private JedisPool pool = null;
	private String url = null;

	public void setUrl(String url) {
		this.url = url;
	}

	private String model = "redis";

	/**
	 * "redis","codis"2种值<br>
	 * 默认"redis"<br>
	 * 
	 * @param model
	 */
	public void setModel(String model) {
		this.model = model;
	}

	private int dbIndex = 0;

	/**
	 * redis模式下，可以选择使用的DB，不同DB之间不互相影响，数据隔离
	 * 
	 * @param dbIndex
	 */
	public void setDbIndex(int dbIndex) {
		this.dbIndex = dbIndex;
	}

	private String pwd = "";

	/**
	 * 连接密码，在redis的requirepass或codis的session_auth
	 * 
	 * @param pwd
	 */
	public void setPwd(String pwd) {
		this.pwd = pwd;
	}

	public synchronized JedisCommands getResource() {
		try
		{
			JedisCommands jedis = null;
			if (this.jedisType == 0)
			{
				jedis = pool.getResource();
				if (dbIndex != 0)
				{
					((Jedis) jedis).select(dbIndex);
				}
				return jedis;
			} else
			{
				logger.error("JedisTools getResource error, not init!!");
				return null;
			}
		} catch (Exception var2)
		{
			logger.error("getResource error {} {}", var2.getMessage(), var2);
			return null;
		}
	}

	public static RedisPipeline getPipeline(JedisCommands jedis) {
		return (RedisPipeline) (jedis instanceof Jedis ? ((Jedis) jedis).pipelined() : null);
	}

	public static void pipelineSync(RedisPipeline pipeLine) {
		if (pipeLine instanceof Pipeline)
		{
			((Pipeline) pipeLine).sync();
		}
	}

	@SuppressWarnings("rawtypes")
	public static List pipelineSyncAndReturnAll(RedisPipeline pipeLine) {
		return pipeLine instanceof Pipeline ? ((Pipeline) pipeLine).syncAndReturnAll() : null;
	}

	public void flushRedis() {
		JedisCommands jedis = null;
		try
		{
			jedis = this.getResource();
			if (this.jedisType == 0)
			{
				logger.info("flush remote redis start");
				((Jedis) jedis).flushDB();
				logger.info("flush remote redis done");
			} else
			{
				logger.error("flush redis error jedis init fail");
			}
		} catch (Exception var8)
		{
			logger.error("flush redis error {}, {}", var8.getMessage(), var8);
		} finally
		{
			JedisTools.close(jedis);
		}

	}

	public static void close(JedisCommands jedis) {
		try
		{
			if (jedis != null)
			{
				((Closeable) jedis).close();
			}
		} catch (Exception var3)
		{
			logger.error("jedis close error {}", jedis);
		}

	}

	public void init() {
		try
		{
			if (StringUtils.isNotBlank(this.url))
			{
				if (StringUtils.equals(model, "redis"))
				{
					String[] ipPort = url.split(":");
					if (ipPort.length != 2)
					{
						logger.error("init error, url formate error, {}", url);
						return;
					}
					if (StringUtils.isNotBlank(pwd))
					{
						pool = new JedisPool(new JedisPoolConfig(), ipPort[0], Integer.parseInt(ipPort[1]), 0, pwd);
					} else
					{
						pool = new JedisPool(new JedisPoolConfig(), ipPort[0], Integer.parseInt(ipPort[1]), 0);
					}
					jedisType = 0;
					logger.info("init remote redis client {} dbIndex {}", url, dbIndex);
				} else
				{
					logger.error("error model param,please check!!!!!!!!");
				}
			} else
			{
				logger.error("redis url is blank,please check!!!!!!!!");
			}

		} catch (Exception var9)
		{
			logger.error("init error, unknown error, {} {}", new Object[] { var9.getMessage(), var9 });
		}
	}

	public void destroyPool() {
		if (jedisType == 0)
		{
			pool.close();
			pool.destroy();
		}
	}

}
