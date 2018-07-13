package ourck.redis.client;

import java.io.Closeable;
import java.io.IOException;

import com.sun.istack.internal.Nullable;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisClient implements Closeable {
	private final String serverUrl;
	private final int serverPortOnListen;
	
	private JedisPool jedisPool;												//连接池
	private Jedis jedis;														//客户端对象
	
	public RedisClient(String serverUrl, @Nullable Integer port) {
		this.serverUrl = serverUrl;
		serverPortOnListen = port;
	}
	
	public RedisClient(String serverUrl) {
		this(serverUrl, 6379);
	}
	
	public void connect() {
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxTotal(20);
		config.setMaxIdle(5);
		config.setMaxWaitMillis(10001);
		config.setTestOnBorrow(false);
		
		jedisPool = new JedisPool(config, serverUrl, serverPortOnListen);
		jedis = jedisPool.getResource();
	}

	public void close() throws IOException {
		if(jedis != null) jedis.close();
	}
	
	/**
	 * 获取该客户端的jedis实体。用于通用客户端代码与业务代码之间的解耦。
	 * 还有更高明的方式吗？
	 * @return 该客户端持有的Jedis对象
	 */
	public Jedis getEntity() { return jedis; }
	
	public static void main(String[] args) {

	}


}
