package ourck.listing;

import java.io.*;
import java.util.*;

import ourck.redis.client.RedisClient;
import static ourck.utils.ScreenReader.jin;

import redis.clients.jedis.Jedis;

public class CategoryRecommander {
	// 三者映射关系：goods <----> category <----> class
	RedisClient client = new RedisClient("127.0.0.1");
	{
		client.connect(); // Initializing
	}
	/**
	 * 商品ID——商品标题&信息映射表
	 */
	private Map<Long, String> categoryId_Info = new HashMap<Long, String>(); 
	
	/**
	 * 样本ID——商品ID映射表（因为文件是无序的）
	 */
	private Map<Long, Long> classId_Category = new HashMap<Long, Long>();
	
	/**
	 * 从Redis服务器读取信息并加载到类的两张表（单个商品——商品标题&信息映射表，样本ID——聚类ID映射表）中
	 * @param listFile 商品总表
	 * @param clusterFile 聚类得到的文件
	 */
	public void load(String listFile, String clusterFile) {
		BufferedReader reader = null;
		// 1. 加载商品数据
		try {
			reader = new BufferedReader(
						new FileReader(listFile));
			
			// 字符串操作，将个各个条目分割开
			String line = reader.readLine(); // 跳过header
			long sampleId = 1l;									// TODO 初始样本ID，从1开始逐个循环
			while((line = reader.readLine()) != null) {
				String[] tokens = line.split("\t");				// TODO 商品列表文件的各个字段
				if(tokens.length < 4) continue;
				Long listId = Long.parseLong(tokens[0]);		// TODO 商品ID
				String title = tokens[1];
				String category = tokens[3];
				
				// 建立两张表
				categoryId_Info.put(listId, String.format("Title: %s \nCategory: %s"
						, title, category + " - " + tokens[2]));
				classId_Category.put(sampleId, listId);
				sampleId++;
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if(reader != null)
				try {
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
		}
		
		// 2.加载聚类数据
		try {
			client.getEntity().flushDB();
			reader = new BufferedReader(
					new FileReader(clusterFile));
			String line = reader.readLine(); // 跳过header
			Long sampleId = 1l;
			while((line = reader.readLine()) != null) {
				String[] tokens = line.split("\t");
				
				String clusterId = String.format("Cluster_%s", tokens[1]);		// 聚类ID
				Long listId = classId_Category.get(sampleId);					// 根据特定样本ID获取对应商品ID
				
				Jedis jedis = client.getEntity();
				jedis.rpush(listId.toString(), clusterId);						// “对特定商品ID，等会给他推荐属于聚类ID的东西”
				jedis.rpush(clusterId, listId.toString());						// “对特定聚类ID，等会给他推荐属于商品ID的东西”
				
				sampleId++;
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public List<String> recommand(Long listId){
		Jedis jedis = client.getEntity();
		String clusterId = jedis.lrange(listId.toString(), 0, 1).get(0);
		
		long len = jedis.llen(clusterId);										// "这个聚类ID下有多少东西可以推荐？“
		long start = new Random(47).nextInt((int)(len - 10)); // 为啥不用nextInt？因为nextInt没有带参数的重载
		return jedis.lrange(clusterId, start, len);								// "对于这个ID的聚类，推荐从start到start + len个物品。”
	}
	
	public String getDetailsByID(long id) {
		return categoryId_Info.get(id);
	}
	
	public static void main(String[] args) {
		CategoryRecommander recommander = new CategoryRecommander(); // 耗时操作！
		String listPath = "/media/ourck/Local/Documents/Workspace/BigData/rawdata/test-shuffled";
		String clusterPath = "/media/ourck/Local/Documents/Workspace/BigData/rawdata/list-clustered";
		recommander.load(listPath, clusterPath); // 耗时操作！
		
		while(true) {
			System.out.print("输入你感兴趣的商品ID：");
			long id = Long.parseLong(jin());
			System.out.println("----------------------------");

			System.out.println(recommander.getDetailsByID(id) + "\n");
			List<String> recomList = recommander.recommand(id);
			List<String> recomListByName = new LinkedList<String>(); // 便于插入
			
			for(int i = 0; recomList.size() >= 3 && i < 3; i++) {
				long everyId = Long.parseLong(recomList.get(i));
				recomListByName.add(everyId + " - " + recommander.getDetailsByID(everyId));
			}
			System.out.println("推荐以下商品：");
			System.out.println("----------------------------");
			for(String detail : recomListByName) 
				System.out.println(detail + "\n");
		}
	}

}
