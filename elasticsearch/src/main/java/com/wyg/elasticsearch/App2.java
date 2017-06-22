package com.wyg.elasticsearch;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;

/**
 * Hello world!
 *
 */

public class App2 {
	public static Client client;
	static {
		try {
			client = ESClient.getESClient();
		} catch (Exception e) {
			throw new ExceptionInInitializerError(e);
		}

	}

	public static void main(String[] args) throws IOException {

		try {
			// ESClient.createIndex(client, "abc", 1, 0);
			// ESClient.putMapping(client, Music.class, "abc", "music");
			// List<Music> musics = put();
			// ESClient.indexList(client, musics, "abc", "music");
			// ESClient.deleteIndex(client,
			// "abc","music","AVra_mo-HI5SLyi3ByV9");
//			ESClient.createIndex(client, "fileinfo", 6, 1);
//			ESClient.putMapping(client, FileInfo.class, "fileinfo", "file");
//			List<FileInfo> fileInfo = getFileInfos("/Users/wuyinggui/dev");
//			ESClient.indexList(client, fileInfo, "fileinfo", "file");
//			List<FileInfo> fileInfo = getFileInfos("/Users/wuyinggui/dev/hbase");
//			ESClient.indexList(client, fileInfo, "fileinfo", "file");
			//ESClient.createIndex(client, "fileinfos", 1, 0);
			//ESClient.putMapping(client, FileInfo.class, "fileinfos", "file");
//			for (int i = 0; i < 20; i++) {
//				new TaskThread().run();
//			}
			
			
			// for (int i = 0; i < 10000; i++) {
			//
			//ESClient.indexList(client, musics, "abc", "music");
			// }
			getList();
			getListWithAggregation();
//			getListWithRouting();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void getListWithRouting() {
		//TermsBuilder builder = AggregationBuilders.terms("name").field("name");
		long start = System.currentTimeMillis();
		SearchResponse response = client.prepareSearch("trace-*")//.setRouting(FileInfo.class.getSimpleName())//.addAggregation(builder)//
				.setQuery("{\"term\" : {\"channel\" : \"oppo\"}}")
				.get();
		long end = System.currentTimeMillis();
		System.out.println("cost" + (end-start) + " ms");
		System.out.println(response.toString());
	}

	public static List<FileInfo> getFileInfos(String path) {
		List<FileInfo> result = new ArrayList<FileInfo>();
		File file = new File(path);
		File[] files = file.listFiles();
		for (File subFile : files) {
			try {
				if (subFile.isFile()) {
					String name = subFile.getName();
					System.out.println(subFile.getAbsolutePath());
					if (name.endsWith("txt") || name.endsWith("html") || name.endsWith("sh") || name.endsWith("js")
							|| name.endsWith("jsp") || name.endsWith("css") || name.endsWith("log")
							|| name.endsWith("java") || name.endsWith("plist") || name.endsWith("json")
							|| name.endsWith("md") || name.endsWith("pl") || name.endsWith("php") || name.endsWith("py")
							|| name.endsWith("s") || name.endsWith("rb") || name.endsWith("sql")
							|| name.endsWith("svg")) {
						FileInfo fileInfo = new FileInfo();
						fileInfo.setName(subFile.getName());
						fileInfo.setPath(subFile.getAbsolutePath());
						fileInfo.setModifyTime(subFile.lastModified());
						result.add(fileInfo);
					}
				} else if (subFile.isDirectory()) {
					result.addAll(getFileInfos(subFile.getAbsolutePath()));
				}
			} catch (Exception e) {
				System.out.println(subFile.getAbsolutePath());
				// TODO Auto-generated catch block
				e.printStackTrace();
				continue;
			}
		}
		return result;
	}

	/**
	 * 添加索引
	 * 
	 * @throws IOException
	 * @throws IllegalAccessException
	 * @throws IllegalArgumentException
	 */
	public static List<Music> put() throws IOException, IllegalArgumentException, IllegalAccessException {
		// BulkRequestBuilder bulkRequestBuilder =
		// client.prepareBulk().setRefresh(true);
		// ESClient.putMapping(client, Music.class, "abc", "music");
		List<Music> musics = new ArrayList<Music>();
		File file = new File("/Users/wuyinggui/Music/iTunes/iTunes Media/Music");
		File[] files = file.listFiles();
		for (File subFolderFile : files) {
			String singerName = subFolderFile.getName();
			if (subFolderFile.isDirectory()) {
				File[] subFolderFiles = subFolderFile.listFiles();
				for (File file2 : subFolderFiles) {
					Music music = new Music();
					music.setSinger(singerName);
					music.setName(file2.getName());
					music.setPath(file2.getAbsolutePath());
					musics.add(music);
					// IndexRequestBuilder request = ESClient.index(client,
					// music, "abc", "music");
					// bulkRequestBuilder.add(request);
				}
			}

		}
		// BulkResponse response = bulkRequestBuilder.execute().actionGet();
		// System.out.println(response.hasFailures());
		return musics;
	}

	/**
	 * 按照索引，类型，id查询
	 */
	public static void get() {
		GetRequestBuilder builder = client.prepareGet("abc", "music", "AVrWNiqrGCZ1yp7lRlTr");
		GetResponse response = builder.setRealtime(true).get();
		System.out.println(response.getSource().toString());
		;
	}

	/**
	 * 查询列表
	 */
	public static void getList() {
		// QueryBuilder builder = QueryBuilders.termQuery("singer", "张");
		// System.out.println(builder.toString());
		// aggregation.put(key, value)
		long start = System.currentTimeMillis();
		SearchResponse searchResponse = client.prepareSearch("trace-*").setQuery("{\"term\" : {\"channel\" : \"oppo\"}}")
				.get();
		long end = System.currentTimeMillis();
		System.out.println("cost" + (end-start) + " ms");
		System.out.println(searchResponse.toString());
	}
	
	public static void getListWithAggregation(){
		TermsBuilder builder = AggregationBuilders.terms("traceError").field("channel");
		long start = System.currentTimeMillis();
		SearchResponse resposne = client.prepareSearch("trace-*").addAggregation(builder).setQuery("{\"term\" : {\"channel\" : \"oppo\"}}").get();
		long end = System.currentTimeMillis();
		System.out.println("cost" + (end-start) + " ms");
		System.out.println(resposne.toString());
	}
}
