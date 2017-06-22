package com.wyg.elasticsearch;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Order;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregator;
import org.elasticsearch.search.aggregations.metrics.avg.AvgBuilder;
import org.elasticsearch.search.aggregations.metrics.geobounds.GeoBounds;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentile;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentiles;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStats;

/**
 * Hello world!
 *
 */

public class App {
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
			ESClient.createIndex(client, "fileinfo_2017-05-02", 1, 0);
			ESClient.putMapping(client, FileInfo.class, "fileinfo_2017-05-02", "file");
//			testAvgAggregations();
//			testMetricsAggregtions();
//			geoBounds();
//			getTop5();
//			scroll();
//			List<FileInfo> fileInfo = getFileInfos("/Users/wuyinggui/dev");
//			ESClient.indexList(client, fileInfo, "fileinfo", "file");
//			List<FileInfo> fileInfo = getFileInfos("/Users/wuyinggui/dev/hbase");
//			ESClient.indexList(client, fileInfo, "fileinfo", "file");
			//ESClient.createIndex(client, "fileinfos", 1, 0);
			//ESClient.putMapping(client, FileInfo.class, "fileinfos", "file");
//			for (int i = 0; i < 200; i++) {
//				new TaskThread().run();
//			}
			new TaskThread().run();
			//System.out.println(ESClient.testIndexExist(client, "1b","fileinfo"));;
			// for (int i = 0; i < 10000; i++) {
			//
			//ESClient.indexList(client, musics, "abc", "music");
			// }
//			getList();
//			getListWithAggregation();
//			getListWithRouting();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void getListWithRouting() {
		//TermsBuilder builder = AggregationBuilders.terms("name").field("name");
		long start = System.currentTimeMillis();
		SearchResponse response = client.prepareSearch("fileinfos").setRouting(FileInfo.class.getSimpleName())//.addAggregation(builder)//
				.setQuery("{\"term\" : {\"name\" : \"integration.html\"}}")
				.get();
		long end = System.currentTimeMillis();
		System.out.println("cost" + (end-start) + " ms");
		System.out.println(response.getHits().getTotalHits());
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
		SearchResponse searchResponse = client.prepareSearch("fileinfos").setQuery("{\"term\" : {\"name\" : \"integration.html\"}}")
				.get();
		long end = System.currentTimeMillis();
		System.out.println("cost" + (end-start) + " ms");
		System.out.println(searchResponse.getHits().getTotalHits());
	}
	
	public static void getListWithAggregation(){
		
		TermsBuilder builder = AggregationBuilders.terms("name").field("name");
		long start = System.currentTimeMillis();
		SearchResponse resposne = client.prepareSearch("fileinfos").addAggregation(builder).setQuery("{\"term\" : {\"name\" : \"integration.html\"}}").get();
		long end = System.currentTimeMillis();
		System.out.println("cost" + (end-start) + " ms");
		System.out.println(resposne.getHits().getTotalHits());
	}
	
	public static void testRangeAggregations(){
		@SuppressWarnings("rawtypes")
		AggregationBuilder builder = AggregationBuilders.range("MemoryRange").field("memory").addRange(0, 1000).addRange(1000, 2000)
									.addRange(2000, 5000).addRange(5000, 10000).addRange(10000, 100000);
		SearchResponse response = client.prepareSearch("logstash-*").addAggregation(builder).setSearchType(SearchType.COUNT).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
		System.out.println(response.toString());
	}
	
	public static void testAvgAggregations(){
		AvgBuilder metrics = AggregationBuilders.avg("avg").field("balance");
		SearchResponse response = client.prepareSearch("bank").addAggregation(metrics).setSearchType(SearchType.COUNT).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
		System.out.println(response.toString());
	}
	
	public static void testMetricsAggregtions(){
		MetricsAggregationBuilder metrics = AggregationBuilders.max("agg").field("account_number");
		SearchResponse response = client.prepareSearch("bank").addAggregation(metrics).setSearchType(SearchType.COUNT).setQuery(QueryBuilders.andQuery(QueryBuilders.termQuery("balance", 39225))).execute().actionGet();
		System.out.println(response.toString());
		
		metrics = AggregationBuilders.min("agg").field("account_number");
		response = client.prepareSearch("bank").addAggregation(metrics).setSearchType(SearchType.COUNT).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
		System.out.println(response.toString());
		metrics = AggregationBuilders.avg("agg").field("account_number");
		response = client.prepareSearch("bank").addAggregation(metrics).setSearchType(SearchType.COUNT).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
		System.out.println(((Avg)response.getAggregations().get("agg")).getValue());
		System.out.println(response.toString());
		metrics = AggregationBuilders.stats("agg").field("balance");
		response = client.prepareSearch("bank").addAggregation(metrics).setSearchType(SearchType.COUNT).execute().actionGet();
		System.out.println(response.toString());
		Stats stats = response.getAggregations().get("agg");
		
		AbstractAggregationBuilder builder = AggregationBuilders.extendedStats("agg").field("balance");
		response = client.prepareSearch("bank").addAggregation(builder).setSearchType(SearchType.COUNT).execute().actionGet();
		ExtendedStats extendStats = response.getAggregations().get("agg");
		System.out.println(extendStats.getStdDeviation());
		
		builder = AggregationBuilders.percentiles("agg").field("balance");
		response = client.prepareSearch("bank").addAggregation(builder).setSearchType(SearchType.COUNT).execute().actionGet();
		System.out.println(response);
		Percentiles percentiles = response.getAggregations().get("agg");
		for (Percentile percentile : percentiles) {
			System.out.println(MessageFormat.format("{0} in {1}%", percentile.getValue(), percentile.getPercent()));
		}
		
		builder = AggregationBuilders.cardinality("agg").field("balance");
		response = client.prepareSearch("bank").addAggregation(builder).setSearchType(SearchType.COUNT).execute().actionGet();
		System.out.println(response);
	}
	
	public static void geoBounds(){
		AbstractAggregationBuilder builder = AggregationBuilders.geoBounds("agg").field("geo.coordinates").wrapLongitude(true);
		SearchResponse response = client.prepareSearch("logstash-*").addAggregation(builder).setSearchType(SearchType.COUNT).execute().actionGet();
		System.out.println(response.toString());
		GeoBounds bounds = response.getAggregations().get("agg");
		System.out.println(MessageFormat.format("topLeft:[{0}],bottomRight:[{1}]", bounds.topLeft().toString(),bounds.bottomRight().toString()));
	}
	//降序
	public static void getTop5(){
		AggregationBuilder builder = AggregationBuilders.terms("agg").field("balance").order(Terms.Order.term(false));
		AbstractAggregationBuilder subBuilder = AggregationBuilders.topHits("top").setSize(5);
		builder.subAggregation(subBuilder);
		SearchResponse response = client.prepareSearch("bank").addAggregation(builder).setSearchType(SearchType.COUNT).execute().actionGet();
		System.out.println(response);
	}
	
	public static void scroll(){
		SearchResponse response = client.prepareSearch("bank").setScroll(new TimeValue(10)).setQuery(QueryBuilders.matchAllQuery()).setSize(100).execute().actionGet();
		while (true) {
		    //Break condition: No hits are returned
		    if (response.getHits().getHits().length == 0) {
		        break;
		    }
		    for (SearchHit hit : response.getHits().getHits()) {
		        System.out.println(hit.getSourceAsString());
		    }
		    response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(10)).execute().actionGet();
		    //Break condition: No hits are returned
		    if (response.getHits().getHits().length == 0) {
		        break;
		    }
		}
	}
}
