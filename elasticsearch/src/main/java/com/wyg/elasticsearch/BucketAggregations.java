package com.wyg.elasticsearch;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.global.Global;

public class BucketAggregations extends App{
	public static void main(String[] args) {
		//testGlobal();
		testFilter();
	}
	public static void testGlobal(){
		AggregationBuilder aggregation = AggregationBuilders.global("agg").subAggregation(AggregationBuilders.terms("agg").field("balance").size(10)); 
		SearchResponse response = client.prepareSearch("bank").addAggregation(aggregation).setSearchType(SearchType.COUNT).execute().actionGet();
		System.out.println(response.toString());
		Global agg = response.getAggregations().get("agg");
		System.out.println(agg.getDocCount());
	}
	
	public static void testFilter(){
		AggregationBuilder aggregation = AggregationBuilders.filters("agg").filter("women",QueryBuilders.termQuery("gender", "F")).filter("men",QueryBuilders.termQuery("gender", "M"));
		SearchResponse response = client.prepareSearch("bank").addAggregation(aggregation).setSearchType(SearchType.COUNT).execute().actionGet();
		System.out.println(response.toString());
	}
	
	
}
