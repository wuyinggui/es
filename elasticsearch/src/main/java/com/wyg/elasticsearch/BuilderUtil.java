package com.wyg.elasticsearch;

import java.io.IOException;
import java.lang.reflect.Field;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

public class BuilderUtil<T> {
	public XContentBuilder getStructBuilder(@SuppressWarnings("rawtypes") Class clazz) throws IOException, IllegalArgumentException, IllegalAccessException{
		Field[] fields = clazz.getDeclaredFields();
		if(fields != null && fields.length > 0){
			XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("file").startObject("_ttl")// 有了这个设置,就等于在这个给索引的记录增加了失效时间,
					// ttl的使用地方如在分布式下,web系统用户登录状态的维护.
					.field("enabled", true)// 默认的false的
					.field("default", "10s").endObject();// 默认的失效时间,d/h/m/s 即天/小时/分钟/秒;
			mapping.startObject("properties");
			for (Field field : fields) {
				field.setAccessible(true);
				String fieldName = field.getName();
				String fieldType = field.getType().getSimpleName().toLowerCase();
				mapping.startObject(fieldName).field("type", fieldType).field("index","not_analyzed").endObject();
			}
			//mapping.startObject("_ttl").field("enabled",true).field("default", "10s").endObject();
			mapping.endObject().endObject().endObject();
			return mapping;
		}
		return null;
	} 
	public XContentBuilder getContentBuilder(T t){
		XContentBuilder result = null;
		Field[] fields = t.getClass().getDeclaredFields();
		if(fields != null && fields.length > 0){
			try {
				result = XContentFactory.jsonBuilder().startObject();
				for (Field field : fields) {
					field.setAccessible(true);
					result.field(field.getName(),field.get(t));
				}
				result.endObject();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				result = null;
			}
			return result;
		}
		return null;
	} 
}
