package com.yeahmobi.datasystem.query.config;

import java.util.HashMap;

import com.fasterxml.jackson.core.type.TypeReference;
import com.yeahmobi.datasystem.query.serializer.ObjectSerializer;

/**
 * Created by ellis on 3/12/15.
 * <p>
 * 配置高级聚合函数对应的公式（如方差怎么在druid实现）
 */
public class ConfigForAgg {
	
	private HashMap<String , HashMap<String, String[][]>> dataSource;

	public HashMap<String, HashMap<String, String[][]>> getDataSource() {
		return dataSource;
	}

	public void setDataSource(HashMap<String, HashMap<String, String[][]>> dataSource) {
		this.dataSource = dataSource;
	}

	private static final String JSON_FILE = "configForAgg.json";
	private static ConfigForAgg cfg = null;

	static {
		cfg = ObjectSerializer.read(JSON_FILE, new TypeReference<ConfigForAgg>() {}, ConfigForAgg.class.getClassLoader());
	}

	public static ConfigForAgg getInstance() {
		return cfg;
	}
	
	public static void main(String[] args) {
		System.out.println(ConfigForAgg.getInstance().getDataSource().get("ymds_druid_datasource").get("click")[0][0]);
	}
}
