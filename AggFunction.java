package com.yeahmobi.datasystem.query.pretreatment;

/**
 * Created by ellis on 3/30/15.
 */
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;

import com.yeahmobi.datasystem.query.config.ConfigForAgg;
import com.yeahmobi.datasystem.query.meta.MetricAggTable;

public enum AggFunction{
	// 方差
	variance(1) {
		@Override
		public String convert(String datasource, MetricAggTable metricAggTable, String metricName, String metric) {
			HashMap<String, String[][]> complexMetrics = ConfigForAgg.getInstance().getDataSource().get(datasource);
			String[] metricNames = complexMetrics.get(metricName)[0];
			String[] expressions = complexMetrics.get(metricName)[2];
			StringBuilder sb = new StringBuilder();
			sb.append(String.format("<js(%s1:%s:[%s]:+:0)&", metricName, StringUtils.join(metricNames, ","), StringUtils.join(metricNames, ",") + "," + expressions[0]));
			sb.append(String.format("js(%s2:%s:[%s]:+:0)&", metricName, StringUtils.join(metricNames, ","), StringUtils.join(metricNames, ",") + "," + expressions[1]));
			sb.append("count(*)");
			sb.append(" to pjs(" + metric + String.format(":%s1,%s2,rows:[\"%s1/rows-(%s2*%s2)/(rows*rows)\"])", metricName, metricName, metricName, metricName, metricName));
			sb.append(" as " + metric + ">");
			return sb.toString();
		}

	},
	
	// 绝对值函数
	abs(2) {
		@Override
		public String convert(String datasource, MetricAggTable metricAggTable, String metricName, String metric) {
			return getDataExp(datasource, metricAggTable, metricName, metric, abs.toString());
		}
	},
	
	// 四舍五入的函数
	round(2) {
		@Override
		public String convert(String datasource, MetricAggTable metricAggTable, String metricName, String metric) {
			return getDataExp(datasource, metricAggTable, metricName, metric, round.toString());
		}
	},
	
	// 向下取整的函数
	floor(2) {
		@Override
		public String convert(String datasource, MetricAggTable metricAggTable, String metricName, String metric) {
			return getDataExp(datasource, metricAggTable, metricName, metric, floor.toString());
		}
	},
	
	// 向上取整的函数
	ceil(2) {
		@Override
		public String convert(String datasource, MetricAggTable metricAggTable, String metricName, String metric) {
			return getDataExp(datasource, metricAggTable, metricName, metric, ceil.toString());
		}
	},
	
	// 自然常数e为底的指数函数
	exp(2) {
		@Override
		public String convert(String datasource, MetricAggTable metricAggTable, String metricName, String metric) {
			return getDataExp(datasource, metricAggTable, metricName, metric, exp.toString());
		}
	},
	
	// 一个数的自然对数
	ln(2) {
		@Override
		public String convert(String datasource, MetricAggTable metricAggTable, String metricName, String metric) {
			return getDataExp(datasource, metricAggTable, metricName, metric, "log");
		}
	},
	
	// 以10为底的对数
	log10(2) {
		@Override
		public String convert(String datasource, MetricAggTable metricAggTable, String metricName, String metric) {
			return getDataExpForLog(datasource, metricAggTable, metricName, metric, 10);
		}
	},
	
	// 以2为底的对数
	log2(2) {
		@Override
		public String convert(String datasource, MetricAggTable metricAggTable, String metricName, String metric) {
			return getDataExpForLog(datasource, metricAggTable, metricName, metric, 2);
		}
	},
	
	sqrt(2) {
		@Override
		public String convert(String datasource, MetricAggTable metricAggTable, String metricName, String metric) {
			return getDataExp(datasource, metricAggTable, metricName, metric, sqrt.toString());
		}
	},
	
	sin(2) {
		@Override
		public String convert(String datasource, MetricAggTable metricAggTable, String metricName, String metric) {
			return getDataExp(datasource, metricAggTable, metricName, metric, sin.toString());
		}
	},
	
	cos(2) {
		@Override
		public String convert(String datasource, MetricAggTable metricAggTable, String metricName, String metric) {
			return getDataExp(datasource, metricAggTable, metricName, metric, cos.toString());
		}
	},
	
	acos(2) {
		@Override
		public String convert(String datasource, MetricAggTable metricAggTable, String metricName, String metric) {
			return getDataExp(datasource, metricAggTable, metricName, metric, acos.toString());
		}
	},
	
	asin(2) {
		@Override
		public String convert(String datasource, MetricAggTable metricAggTable, String metricName, String metric) {
			return getDataExp(datasource, metricAggTable, metricName, metric, asin.toString());
		}
	};
	
	    public final int duration;

    private AggFunction(int duration) {
        this.duration = duration;
    }
    
	abstract public String convert(String datasource, MetricAggTable metricAggTable, String metricName, String metric);
	
	private static String getDataExp(String datasource, MetricAggTable metricAggTable, String metricName, String metric, String metricFunction){
		HashMap<String, String[][]> complexMetrics = ConfigForAgg.getInstance().getDataSource().get(datasource);
		int level = metricAggTable.getAggLevel(metricName);
		StringBuilder sb = new StringBuilder();
		if(level == 1){
			String aggExpr = metricAggTable.getAggExpr(complexMetrics.get(metricName)[1][0]);
			int end = metricAggTable.getAggExpr(complexMetrics.get(metricName)[1][0]).indexOf("as");
			sb.append("<");
			sb.append(aggExpr.substring(1, end-1));
			sb.append(" to pjs(" + metric + ":" + metricAggTable.getL1Alias("sum(" + metricName + ")") + ":[\"" + "Math." + metricFunction + "(" + complexMetrics.get(metricName)[1][0] + ")\"])");
			sb.append(" as " + metric + ">");
		}else if(level == 2){
			String[] aggExprs = new String[complexMetrics.get(metricName)[1].length];
			String[] metricNames = new String[complexMetrics.get(metricName)[1].length];
			for(int i = 0; i < complexMetrics.get(metricName)[1].length ; i++){
				String aggExpr = metricAggTable.getAggExpr(complexMetrics.get(metricName)[1][i]);
				int end = metricAggTable.getAggExpr(complexMetrics.get(metricName)[1][i]).indexOf("as");
				aggExprs[i] = aggExpr.substring(1, end-1);
				metricNames[i] = metricAggTable.getL1Alias("sum(" + complexMetrics.get(metricName)[1][i] + ")");
			}
			sb.append("<");
			sb.append(StringUtils.join(aggExprs , "&"));
			sb.append(" to pjs(" + metric + ":" + StringUtils.join(metricNames , ",") + ":[\"" + "Math." + metricFunction + metricAggTable.getAggName(metricName) + "\"])");
			sb.append(" as " + metric + ">");
		}
		return sb.toString();
	}
	
	private static String getDataExpForLog(String datasource, MetricAggTable metricAggTable, String metricName, String metric, int number){
		HashMap<String, String[][]> complexMetrics = ConfigForAgg.getInstance().getDataSource().get(datasource);
		int level = metricAggTable.getAggLevel(metricName);
		StringBuilder sb = new StringBuilder();
		if(level == 1){
			String aggExpr = metricAggTable.getAggExpr(complexMetrics.get(metricName)[1][0]);
			int end = metricAggTable.getAggExpr(complexMetrics.get(metricName)[1][0]).indexOf("as");
			sb.append("<");
			sb.append(aggExpr.substring(1, end-1));
			sb.append(" to pjs(" + metric + ":" + metricAggTable.getL1Alias("sum(" + metricName + ")") + ":[\"" + "Math.log" + "(" + complexMetrics.get(metricName)[1][0] + ")/Math.log(" + number + ")\"])");
			sb.append(" as " + metric + ">");
		}else if(level == 2){
			String[] aggExprs = new String[complexMetrics.get(metricName)[1].length];
			String[] metricNames = new String[complexMetrics.get(metricName)[1].length];
			for(int i = 0; i < complexMetrics.get(metricName)[1].length ; i++){
				String aggExpr = metricAggTable.getAggExpr(complexMetrics.get(metricName)[1][i]);
				int end = metricAggTable.getAggExpr(complexMetrics.get(metricName)[1][i]).indexOf("as");
				aggExprs[i] = aggExpr.substring(1, end-1);
				metricNames[i] = metricAggTable.getL1Alias("sum(" + complexMetrics.get(metricName)[1][i] + ")");
			}
			sb.append("<");
			sb.append(StringUtils.join(aggExprs , "&"));
			sb.append(" to pjs(" + metric + ":" + StringUtils.join(metricNames , ",") + ":[\"" + "Math.log" + metricAggTable.getAggName(metricName) + "/Math.log(" + number + ")\"])");
			sb.append(" as " + metric + ">");
		}
		return sb.toString();
	}
}
