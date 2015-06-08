package com.yeahmobi.datasystem.query.pretreatment;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.yeahmobi.datasystem.query.config.ConfigForAgg;
import com.yeahmobi.datasystem.query.extensions.DataSourceView;
import com.yeahmobi.datasystem.query.meta.MetricAggTable;
import com.yeahmobi.datasystem.query.reportrequest.ReportParam;
import com.yeahmobi.datasystem.query.reportrequest.ReportParamFactory;
import com.yeahmobi.datasystem.query.skeleton.DataSourceViews;

public class Pretreatment {

    /**
     * 1.公式替换
     * 2.给filters重新赋值,防止出现转移字符，
     * 3.替换data里面公式里面的双引号，否则g4解析不了
     * 4.反转义
     * @return
     */
    public static String replaceFormula(String datasource, ReportParam reportParam) {
        String result="";
        MetricAggTable metricAggTable = DataSourceViews.getViews().get(datasource).metrics().getMetricAggTable();
        List<String> metrics = reportParam.getData();
        List<String> newMetrics = new ArrayList<String>();
        if (null != metrics) {
            for (String metric : metrics) {
            	if(metric.endsWith(")")){
            		int splitNum = metric.indexOf("(");
            		String mathFunction = metric.substring(0 , splitNum);
            		String metricName = metric.substring(splitNum+1 , metric.indexOf(")"));
            		String newMetricstr = "";
        			newMetricstr = AggFunction.valueOf(mathFunction).convert(datasource, metricAggTable, metricName, metric);
            		newMetrics.add(newMetricstr);
            	} else{
            		newMetrics.add(metricAggTable.getAggExpr(metric));
            	}
            }
            reportParam.setData(newMetrics);
        }
        
        result = ReportParamFactory.toString(reportParam);
        return result;
    }

    /**
     * 创建责任链
     * 
     * @param reportContext
     */
    public static void createChainAndExecute(ReportContext reportContext) {
        PretreatmentHandler timeHandler = new TimeDimentionHandler();
        PretreatmentHandler generalHandler = new GeneralQueryHandler();
        // 2. get the extra handler for the specific data source
        PretreatmentHandler pluginHandler = null;
        DataSourceView dsView = DataSourceViews.getViews().get(reportContext.getReportParam().getSettings().getData_source());
        if (dsView != null) {
            pluginHandler = dsView.getPreExtraHandler(reportContext);
        } else {
            pluginHandler = new GeneralQueryHandler();
        }
        timeHandler.setNextHandler(generalHandler);
        generalHandler.setNextHandler(pluginHandler);

        // 进行预处理
        timeHandler.handleRequest(reportContext);

    }
}
