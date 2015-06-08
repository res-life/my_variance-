grammar DruidReport;

@header {
import com.yeahmobi.datasystem.query.pretreatment.ReportContext;
import org.apache.commons.lang.StringEscapeUtils;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.base.Predicate;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.collect.Collections2;
import io.druid.granularity.PeriodGranularity;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.MaxAggregatorFactory;
import io.druid.query.aggregation.MinAggregatorFactory;
import io.druid.query.aggregation.JavaScriptAggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.post.ArithmeticPostAggregator;
import io.druid.query.aggregation.post.ConstantPostAggregator;
import io.druid.query.aggregation.post.FieldAccessPostAggregator;
import io.druid.query.aggregation.post.JavaScriptPostAggregator;
import io.druid.query.dimension.ExtractionDimensionSpec;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.TypedDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.extraction.JavascriptDimExtractionFn;
import io.druid.query.groupby.having.HavingSpec;
import io.druid.query.groupby.having.NotHavingSpec;
import io.druid.query.groupby.having.GreaterThanHavingSpec;
import io.druid.query.groupby.having.LessThanHavingSpec;
import io.druid.query.groupby.having.EqualToHavingSpec;
import io.druid.query.groupby.having.AndHavingSpec;
import io.druid.query.groupby.having.OrHavingSpec;
import io.druid.query.groupby.orderby.LimitSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.groupby.orderby.DefaultLimitSpec;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.NotDimFilter;
import io.druid.query.filter.OrDimFilter;
import io.druid.query.filter.RegexDimFilter;
import io.druid.query.filter.JavaScriptDimFilter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.topn.TopNMetricSpec;
import io.druid.query.topn.NumericTopNMetricSpec;
import org.antlr.v4.runtime.NoViableAltException;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.atn.ATN;
import org.antlr.v4.runtime.atn.ATNSimulator;
import org.antlr.v4.runtime.atn.ParserATNSimulator;
import org.antlr.v4.runtime.atn.PredictionContextCache;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.tree.ParseTreeListener;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;

import java.text.NumberFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;

import com.yeahmobi.datasystem.query.meta.IntervalTable;
import com.yeahmobi.datasystem.query.meta.IntervalUnit;
import com.yeahmobi.datasystem.query.meta.DimensionTable;
import com.yeahmobi.datasystem.query.meta.MetricAggTable;
import com.yeahmobi.datasystem.query.meta.TimeFilter;
import com.yeahmobi.datasystem.query.meta.TokenType;
import com.yeahmobi.datasystem.query.meta.ValueType;
import com.yeahmobi.datasystem.query.skeleton.DataSourceViews;
import com.yeahmobi.datasystem.query.meta.GlobalCfg;

}

@parser::members {

    private static Logger logger = Logger.getLogger("DruidReport.g4");

    // aggregation names given in query [data]
    public List<String> fields = new LinkedList<String>();
    // use for impala sql ,use for record fields who use high agg
    public Set<String> fieldsForSqlRemove = new HashSet<String>();
    // Aggregations defined in query
    public Map<String, AggregatorFactory> aggregators = new LinkedHashMap<String, AggregatorFactory>();
    // PostAggregations defined in query
    public List<PostAggregator> postAggregators = new LinkedList<PostAggregator>();
    // PostAggregations created while parsing query
    private Map<String, PostAggregator> postAggregatorMap = new HashMap<String, PostAggregator>();

    // dimensions selected in query [category]
    public Map<String, DimensionSpec> groupByDimensions = new LinkedHashMap<String, DimensionSpec>();
    // intervals selected in query [category]
    public List<String> intervalUnits = new ArrayList<String>();
    // time granularity: defalut to ALL
    public QueryGranularity granularity = QueryGranularity.ALL;

    // combined dimesion filter created with given filter criterias [filters]
    public DimFilter filter;
    // combined metric filter created with given filter criterias [filters]
    public HavingSpec having;
    // use for trans for impala sql 
    public Map<String, ValueType> types = new HashMap<String, ValueType>();
    
    public DimensionSpec dimension;
    
    public TopNMetricSpec metric;
    public int threshold = -1;
    
    public boolean isDoTimeDb = false;
    public Period timePeriod;

    // possible values are: null, lp; if it's null, use origin logic
    // if it's "lp", indicates need do landing page special process
    // will first save druid query result to DB, and then use SQL do some special join process.
    public String processType = null;
    
    // limit spec [pagination]
    public LimitSpec orderBy;
    // ordering specs [order]
    public List<OrderByColumnSpec> columns = new ArrayList<OrderByColumnSpec>();
    public List<OrderByColumnSpec> timeColumns = new ArrayList<OrderByColumnSpec>();

    // query interval [start, end)
    public List<org.joda.time.Interval> intervals;
    // timezone offset to UTC in minutes
    public int tzMinOffset = 0;
    public double timeZoneDouble= 0.0;
    // timezone
    public DateTimeZone timeZone = DateTimeZone.UTC;
    private DateTime endDate = new DateTime(DateTimeZone.UTC);
    private DateTime startDate = endDate.withTimeAtStartOfDay();

    public String currency;
    public int page = 0;
    public int size = 50;
    public int offset = -1;
    public int maxRows = -1;

    private String alisa;

	public Map<String,TimeFilter> timeFilters = new HashMap<String,TimeFilter>();
    public static final String[] timeTypes = {"year","month","week","day","hour"};
    // use for extraction dimension
    // private static final String functionStr = "function(str) {var old_time = new Date(str.replace(/-/, '/').replace(/-/, '/'));var d = new Date(old_time.getTime()+(3600000*%s)-(old_time.getTimezoneOffset()*60000));return d.getUTCFullYear() + '-' + ((d.getUTCMonth()+1)>9?(d.getUTCMonth()+1).toString():'0'+(d.getUTCMonth()+1)) + '-' + (d.getUTCDate()>9?d.getUTCDate().toString():'0' + d.getUTCDate()) + ' ' + (d.getUTCHours()>9?d.getUTCHours().toString():'0' + d.getUTCHours()) + ':' + (d.getUTCMinutes()>9?d.getUTCMinutes().toString():'0' + d.getUTCMinutes()) + ':' + (d.getUTCSeconds()>9?d.getUTCSeconds().toString():'0' + d.getUTCSeconds());}";
    private static final String functionStr = "function(str) {var sstr = str.substr(0,19).replace(/-/, '/').replace(/-/, '/').replace(/T/,' ');var r = sstr.match(/^(\\d{1,4})\\/(\\d{1,2})\\/(\\d{1,2}) (\\d{1,2}):(\\d{1,2}):(\\d{1,2})$/);if(r==null){return str;}else{var old_time = new Date(sstr);var d = new Date(old_time.getTime()+(3600000*%s)-(old_time.getTimezoneOffset()*60000));return d.getUTCFullYear() + '-' + ((d.getUTCMonth()+1)>9?(d.getUTCMonth()+1).toString():'0'+(d.getUTCMonth()+1)) + '-' + (d.getUTCDate()>9?d.getUTCDate().toString():'0' + d.getUTCDate()) + ' ' + (d.getUTCHours()>9?d.getUTCHours().toString():'0' + d.getUTCHours()) + ':' + (d.getUTCMinutes()>9?d.getUTCMinutes().toString():'0' + d.getUTCMinutes()) + ':' + (d.getUTCSeconds()>9?d.getUTCSeconds().toString():'0' + d.getUTCSeconds());}}";
    // Set<String> extractionDimensions = Sets.newHashSet("click_time","conversion_time");

    public static final int DESC = -1;
    public static final int ASC = 1;

    // table used to determine dimension value type
    private DimensionTable dimTable = null;
    // table used to determine alias name(@client view) of metrics
    private MetricAggTable metricTable = null;
    // data source of current query
    String dataSourceName = null;

    String routedDataSourceName = null;
    
    String returnFormat = null;
    
    public String getDataSource() {
        return dataSourceName;
    }

    public void setDataSource(String ds) {
        this.dataSourceName = ds;
    }

    public void setRoutedDataSource(String ds) {
        this.routedDataSourceName = ds;
    }

    public String getRoutedDataSource() {
        return routedDataSourceName;
    }

    private void setMaxRows() {
		boolean isSync = ReportContext.isSyncProcess(this.returnFormat, this.processType);
		if(isSync){
			int tmpSize = GlobalCfg.getInstance().getMaxPageNumber() * this.size;
			int globalMaxRow = GlobalCfg.getInstance().getMaxSyncDruidResultRow();
			maxRows = tmpSize > globalMaxRow ? globalMaxRow : tmpSize;
		}else{
		    maxRows = GlobalCfg.getInstance().getMaxAsyncDruidResultRow();
		}
    }

    public String unescape(String quoted) {
        String unquote = quoted.trim().replaceFirst("^'(.*)'\$", "\$1");
        return unquote.replace("''", "'");
    }

    public String trim(String quoted) {
        return StringUtils.remove(quoted, '"');
    }
    
    public String trimForMatch(String quoted) {
        return quoted.substring(1,quoted.length()-1);
    }
    
    /**
     * Create a AggregatorFactory with given field name and agg type fn
     * @param name: related field name
     * @param fn: agg type {sum, longsum, doublesum, min, max, count}
     * @throws IllegalArgumentException if fn paramter is unknown
     * @return a <code>AggregatorFactory<code> representing desired aggregation
     */
    AggregatorFactory evalAgg(String name, int fn) {
        String alias = "sum(" + name + ")";
        if (null == metricTable) {
            metricTable = (MetricAggTable)DataSourceViews.getViews().get(dataSourceName).metrics().getMetricAggTable();
        }
        if (null != metricTable) {
            alias = metricTable.getL1Alias(alias);
            if (null == alias) {
                alias = "sum(" + name + ")";
            }
        }
        switch (fn) {
            case SUM:
            case LONGSUM: return new LongSumAggregatorFactory(alias, name);
            case DOUBLESUM: return new DoubleSumAggregatorFactory(alias, name);
            case MIN: return new MinAggregatorFactory(alias, name);
            case MAX: return new MaxAggregatorFactory(alias, name);
            case COUNT: return new CountAggregatorFactory(name);
        }
        throw new IllegalArgumentException("Unknown function [" + fn + "]");
    }

    /**
     * Create a combine string with given type fn
     * @param fn: agg type {sum, longsum, doublesum, min, max, count}
     * @throws IllegalArgumentException if fn paramter is unknown
     * @return a stringrepresenting desired aggregation
     */
    String evalFnCombine(int fn) {
        switch (fn) {
            case PLUS: return ("function(partialA, partialB)" + "{" + "return partialA + partialB;" + "}");
            case MINUS: return ("function(partialA, partialB)" + "{" + "return partialA - partialB;" + "}");
            case STAR: return ("function(partialA, partialB)" + "{" + "return partialA * partialB;" + "}");
            case DIV: return ("function(partialA, partialB)" + "{" + "return partialA / partialB;" + "}");
        }
        throw new IllegalArgumentException("Unknown combine function [" + fn + "]");
    }


    DimFilter evalDimFilter(Token op, Token dimension, Token value) {
        DimFilter filter = null;
        if (null != dimension && null != op && null != value) {
            String dim = trim(dimension.getText());
            String val = trim(value.getText());
            if(!ArrayUtils.contains(timeTypes,dim)){
	            switch (op.getType()) {
	                case(EQ): filter = new SelectorDimFilter(dim, val); break;
	                case(NEQ): filter = new NotDimFilter(new SelectorDimFilter(dim, val)); break;
	                case(JAVASCRIPT): filter = new JavaScriptDimFilter(dim, val); break;
	                case(MATCH):
	                    val = trimForMatch(value.getText());
	                	String decodeStr = StringEscapeUtils.unescapeJava(val); 
	                	filter = new RegexDimFilter(dim, decodeStr); break;
	            }
            }else{
            	switch (op.getType()) {
	                case(EQ): timeFilters.put(dim,new TimeFilter(IntervalUnit.valueOf(dim.toUpperCase()),TokenType.equals,val)); break;
	                case(NEQ): timeFilters.put(dim,new TimeFilter(IntervalUnit.valueOf(dim.toUpperCase()),TokenType.notEquals,val)); break;
	                case(JAVASCRIPT): break;
	                case(MATCH): break;
	            }
            }
        }
        return filter;
    }

    DimFilter evalDimFilter(Token op, Token dimension, List<Token> vals) {
        DimFilter filter = null;
        if (null != dimension && null != op
            && null != vals && !vals.isEmpty()) {
            List<DimFilter> filterList = new LinkedList<DimFilter>();
            String dim = trim(dimension.getText());
            switch (op.getType()) {
                case (NIN):
                    for(Token e : vals) {
                        filterList.add(new NotDimFilter(new SelectorDimFilter(dim, trim(e.getText()))));
                    }
                    filter = new AndDimFilter(filterList);
                    break;
                case (IN):
                    for(Token e : vals) {
                        filterList.add(new SelectorDimFilter(dim, trim(e.getText())));
                    }
                    filter = new OrDimFilter(filterList);
                    break;
            }
        }
        return filter;
    }

    PostAggregator evalArithmeticPostAggregator(PostAggregator a, List<Token> ops, List<PostAggregator> b) {
        if(b.isEmpty()) {
            return a;
        } else {
            int i = 0;

            PostAggregator root = a;
            while(i < ops.size()) {
                List<PostAggregator> list = new LinkedList<PostAggregator>();
                List<String> names = new LinkedList<String>();

                names.add(root.getName());
                list.add(root);

                Token op = ops.get(i);

                while(i < ops.size() && ops.get(i).getType() == op.getType()) {
                    PostAggregator e = b.get(i);
                    list.add(e);
                    names.add(e.getName());
                    i++;
                }

                String name = "("+Joiner.on(op.getText()).join(names)+")";

                root = new ArithmeticPostAggregator(name,
                    op.getText(), list);
            }

            return root;
        }
    }
    
    PostAggregator evalArithmeticPostAggregatorForJS(String name, List<String> pars, String function) {
    	String functionStr = String.format("function(%s) { return %s; }", StringUtils.join(pars, ","), trim(function));
  		PostAggregator root = new JavaScriptPostAggregator(name, pars, functionStr);
        return root;
    }

    private OrderByColumnSpec evalOrderBySpec(String key, int dir) {
        if (Strings.isNullOrEmpty(key)) return null;
        switch(dir) {
            case(DESC): return OrderByColumnSpec.desc(trim(key));
            case(ASC):
            default: return OrderByColumnSpec.asc(trim(key));
        }
    }

    private void organizingPostAggerators() {
        if (null == metricTable) {
            metricTable = (MetricAggTable)DataSourceViews.getViews().get(dataSourceName).metrics().getMetricAggTable();
        }

        if (null != metricTable) {
            for (String field : fields) {
                String l2Name = metricTable.getL2Name(field);
                if (!Strings.isNullOrEmpty(l2Name)) {
                	ArithmeticPostAggregator aggregator = null;
                	if(postAggregatorMap.get(l2Name) instanceof ArithmeticPostAggregator)
                    	aggregator = (ArithmeticPostAggregator)postAggregatorMap.get(l2Name);
                    if (null != aggregator) {
                        postAggregators.add(new ArithmeticPostAggregator(field, aggregator.getFnName(), aggregator.getFields()));
                    }
                }else if(field.indexOf("(") != -1){
                	l2Name = field;
                	JavaScriptPostAggregator aggregator2 = null;
                    if(postAggregatorMap.get(l2Name) instanceof JavaScriptPostAggregator)
                    	aggregator2 = (JavaScriptPostAggregator)postAggregatorMap.get(l2Name);
                    if (null != aggregator2) {
                        postAggregators.add(aggregator2);
                    }
                }
            }
        }
    }


}


AND: '"$and"';
OR: '"$or"';
SUM: 'sum';
LONGSUM: 'longsum';
DOUBLESUM: 'doublesum';
MIN: 'min';
MAX: 'max';
COUNT: 'count';
JS: 'js';
PJS: 'pjs';
AS: 'as';
TO: 'to';

BEGIN: '<';
END: '>';
OPEN: '(';
CLOSE: ')';
OBJOPEN: '{';
OBJCLOSE: '}';
ARRAYOPEN: '[';
ARRAYCLOSE: ']';
COMMA: ',';
PAIRDELIM: ':';
QUOTE: '"';

STAR: '*';
PLUS: '+';
MINUS: '-';
DIV: '/';
VERSUS: '&';

JAVASCRIPT: '"$js"';
MATCH: '"$match"';
NOT: '"$not"';
GT: '"$gt"';
GTE: '"$gte"';
LT: '"$lt"';
LTE: '"$lte"';
EQ: '"$eq"';
NEQ: '"$neq"';
IN: '"$in"';
NIN: '"$nin"';

NULL: 'null';
TRUE: 'true';
FALSE: 'false';

/*
QUOTED_IDENT : '"' (STRING) '"';
STRING : '"' (ESC | ~('"'|'\\'))* '"' ;
STRINGFORMATCH : '"' (ESC | ~('"'))* '"';
*/

STRING : '"' (ESC | ~('"'|'\\'))* '"' ;

fragment ESC : '\\' (["\\/bfnrt] | UNICODE) ;
fragment UNICODE : 'u' HEX HEX HEX HEX ;
fragment HEX : [0-9a-fA-F] ;

IDENT : (LETTER)(LETTER | DIGIT | '_')* ;

NUMBER: '-'?DIGIT*'.'?DIGIT+(EXPONENT)?;
EXPONENT: ('e') ('+'|'-')? ('0'..'9')+;
fragment DIGIT : '0'..'9';
fragment LETTER : 'a'..'z' | 'A'..'Z';

WS :  ' '+ -> skip;

query
    : OBJOPEN! query_comp (COMMA! query_comp)* OBJCLOSE! {
        /* finalize set QueryGranularity */
        if(!(this.intervalUnits == null || this.intervalUnits.isEmpty())){
        	if(IntervalUnit.isSingle(intervalUnits)){
        		timePeriod = IntervalUnit.singleIntervalUnit(intervalUnits);
        		if(intervalUnits.get(0).equals("year") || intervalUnits.get(0).equals("day")){
        			isDoTimeDb = false;
        		}else
        			isDoTimeDb = true;
        	}else if(IntervalUnit.isContinuous(intervalUnits)){
        		timePeriod = IntervalUnit.continuousIntervalUnit(intervalUnits);
        		if(!IntervalUnit.isSingle(timePeriod)){
        		   isDoTimeDb = true;
        		}
        		else{
        		   isDoTimeDb = false;
        		}
	  		}else{
	  			timePeriod = IntervalUnit.compoundIntervalUnit(intervalUnits);
	  			if(!IntervalUnit.isSingle(timePeriod)){
	  				isDoTimeDb = true;
	  			}
	  			else{
	  				isDoTimeDb = false;
	  			}
	  		}
        }
        
        IntervalUnit interval = IntervalUnit.min(this.intervalUnits );
        if (null != interval) {
            granularity = new PeriodGranularity(interval.granularityPeriod(), null, timeZone);
        }
        setMaxRows();
        orderBy = new DefaultLimitSpec(columns, maxRows);
        organizingPostAggerators();
    }
    ;

query_comp
    : setting_comp
    | group_comp
    | extract_comp
    | filter_comp
    | topn_comp
    | data_comp
    | sort_comp
    | currency_comp
    ;

setting_comp
    : '"settings"' ':' OBJOPEN! settingObj (COMMA! settingObj)* OBJCLOSE!
    ;

settingObj
    : '"report_id"' ':' STRING
    | '"return_format"' ':' returnFormatTmp=STRING {this.returnFormat = trim($returnFormatTmp.text);}
    | '"time"' ':' f=timeFilter {
        this.intervals = Lists.newArrayList(new org.joda.time.Interval(startDate, endDate));
    }
    | '"pagination"' ':' pageObj {
    }
    | '"type"' ':' '"rows"' | '"object"'
    | '"data_source"' ':' ds=dataSource {
        if (null == dimTable) dimTable = (DimensionTable)DataSourceViews.getViews().get(dataSourceName).dimentions().getDimensionTable();
        if (null == metricTable) metricTable = (MetricAggTable)DataSourceViews.getViews().get(dataSourceName).metrics().getMetricAggTable();
    }| '"process_type"' ':' processTypeTmp=STRING{
        this.processType = trim($processTypeTmp.text);
        this.processType =  StringUtils.deleteWhitespace(this.processType);
    }
    ;

dataSource
    : STRING
    ;

/**
 * {"start":11111111,"end":11111110,"timezone":-5.5}
 */
timeFilter
    : OBJOPEN! timeFilterComp (COMMA! timeFilterComp)* OBJCLOSE!
    | OBJOPEN! OBJCLOSE!
    ;

timeFilterComp
    : '"start"' ':' s=timestamp { startDate = $s.t;}
    | '"end"' ':' e=timestamp { endDate = $e.t; }
    | '"timezone"' ':' o=NUMBER {
        String str = null;
        if ($o != null) {
            try {
                str = $o.text.trim();
                timeZoneDouble = Double.parseDouble(str);
                tzMinOffset = (int)(NumberFormat.getInstance().parse(str).floatValue() * 60);
                timeZone = DateTimeZone.forOffsetMillis((int)TimeUnit.MINUTES.toMillis(tzMinOffset));
            } catch(ParseException e) {
                logger.error("Unable to parse number [" + str + "]", e);
            }
        }
    }
    ;

/**
 * {"size":50,"page":0}
 */
pageObj
    : OBJOPEN! pageObjComp (COMMA! pageObjComp)* OBJCLOSE!
    | OBJOPEN! OBJCLOSE!
    ;

pageObjComp
    : '"size"' ':' s=NUMBER {
        if (null != $s) {
            size = Integer.parseInt($s.text);
        }
    }
    | '"page"' ':' p=NUMBER {
        if (null != $p) {
            page = Integer.parseInt($p.text);
        }
    }
    | '"offset"' ':' p=NUMBER {
        if (null != $p) {
            offset = Integer.parseInt($p.text);
        }
    }
    ;

/**
 * "group":["a","b","hour"]
 */
group_comp
    : '"group"' ':' ARRAYOPEN! groupExpr ( COMMA! groupExpr )* ARRAYCLOSE!
    | '"group"' ':' ARRAYOPEN! ARRAYCLOSE!
    ;

groupExpr
    : dim=STRING {
    	Set<String> extractionDimensions = GlobalCfg.getInstance().getExtractionDimensions().get(dataSourceName);
        if (null != $dim && !Strings.isNullOrEmpty($dim.text)) {
            String dimension = trim($dim.text);
            this.dimension = new DefaultDimensionSpec(dimension, dimension);
            if (IntervalTable.contains(dimension)) {
                this.intervalUnits.add(dimension);
            } else if(extractionDimensions.contains(dimension)){
                this.groupByDimensions.put(dimension,
                    new ExtractionDimensionSpec(
						dimension,
						dimension,
	                    new JavascriptDimExtractionFn(String.format(functionStr, timeZoneDouble))
	                ));
            } else if(!groupByDimensions.keySet().contains(dimension)){
                if (null == dimTable) dimTable = (DimensionTable)DataSourceViews.getViews().get(dataSourceName).dimentions().getDimensionTable();
                this.groupByDimensions.put(dimension,
                    new TypedDimensionSpec(dimension, dimension,
                     dimTable.getValueType(dimension).toDimType()));
                    // new DefaultDimensionSpec(dimension, dimension));
            }
        }
    }
    ;
/**
   "extract":[{"extractDimension":"click_time","jsFunction":"function(str){return 'aa';}"}]
*/   
extract_comp
	:'"extract"' ':' ARRAYOPEN! d+=dimensionByExtract (COMMA! d+=dimensionByExtract)* ARRAYCLOSE! {
        for(DimensionByExtractContext ed : $d) {
            if (null != ed) {
                ExtractionDimensionSpec eds = ed.e;
                String dimension = ed.dd;
                if (null != eds) {
                	this.groupByDimensions.put(dimension, eds);
                }
            }
        }
    }
    | '"extract"' ':' ARRAYOPEN! ARRAYCLOSE!
    ;
   
dimensionByExtract returns [ExtractionDimensionSpec e, String dd]
    : OBJOPEN! key=extractByKey (COMMA! dir=extractByDir)* OBJCLOSE! {
    	if (Strings.isNullOrEmpty($key.key)) $e = null;
        $e = new ExtractionDimensionSpec(
						$key.key,
						$key.key,
	                    new JavascriptDimExtractionFn($dir.dir)
	                );
    	$dd = $key.key;	
    }
    | OBJOPEN! dir=extractByDir COMMA! key=extractByKey OBJCLOSE! {
    	if (Strings.isNullOrEmpty($key.key)) $e = null;
        $e = new ExtractionDimensionSpec(
						$key.key,
						$key.key,
	                    new JavascriptDimExtractionFn($dir.dir)
	                );
        $dd = $key.key;
    }
    ;

extractByKey returns [String key]
    : '"extractDimension"' ':' k=STRING {
        if (!Strings.isNullOrEmpty($k.text)) {
            $key = trim($k.text);
        }
    }
    ;

extractByDir returns [String dir]
    : '"jsFunction"' ':' d=STRING {
        if (!Strings.isNullOrEmpty($d.text)) {
            $dir = trim($d.text);
        }
    }
    ;
    
     
topn_comp
    : '"topn"' ':' '{' metrics '}'
    ;

metrics
    : '"metricvalue"' ':' m=STRING ',' '"threshold"' ':' n=NUMBER {
        metric = new NumericTopNMetricSpec(trim($m.text));
        threshold = Integer.parseInt($n.text);
    }
    ;

/**
 * "data":["(sum(a) as as)","(sum(a)-sum(b) as net)"]
 * &&
 * <js(cost2:clicks,clicks:[a,b,"Math.pow(a*b,2)"]:+:0) & js(cost2:clicks,clicks:[a,b,"[a,b,"(a*b)"]:+:0) & count(*) to pjs(variance(clicks):cost1,cost2,rows:[cost1,cost2,rows,cost1/rows-(cost2*cost2)/(rows*rows)]) as variance(clicks)>
 */
data_comp
    : '"data"' ':' ARRAYOPEN! e+=aliasedQuoteExpression ( COMMA! e+=aliasedQuoteExpression )* ARRAYCLOSE! {
        for(AliasedQuoteExpressionContext a : $e) {
            fields.add(a.p.getName());
            postAggregatorMap.put(a.p.getName(), a.p); 
        }
    }
    | '"data"' ':' ARRAYOPEN! ARRAYCLOSE!
    ;

aliasedQuoteExpression returns [PostAggregator p]
    :  e=aliasedExpression {$p = $e.p;}
    |  g=aliasedExpressionForJS {$p = $g.p;}
    ;
aliasedExpression returns [PostAggregator p]
    : OPEN! expression ( AS^ name=IDENT )? CLOSE! {
        if($name != null) {
            alisa = $name.text;
            postAggregatorMap.put($expression.p.getName(), $expression.p);
            $p = new FieldAccessPostAggregator(alisa, $expression.p.getName());
        } else {
            $p = $expression.p;
        }
    }
    ;
/* ( TO^ formula=jsFormula )? 
*/    
aliasedExpressionForJS returns [PostAggregator p]
: BEGIN! expressionForJS ( TO^ expressionForPostJS)? ( AS^ function=IDENT )? OPEN! name=IDENT CLOSE! END! {
    if($name != null) {
        alisa = $function.text + "(" + $name.text + ")";
        postAggregatorMap.put($expressionForPostJS.p.getName(), $expressionForPostJS.p);
        $p = $expressionForPostJS.p;
    } else {
        $p = $expressionForPostJS.p;
    }
}
;

expressionForPostJS returns [PostAggregator p]
	: PJS OPEN! jsAggregateForJS CLOSE!{$p = $jsAggregateForJS.p;}
	;
	
jsAggregateForJS returns [PostAggregator p]	
	: function=IDENT OPEN! name=IDENT CLOSE! PAIRDELIM! fnf=fieldNamesForJS PAIRDELIM! fna=fnAggregateForJS {
        $p = evalArithmeticPostAggregatorForJS($function.text + "(" + $name.text + ")", $fnf.l, $fna.a);
    }
    ;
    
fieldNamesForJS returns [List<String> l]
: f+=IDENT (COMMA! f+=IDENT)* {
    List<String> fields = new ArrayList<String>();
    for (Token t : $f) {
        fields.add(t.getText());
    }
    $l = fields;
}
;
    
fnAggregateForJS returns [String a]
    : ARRAYOPEN! STRING ARRAYCLOSE! {
        $a = $STRING.text;
    }
    ;    

expressionForJS
    : additiveExpressionForJS
    ;
    
additiveExpressionForJS
	: e+=primaryExpression (( ops+=VERSUS^ ) e+=primaryExpression)* { 
	  for(PrimaryExpressionContext a : $e) {
          fieldsForSqlRemove.add(a.p.getName());
      }
	}
	;
    
expression returns [PostAggregator p]
    : additiveExpression { $p = $additiveExpression.p; }
    ;

additiveExpression returns [PostAggregator p]
    : a=multiplyExpression (( ops+=PLUS^ | ops+=MINUS^ ) b+=multiplyExpression)* {
        List<PostAggregator> rhs = new LinkedList<PostAggregator>();
        for(MultiplyExpressionContext e : $b) rhs.add(e.p);
        $p = evalArithmeticPostAggregator($a.p, $ops, rhs);
    }
    ;

multiplyExpression returns [PostAggregator p]
    : a=unaryExpression ((ops+= STAR | ops+=DIV ) b+=unaryExpression)* {
        List<PostAggregator> rhs = new LinkedList<PostAggregator>();
        for(UnaryExpressionContext e : $b) rhs.add(e.p);
        $p = evalArithmeticPostAggregator($a.p, $ops, rhs);
    }
    ;

unaryExpression returns [PostAggregator p]
    : MINUS e=unaryExpression {
        if($e.p instanceof ConstantPostAggregator) {
            ConstantPostAggregator c = (ConstantPostAggregator)$e.p;
            double v = c.getConstantValue().doubleValue() * -1;
            $p = new ConstantPostAggregator(Double.toString(v), v, null);
        } else {
            $p = new ArithmeticPostAggregator(
                "-"+$e.p.getName(),
                "*",
                Lists.newArrayList($e.p, new ConstantPostAggregator("-1", -1.0, null))
            );
        }
    }
    | PLUS e=unaryExpression { $p = $e.p; }
    | primaryExpression { $p = $primaryExpression.p; }
    ;

primaryExpression returns [PostAggregator p]
    : constant { $p = $constant.c; }
    | aggregate {
        aggregators.put($aggregate.agg.getName(), $aggregate.agg);
        // TODO
        $p = new FieldAccessPostAggregator($aggregate.agg.getName(), $aggregate.agg.getName());
    }
    | OPEN! e=expression CLOSE! { $p = $e.p; }
    ;

aggregate returns [AggregatorFactory agg]
    : fn=( SUM^ | LONGSUM^ | DOUBLESUM^ | MIN^ | MAX^ ) OPEN! name=(IDENT|COUNT) CLOSE! {
        $agg = evalAgg($name.text, $fn.type);
    }
    | fn=COUNT OPEN! STAR CLOSE! { $agg = evalAgg("rows", $fn.type); }
    | fn=JS OPEN! f=jsAggregate CLOSE! {
        $agg = $f.a;
    }
    ;

jsAggregate returns [AggregatorFactory a]
    : name=IDENT PAIRDELIM! fnf=fieldNames PAIRDELIM! fna=fnAggregate PAIRDELIM! fnc=fnCombine PAIRDELIM! fnr=fnReset {
        $a = new JavaScriptAggregatorFactory($name.text, $fnf.l, $fna.a, $fnr.r, $fnc.c);
    }
    ;

fieldNames returns [List<String> l]
    : f+=IDENT (COMMA! f+=IDENT)? {
        List<String> fields = new ArrayList<String>();
        for (Token t : $f) {
            fields.add(t.getText());
        }
        $l = fields;
    }
    ;

fnAggregate returns [String a]
    : ARRAYOPEN! l+=IDENT ( COMMA! l+=IDENT )? COMMA! STRING ARRAYCLOSE! {
        StringBuilder builder = new StringBuilder();
        builder.append("function(current");
        for (Token t : $l) {
            builder.append(',').append(t.getText());
        }
        builder.append(')');
        builder.append("{ return current + (")
            .append(trim($STRING.text))
            .append("); }");
        $a = builder.toString();
    }
    ;

fnCombine returns [String c]
    : fn=( PLUS^ | MINUS^ | STAR^ | DIV^ ) {
        $c = evalFnCombine($fn.type);
    }
    ;

fnReset returns [String r]
    : NUMBER {
        $r = "function(){ return " + $NUMBER.text + ";}";
    }
    ;

constant returns [ConstantPostAggregator c]
    : value=NUMBER  {
        double v = Double.parseDouble($value.text);
        $c = new ConstantPostAggregator(Double.toString(v), v, null);
    }
    ;

/**
 * "filters":{"offer_id":{"$in":[1,2]}}
 */
filter_comp
    : '"filters"' ':' OBJOPEN! f=dataFilter OBJCLOSE! {
        if($f.filter != null) this.filter = $f.filter;
        if($f.spec != null) this.having = $f.spec;
    }
    ;

dataFilter returns [DimFilter filter, HavingSpec spec]
    : oe=orDataFilter { $filter = $oe.filter; $spec = $oe.spec; }
    | ae=andDataFilter { $filter = $ae.filter; $spec = $ae.spec; }
    ;

orDataFilter returns [DimFilter filter, HavingSpec spec]
    : OR ':' OBJOPEN! a=primaryDataFilter (COMMA! b+=primaryDataFilter)* OBJCLOSE! {
        if($b.isEmpty()) {
            $filter = $a.filter;
            $spec = $a.spec;
        } else {
            List<DimFilter> restFilters = new ArrayList<DimFilter>();
            List<HavingSpec> restSpecs = new ArrayList<HavingSpec>();
            if (null != $a.filter)
                restFilters.add($a.filter);
            if (null != $a.spec)
                restSpecs.add($a.spec);
            for(PrimaryDataFilterContext e : $b) {
                if (null != e.filter)
                    restFilters.add(e.filter);
                if (null != e.spec)
                    restSpecs.add(e.spec);
            }

            if (!restFilters.isEmpty()) {
                $filter = new OrDimFilter(restFilters);
            }

            if (!restSpecs.isEmpty()) {
                $spec = new OrHavingSpec(restSpecs);
            }

        }
    }
    | OR ':' OBJOPEN! OBJCLOSE!
    ;

andDataFilter returns [DimFilter filter, HavingSpec spec]
    : AND ':' OBJOPEN! a=primaryDataFilter (COMMA! b+=primaryDataFilter)* OBJCLOSE! {
        if($b.isEmpty()) {
            $filter = $a.filter;
            $spec = $a.spec;
        } else {
            List<DimFilter> restFilters = new ArrayList<DimFilter>();
            List<HavingSpec> restSpecs = new ArrayList<HavingSpec>();
            if (null != $a.filter)
                restFilters.add($a.filter);
            if (null != $a.spec)
                restSpecs.add($a.spec);
            for(PrimaryDataFilterContext e : $b) {
                if (null != e.filter)
                    restFilters.add(e.filter);
                if (null != e.spec)
                    restSpecs.add(e.spec);
            }

            if (!restFilters.isEmpty()) {
                $filter = new AndDimFilter(restFilters);
            }

            if (!restSpecs.isEmpty()) {
                $spec = new AndHavingSpec(restSpecs);
            }

        }
    }
    | AND ':' OBJOPEN! OBJCLOSE!
    ;

primaryDataFilter returns [DimFilter filter, HavingSpec spec]
    : fSingle=selectorDimFilter   { $filter = $fSingle.filter; }
    | fMulti=multiSelectorDimFilter     { $filter = $fMulti.filter; }
    | hv=havingSpec       { $spec = $hv.spec; }
    | f=dataFilter { $filter = $f.filter; $spec = $f.spec; }
    ;

/*
    : fSingle=selectorDimFilter   { $filter = $fSingle.filter; }
    | fMulti=multiSelectorDimFilter     { $filter = $fMulti.filter; }
*/
selectorDimFilter returns [DimFilter filter]
    : dimension=STRING ':' '{' op=(EQ|NEQ|JAVASCRIPT|MATCH) ':' value=STRING '}' {
        $filter = evalDimFilter($op, $dimension, $value);
        types.put(trim($dimension.getText()),ValueType.STRING);

    }
    | dimension=STRING ':' '{' op=(EQ|NEQ|JAVASCRIPT|MATCH) ':' value=NUMBER '}' {
        $filter = evalDimFilter($op, $dimension, $value);
        types.put(trim($dimension.getText()),ValueType.NUMBER);
    }
    ;

multiSelectorDimFilter returns [DimFilter filter]
    : dimension=STRING ':' '{' op=(IN|NIN) ':' (ARRAYOPEN! ( (vals+=NUMBER (COMMA! vals+=NUMBER)*) ) ARRAYCLOSE!) '}' {
    	String dim = trim($dimension.text);
    	if(!ArrayUtils.contains(timeTypes,dim))
    	$filter = evalDimFilter($op, $dimension, $vals);
        types.put(trim($dimension.getText()),ValueType.NUMBER);
    }
    | dimension=STRING ':' '{' op=(IN|NIN) ':' (ARRAYOPEN! ( (vals+=STRING (COMMA! vals+=STRING)*) ) ARRAYCLOSE!) '}' {
    	String dim = trim($dimension.text);
    	if(!ArrayUtils.contains(timeTypes,dim))
        $filter = evalDimFilter($op, $dimension, $vals);
        types.put(trim($dimension.getText()),ValueType.STRING);
    }
    ;

/*
havingSpec
    : NOT=notHavingSpec
    | GT=greaterThanHavingSpec
    | LT=lessThanHavingSpec
    | GTE=greaterEqualHavingSpec
    | LTE=lessEqualHavingSpec
    ;
*/

havingSpec returns [HavingSpec spec]
    : dimension=STRING ':' '{' op=(NOT|GT|GTE|LT|LTE) ':' value=NUMBER '}' {
        String dim = trim($dimension.text);
        Number val;
        try {
            val = NumberFormat.getInstance().parse(trim($value.text));
        }
        catch(ParseException e) {
            throw new IllegalArgumentException("Unable to parse number [" + $value.text + "]");
        }
        if(!ArrayUtils.contains(timeTypes,dim)){
			List<HavingSpec> specs;
	        switch($op.type) {
	            case(NOT): $spec = new NotHavingSpec(new EqualToHavingSpec(dim, val)); break;
	            case(GT): $spec = new GreaterThanHavingSpec(dim, val); break;
	            case(GTE): {
	                specs = new ArrayList<HavingSpec>();
	                specs.add(new GreaterThanHavingSpec(dim, val));
	                specs.add(new EqualToHavingSpec(dim, val));
	                $spec = new OrHavingSpec(specs);
	                break;
	            }
	            case(LT): $spec = new LessThanHavingSpec(dim, val); break;
	            case(LTE): {
	                specs = new ArrayList<HavingSpec>();
	                specs.add(new LessThanHavingSpec(dim, val));
	                specs.add(new EqualToHavingSpec(dim, val));
	                $spec = new OrHavingSpec(specs);
	                break;
	            }
	        }
        }else{
	        switch($op.type) {
	            case(NOT): timeFilters.put(dim,new TimeFilter(IntervalUnit.valueOf(dim.toUpperCase()),TokenType.notEquals,val.toString())); break;
	            case(GT): break;
	            case(GTE): break;
	            case(LT): break;
	            case(LTE): break;
	            default: break;
	        }
        }
    }
    ;

timestamp returns [DateTime t]
    : NUMBER {
        if ($NUMBER != null) {
            String str = Strings.nullToEmpty($NUMBER.text).trim();
            try {
                $t = new DateTime(1000L * NumberFormat.getInstance().parse(str).longValue(), DateTimeZone.UTC);
            }
            catch(ParseException e) {
                logger.error("Unable to parse number [" + str + "]", e);
            }
        }
    }
    | STRING {
        if ($STRING != null && !Strings.isNullOrEmpty($STRING.text)) {
            $t = new DateTime(unescape($STRING.text));
        }
    }
    ;

sort_comp
    :'"sort"' ':' ARRAYOPEN! s+=orderBySpec (COMMA! s+=orderBySpec)* ARRAYCLOSE! {
        for(OrderBySpecContext oc : $s) {
            if (null != oc) {
                OrderByColumnSpec o = oc.o;
                if (null != o) {
                	if(!ArrayUtils.contains(timeTypes,o.getDimension())){
                		columns.add(o);
                	}else{
                    	timeColumns.add(o);
                	}
                }
            }
        }
    }
    | '"sort"' ':' ARRAYOPEN! ARRAYCLOSE!
    ;

orderBySpec returns [OrderByColumnSpec o]
    : OBJOPEN! key=orderByKey (COMMA! dir=orderByDir)* OBJCLOSE! {
        $o = evalOrderBySpec($key.key, $dir.dir);
    }
    | OBJOPEN! dir=orderByDir COMMA! key=orderByKey OBJCLOSE! {
        $o = evalOrderBySpec($key.key, $dir.dir);
    }
    ;

orderByKey returns [String key]
    : '"orderBy"' ':' k=STRING {
        if (!Strings.isNullOrEmpty($k.text)) {
            $key = trim($k.text);
        }
    }
    ;

orderByDir returns [int dir]
    : '"order"' ':' d=NUMBER {
        $dir = ASC;
        if (!Strings.isNullOrEmpty($d.text)) {
            String str = null;
            try {
                str = trim($d.text);
                $dir = NumberFormat.getInstance().parse(str).intValue();
            } catch(ParseException e) {
                logger.error("Unable to parse number [" + str + "]", e);
            }
        }
    }
    ;

currency_comp
    :'"currency_type"' ':' curc=STRING {
        currency = $curc.text;
    }
    ;
