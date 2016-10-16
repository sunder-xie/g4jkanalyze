package cm.storm.g4jk.Bolts;

import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import cm.storm.g4jk.Beans.Yunguan_G4JK_Basic4GFields;
import cm.storm.g4jk.Commons.RedisServer;

public class Yunguan_G4JK_SJJS245_87ToRedis extends BaseRichBolt {
	//代码自动生成的类序列号
	private static final long serialVersionUID = -1003508587231025472L;
	
	//记录作业日志到storm的logs目录下对应的topology日志中
	public static Logger LOG=Logger.getLogger(Yunguan_G4JK_SJJS245_87ToRedis.class);
	
	//元组发射搜集器
	private OutputCollector collector;

	//获取redis连接
	private RedisServer redisserver;
	
	//初始化bolt元组搜集器，用于存放需要发射元组
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, 
			TopologyContext topologyContext, 
			OutputCollector outputCollector) {
		this.collector = outputCollector;
	}

	//初始方案：先通过url进行检索，获取对应的中文字与维表上的中文字直接匹配
	@Override
	public void execute(Tuple tuple) {
		//redis操作
		redisserver=RedisServer.getInstance();
		String tdate=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.STARTTIME);
		String imsi=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.IMSI);
		String words=null;
		String key=null;
		String phnum=null;
		boolean flag=false;
		
		//业务app小类，上网业务小类id
		//String appsid=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.INTAPPID);
		//String intsid=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.INTSID);
		if(imsi!=null&&imsi.length()>=15){
			key="ref_imsiphn_"+imsi;
			phnum=redisserver.get(key);
			if(phnum!=null&&phnum.length()>=11){
				tdate=tdate.substring(0,10);	//获取日期YYYY-MM-DD
					
				//业务：分析用户是为苹果手机潜在客户，对应触点id为SJJS245
				key="ref_sjjsparams_SJJS245";
				words=redisserver.get(key);	//20161009取值为--上网搜索热词:苹果#iphone;上网行为类型:购物;
				flag=businessJudge(tuple, words, phnum);		
				if(flag==true){								//需要触点，再将号码放入当天的触点集合中
					key="mfg4_"+tdate+"_sjjs_"+phnum;
					redisserver.sadd(key, "SJJS245");
					key="mfg4_"+tdate+"_UnTouchSet";
					redisserver.sadd(key, phnum);
				}
			}
		}
		//释放内存
		redisserver=null;
		tdate=null;
		imsi=null;
		phnum=null;
		words=null;
		key=null;
		
		collector.ack(tuple);
	}
	
	//自定义方法区域
	/**
	 * 触点逻辑判断，决定号码是否为潜在推送触点的目标号码
	 * @param tuple 流数据，已经划分好字段，直接取值用于做业务判断
	 * @param words 从触点配置的参数中获取的所有参数，格式为 名称1:值1;名称2:值2;名称3:值3;...;
	 * @param phnum 用户的号码
	 * @return 用于最后标记业务逻辑判断是否添加触点 true为需要添加，false为不需要添加
	 */
	public boolean businessJudge(Tuple tuple, String words, String phnum){
		boolean flag=false; 
		String url=null;
		String[] params=null;
		String[] keywords=null;
		int i=0;
		int j=0;
		//没有参数，直接返回
		if(words==null||words.endsWith(";")==false)return false;
		
		params=words.split(";");
		//内存不足直接返回
		if(params==null||params.length<1)return false;
		
		//对url做转换操作，如果解析失败，返回false
		try {
			String reg = "[^\u4e00-\u9fa5]";   //^匹配所有非中文字符, \u4e00, \u9fa5代表是两个unicode编码值，他们正好是Unicode表中的汉字的头和尾
			url=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.URL);
			String fis= java.net.URLDecoder.decode(url, "gb2312");
			String sec = new String(fis.getBytes("gb2312"), "gb2312");
			if (fis.equals(sec)==true)
				url=fis;
	        else
	        	url= java.net.URLDecoder.decode(url, "utf-8");
			
			//提取中文
			url = url.replaceAll(reg, "");
		} catch (Exception ex) {
//			LOG.info("Yunguan_G4JK_TouchSjjsToRedis execute error: "+ex.getMessage());
			return false;
		}
		
		for(i=0;i<params.length;i++){
			keywords=null;
			words=params[i].trim();
			if(words!=null&&words.contains(":")==true)keywords=words.split(":");
			//必须是变量名称和值组成，长度必须为2
			if(keywords!=null&&keywords.length==2){
				//热词搜索匹配逻辑
				if(keywords[0].contains("热词")){
					words=keywords[1]+"#"; //取值为--苹果#iphone#
					keywords=words.split("#");
					for(j=0;j<keywords.length;j++){
						keywords[j]=keywords[j].trim();
						if(keywords[j].equals("")==false&&url.contains(keywords[j])==true){
							return true;
						}
					}
				}
			}
		}
		return flag;
	}

	//对发射出去的元组进行字段的声明
	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		//字段说明，如果execute有后续处理需求，发射后可以依赖以下字段进行标记
	}

}
