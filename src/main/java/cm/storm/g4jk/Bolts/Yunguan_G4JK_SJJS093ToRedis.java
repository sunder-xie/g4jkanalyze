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


/**
 * 实时分析网分记录IMSI对应的号码，以及号码可以参与的SJJSXXX活动，符合规则则添加SJJSid到对应的key中，并且将号码丢入触点集合中
 * @author chinamobile
 * 20161008
 */
public class Yunguan_G4JK_SJJS093ToRedis extends BaseRichBolt {
	//代码自动生成的类序列号
	private static final long serialVersionUID = -2349911902769092963L;

	//记录作业日志到storm的logs目录下对应的topology日志中
	public static Logger LOG=Logger.getLogger(Yunguan_G4JK_SJJS093ToRedis.class);
	
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
		String url=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.URL);
		String words=null;
		String key=null;
		String[] keywords=null;
		String phnum=null;
		int i=0;
		
		//业务app小类，上网业务小类id
		//String appsid=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.INTAPPID);
		//String intsid=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.INTSID);
		if(imsi!=null&&imsi.length()>=15&&url!=null&&url.trim().equals("none")==false&&url.trim().equals("")==false){
			key="ref_imsiphn_"+imsi;
			phnum=redisserver.get(key);
			if(phnum!=null&&phnum.length()>=11){
				try {
					url=java.net.URLDecoder.decode(url, "utf-8");
					tdate=tdate.substring(0,10);	//获取日期YYYY-MM-DD
					
					//业务：分析用户是否搜索宽带关键字，对应触点id为SJJS093
					key="ref_sjjsparams_SJJS093";
					words=redisserver.get(key);	//取值为--上网行为类型:购物#论坛;上网搜索热词:家宽#宽带#极光#电信;
					if(words!=null&&words.trim().length()>0){
						i=words.indexOf("热词");	//针对热词挖掘行为特征
						if(i>=0){
							words=words.substring(i);//取值为--热词:家宽#宽带#极光#电信
							i=words.indexOf(":");
							if(i>=0){
								words=words.substring(i+1);//取值为--家宽#宽带#极光#电信
								keywords=words.split("#");
								for(i=0;i<keywords.length;i++){
									keywords[i]=keywords[i].trim();
									if(keywords[i].equals("")==false&&url.contains(keywords[i])==true){
										key="mfg4_"+tdate+"_sjjs_"+phnum;
										redisserver.sadd(key, "SJJS093");
										key="mfg4_"+tdate+"_UnTouchSet";
										redisserver.sadd(key, phnum);
										break;
									}
								}
							}
						}
					}
				} catch (Exception ex) {
					LOG.info("Yunguan_G4JK_TouchSjjsToRedis execute error: "+ex.getMessage());
				}
			}
			
		}
		
		//释放内存
		redisserver=null;
		tdate=null;
		imsi=null;
		phnum=null;
		url=null;
		words=null;
		key=null;
		keywords=null;
		i=0;
		
		collector.ack(tuple);
	}

	//对发射出去的元组进行字段的声明
	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		//字段说明，如果execute有后续处理需求，发射后可以依赖以下字段进行标记
	}
}
