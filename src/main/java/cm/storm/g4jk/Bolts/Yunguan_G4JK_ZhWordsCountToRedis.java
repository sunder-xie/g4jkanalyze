package cm.storm.g4jk.Bolts;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.storm.shade.org.apache.commons.codec.binary.Base64;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apdplat.word.WordSegmenter;
import org.apdplat.word.segmentation.Word;

import cm.storm.g4jk.Beans.Yunguan_G4JK_Basic4GFields;
import cm.storm.g4jk.Commons.RedisServer;

/**
 * 对url中出现的中文进行拆词，统计热搜次数，元组来源于SJJS093这个bolt
 * 这是用户网络热搜的基础
 * @author chinamobile
 *
 */
public class Yunguan_G4JK_ZhWordsCountToRedis extends BaseRichBolt {
	//代码自动生成的类序列号
	private static final long serialVersionUID = 156585005107889286L;
	
	//记录作业日志到storm的logs目录下对应的topology日志中
	public static Logger LOG=Logger.getLogger(Yunguan_G4JK_ZhWordsCountToRedis.class);

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

	//初始方案：获取url中提取的中文字符，进行拆词并统计热词
	@Override
	public void execute(Tuple tuple) {
		//redis操作
		redisserver=RedisServer.getInstance();
		String tdate=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.STARTTIME);
		String chwords=tuple.getStringByField("ChineseInfo");		
		List<Word> words = null;	
		String key=null;
		try{
			
			tdate=tdate.substring(0,10);	//获取日期YYYY-MM-DD

			if(chwords!=null&&chwords.length()>=2&&tdate!=null&&tdate.length()==10){
				//1.对中文做分词，移除停用词，采用words库，详细参考pom的配置
				words=WordSegmenter.seg(chwords);
				//2.对热词做BASE64URLSAFE转码，然后存入集合中，对每个热词分别做计数
				if(words!=null&&words.size()>0){
					for(int i=0;i<words.size();i++)
					{
						chwords=words.get(i).getText();
						if(chwords!=null&&chwords.length()>=2){
							chwords=Base64.encodeBase64URLSafeString(chwords.getBytes("UTF-8"));
							if(chwords!=null&&chwords.length()>0){
								key="mfg4_"+tdate+"_ChineseSet";
								redisserver.sadd(key, chwords);
								key="mfg4_"+tdate+"_Zh_"+chwords;
								redisserver.incr(key);
							}
							chwords=null;
						}
					}
				}
			}
		}catch(Exception ex){
			//LOG.info(" Thread Yunguan_G4JK_ChineseWordsCountToRedis execute crashes: "+ex.getMessage());
		}

		//释放内存
		redisserver=null;
		chwords=null;
		tdate=null;
		words = null;	
		key=null;
		collector.ack(tuple);
	}

	//对发射出去的元组进行字段的声明
	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		//字段说明，如果execute有后续处理需求，发射后可以依赖以下字段进行标记
	}
}


//String imsi=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.IMSI);
//String tac=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.TAC);
//String ci=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.CID);
//String intsid=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.INTSID);
//String host=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.HOST);
//imsi=null;
//tac=null;
//ci=null;
//intsid=null;
//host=null;
//String sdate=null;
//sdate=tdate;
//String value=null;
//value="";
//value=null;
//sdate=null;
//value+=","+chwords;
//if(value.length()>0)value=value.substring(1);
//
////记录用户url中关注信息的明细记录
//if(imsi.length()>=15&&tac.length()>0&&sdate.length()>=23&&value.length()>0)
//{
//	sdate=sdate.substring(0, 13);  //以小时为单位
//	sdate=sdate.replaceAll("[^0-9]","");
//	sdate+="0000";
//	key="mfg4_"+tdate+"_SrhDetail_"+imsi; //记录每个imsi当天的热搜记录信息
//	value=tac+"#"+ci+"#"+value+"#"+intsid+"#"+host+"#"+sdate;
//	redisserver.sadd(key, value);
//}

//如果获取的词的长度6个字以内，不进行拆词
//if(chwords.length()<=6){
//	chwords=Base64.encodeBase64URLSafeString(chwords.getBytes("UTF-8"));
//	if(chwords!=null&&chwords.length()>0){
//		key="mfg4_"+tdate+"_ChineseSet";
//		redisserver.sadd(key, chwords);
//		key="mfg4_"+tdate+"_Zh_"+chwords;
//		redisserver.incr(key);
//	}
//	chwords=null;
//}else{//对于超长的字符串，才进行拆词

