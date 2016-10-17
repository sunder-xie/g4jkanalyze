package cm.storm.g4jk.Bolts;

import java.security.MessageDigest;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apdplat.word.WordSegmenter;
import org.apdplat.word.segmentation.Word;

import cm.storm.g4jk.Commons.RedisServer;

public class Yunguan_G4JK_ChineseWordsCountToRedis extends BaseRichBolt {
	//代码自动生成的类序列号
	private static final long serialVersionUID = 156585005107889286L;
	
	//记录作业日志到storm的logs目录下对应的topology日志中
	public static Logger LOG=Logger.getLogger(Yunguan_G4JK_ChineseWordsCountToRedis.class);

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
		List<Word> words = null;
		String chwords=null;
		String tdate=null;
		String key=null;
		try{
			tdate=tuple.getStringByField("TupleDate");
			chwords=tuple.getStringByField("ChineseInfo");
			if(chwords!=null&&chwords.length()>2&&tdate!=null&&tdate.length()==10){
				//如果获取的词的长度大于7，才进行拆词
				if(chwords.length()>7){
					//1.对中文做分词，移除停用词，采用words库，详细参考pom的配置
					words=WordSegmenter.seg(chwords);
					//2.对热词做md5转码，然后存入集合中，同时每个字符做计数
					if(words!=null&&words.isEmpty()==false){
						for(int i=0;i<words.size();i++)
						{
							chwords=words.get(i).getText();
							if(chwords!=null&&chwords.length()>=2)chwords=md5str32(chwords);
							if(chwords!=null&&chwords.length()==16){
								key="mfg4_"+tdate+"_ChineseSet";
								redisserver.sadd(key, chwords);
								key="mfg4_"+tdate+"_Zh_"+chwords;
								redisserver.incr(key);
							}
							chwords=null;
						}
					}
				}else{
					chwords=md5str32(chwords);
					if(chwords!=null&&chwords.length()==16){
						key="mfg4_"+tdate+"_ChineseSet";
						redisserver.sadd(key, chwords);
						key="mfg4_"+tdate+"_Zh_"+chwords;
						redisserver.incr(key);
					}
					chwords=null;
				}
			}
		}catch(Exception ex){
			//LOG.info(" Thread Yunguan_G4JK_ChineseWordsCountToRedis execute crashes: "+ex.getMessage());
		}

		//释放内存
		redisserver=null;
		words = null;
		chwords=null;
		tdate=null;
		collector.ack(tuple);
	}

	//对发射出去的元组进行字段的声明
	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		//字段说明，如果execute有后续处理需求，发射后可以依赖以下字段进行标记
	}
	
	//自定义方法区间
	/**
	 * 将明文的触点签名，转化为MD5 32位数字与字母组成的16进制编码
	 * @param sign 触点需求为account，timestamp，key组合成的签名
	 * @return resSign 转化后的MD5 32位 16进制编码，小写
	 */
	private String md5str32(String chwd){
		String resSign=null;
		try{
			 // 生成一个MD5加密计算摘要
	        MessageDigest md = MessageDigest.getInstance("MD5");
	        // 计算md5函数
	        md.update(chwd.getBytes());
	        // digest()最后确定返回md5 hash值，返回值为8为字符串。因为md5 hash值是16位的hex值，实际上就是8位的字符
	        byte b[] = md.digest();
	        int i;
	        StringBuffer buf = new StringBuffer("");
	        for (int offset = 0; offset < b.length; offset++) {
		        i = b[offset];
		        if (i < 0)i += 256;
		        if (i < 16)buf.append("0");
		        buf.append(Integer.toHexString(i));
	        }
	        //最后获取32位的md5码
	        resSign = buf.toString();
		}catch(Exception ex){
			LOG.info(" Thread md5str16 crashes: "+ex.getMessage());
			return null;
		}
		return resSign;
	}

}
