package cm.storm.g4jk.Bolts;

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
				//如果获取的词的长度大于6，才进行拆词
				if(chwords.length()>6){
					//1.对中文做分词，移除停用词，采用words库，详细参考pom的配置
					words=WordSegmenter.seg(chwords);
					//2.对热词做md5转码，然后存入集合中，同时每个字符做计数
					if(words!=null&&words.isEmpty()==false){
						for(int i=0;i<words.size();i++)
						{
							chwords=words.get(i).getText();
							if(chwords!=null&&chwords.length()>=2)chwords=Base32Encode(chwords);
							if(chwords!=null&&chwords.length()>0){
								key="mfg4_"+tdate+"_ChineseSet";
								redisserver.sadd(key, chwords);
								key="mfg4_"+tdate+"_Zh_"+chwords;
								redisserver.incr(key);
							}
							chwords=null;
						}
					}
				}else{
					chwords=Base32Encode(chwords);
					if(chwords!=null&&chwords.length()>0){
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
	 * 将明文的触点签名，转化为 Base 32位数字与字母组成的编码
	 * @param sign 触点需求为account，timestamp，key组合成的签名
	 * @return resSign 转化后的Base 32位 数字与字母组成的编码，小写
	 */
	private String Base32Encode(String chwd){
		String res=null;
		String base32Chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";  //Base32加密算法
		try{
	        byte bytes[] = chwd.getBytes();//获取中文转成的bytes
	        int i = 0, index = 0, digit = 0;  
	        int currByte, nextByte;  
	        StringBuffer base32 = new StringBuffer((bytes.length + 7) * 8 / 5);  
	        while (i < bytes.length) {  
	            currByte = (bytes[i] >= 0) ? bytes[i] : (bytes[i] + 256); // 无符号  
	            /* Is the current digit going to span a byte boundary? */  
	            if (index > 3) {  
	                if ((i + 1) < bytes.length) {  
	                    nextByte = (bytes[i + 1] >= 0)?bytes[i + 1]:(bytes[i + 1] + 256);  
	                } else {  
	                    nextByte = 0;  
	                }  
	                digit = currByte & (0xFF >> index);  
	                index = (index + 5) % 8;  
	                digit <<= index;  
	                digit |= nextByte >> (8 - index);  
	                i++;  
	            } else {  
	                digit = (currByte >> (8 - (index + 5))) & 0x1F;  
	                index = (index + 5) % 8;  
	                if (index == 0) i++;  
	            }  
	            base32.append(base32Chars.charAt(digit));  
	        }  
	        res=base32.toString();
		}catch(Exception ex){
			LOG.info(" Thread Base32Encode crashes: "+ex.getMessage());
			return null;
		}
		return res;
	}
}

//private static final String base32Chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";  
//private static final int[] base32Lookup = { 0xFF, 0xFF, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F, // '0', '1', '2', '3', '4', '5', '6', '7'  
//      0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // '8', '9', ':', ';', '<', '=',  '>', '?'  
//      0xFF, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, // '@', 'A', 'B', 'C', 'D', 'E', 'F', 'G'  
//      0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, // 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O'  
//      0x0F, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, // 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W'  
//      0x17, 0x18, 0x19, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // 'X', 'Y', 'Z', '[', '', ']', '^', '_'  
//      0xFF, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, // '`', 'a', 'b', 'c', 'd', 'e', 'f', 'g'  
//      0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, // 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o'  
//      0x0F, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, // 'p', 'q', 'r', 's', 't', 'u', 'v', 'w'  
//      0x17, 0x18, 0x19, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF // 'x', 'y', 'z', '{', '|', '}', '~', 'DEL'  
//}; 
