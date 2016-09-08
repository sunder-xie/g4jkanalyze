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
 * 根据type类型对应的维表，累计当天标签下的人数(区别imsi)，http 4G流量，
 * @author yanxu
 * 20160907
 */
public class Yunguan_G4JK_TagsAccToRedis extends BaseRichBolt {
	//代码自动生成的类序列号
	private static final long serialVersionUID = 3462618274077135401L;

	//记录作业日志到storm的logs目录下对应的topology日志中
	public static Logger LOG=Logger.getLogger(Yunguan_G4JK_TagsAccToRedis.class);
	
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

	//按照4G网分数据IMSI进行标签的汇总，流量的汇总
	@Override
	public void execute(Tuple tuple) {
		//redis操作
		redisserver=RedisServer.getInstance();
		String tdate=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.STARTTIME);
		String imsi=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.IMSI);
		String dlflux=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.DL_DATA);
		String ulflux=tuple.getStringByField(Yunguan_G4JK_Basic4GFields.UL_DATA);
		String tag=null;
		String key=null;
		double g4flux=0;
		if(tdate.length()>11&&imsi.length()>=15){
			key="ref_"+imsi;
			//查询维表获取标签
			tag=redisserver.get(key);
			if(tag!=null&&tag.equals("nil")==false)
			{
				tdate=tdate.substring(0,10);
				key="mfg4_"+tdate+"_tagset_"+tag;
				//将imsi累计到对应的标签中
				redisserver.sadd(key, imsi);
				
				key="mfg4_"+tdate+"_tagflux_"+tag;
				g4flux=(Double.valueOf(dlflux)+Double.valueOf(ulflux))/1048576; //单位由Byte转为MB
				//将标签产生的流量值累计到对应的标签中
				redisserver.incrbyfloat(key, g4flux);
			}
		}
		//释放内存
		redisserver=null;
		tdate=null;
		imsi=null;
		dlflux=null;
		ulflux=null;
		tag=null;
		key=null;
		g4flux=0;
		collector.ack(tuple);
	}
	
	//对发射出去的元组进行字段的声明
	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		//字段说明，如果execute有后续处理需求，发射后可以依赖以下字段进行标记
	}

}
