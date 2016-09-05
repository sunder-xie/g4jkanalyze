package cm.storm.g4jk.Bolts;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import cm.storm.g4jk.Beans.Yunguan_G4JK_BasicTAUFields;
import cm.storm.g4jk.Beans.Yunguan_G4JK_BasicTAUBean;

/*
 * 2016-09-05 对4G网分位置变动记录字段拆分解析
 * nicolashsu
 */
public class Yunguan_G4JK_BasicTAUSplitter extends BaseRichBolt {
	//代码自动生成的类序列号
	private static final long serialVersionUID = 7495547906879156065L;

	//记录作业日志到storm的logs目录下对应的topology日志中
	public static Logger LOG=Logger.getLogger(Yunguan_G4JK_BasicTAUSplitter.class);
	
	//元组发射搜集器
	private OutputCollector collector;
			
	//元组存储结构
	private Yunguan_G4JK_BasicTAUBean g4jkbasictaubean=null;
	
	//初始化bolt元组搜集器，用于存放需要发射元组
	@Override
	public void prepare(Map conf, 
			TopologyContext topologyContext, 
			OutputCollector outputCollector) {
		this.collector = outputCollector;
	}

	//从Spoutwftau获取字段信息进行字段解析,发射
	@Override
	public void execute(Tuple tuple) {
		// TODO Auto-generated method stub
		String str_tuple=tuple.getString(0);		//获取tuple中的string组成的记录
		if(StringUtils.isBlank(str_tuple)==false)
		{
			//字段获取开始
			process_tuple(str_tuple);
			//字段获取结束
			
			if(g4jkbasictaubean!=null)
			{
				//发射元组
				collector.emit(new Values(
						g4jkbasictaubean.getTtime(),
						g4jkbasictaubean.getImsi(),
						g4jkbasictaubean.getTac(),
						g4jkbasictaubean.getCi()
				));
			}
		}
		//释放内存
		g4jkbasictaubean=null;
		collector.ack(tuple);
	}

	//自定义方法区域
	/*
	 * @param tuple:原始String类型的一条会话流
	 */
	public void process_tuple(String tuple){
		
	}
	
	//对发射出去的元组进行字段的声明
	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		//字段说明，如果execute有后续处理需求，发射后可以依赖以下字段进行标记
		outputFieldsDeclarer.declare(new Fields(
				Yunguan_G4JK_BasicTAUFields.TTIME,
				Yunguan_G4JK_BasicTAUFields.IMSI,
				Yunguan_G4JK_BasicTAUFields.TAC,
				Yunguan_G4JK_BasicTAUFields.CI
		));
	}

}
