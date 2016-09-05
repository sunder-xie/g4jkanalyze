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

import cm.storm.g4jk.Beans.Yunguan_G4JK_BasicJKBean;
import cm.storm.g4jk.Beans.Yunguan_G4JK_BasicJKFields;
import cm.storm.g4jk.Commons.TimeFormatter;

/*
 * 2016-09-05 对家宽网分记录字段拆分解析
 * nicolashsu
 */
public class Yunguan_G4JK_BasicJKSplitter extends BaseRichBolt {
	//代码自动生成的类序列号
	private static final long serialVersionUID = -5352411334056685457L;

	//记录作业日志到storm的logs目录下对应的topology日志中
	public static Logger LOG=Logger.getLogger(Yunguan_G4JK_BasicJKSplitter.class);
	
	//元组发射搜集器
	private OutputCollector collector;
		
	//元组存储结构
	private Yunguan_G4JK_BasicJKBean g4jkbasicjkbean=null;

	//初始化bolt元组搜集器，用于存放需要发射元组
	@Override
	public void prepare(Map conf, 
			TopologyContext topologyContext, 
			OutputCollector outputCollector) {
		this.collector = outputCollector;
	}

	//从Spoutwfjk获取字段信息进行字段解析,发射
	@Override
	public void execute(Tuple tuple) {
		String str_tuple=tuple.getString(0);		//获取tuple中的string组成的记录
		if(StringUtils.isBlank(str_tuple)==false)
		{
			//字段获取开始
			process_tuple(str_tuple);
			//字段获取结束
			
			if(g4jkbasicjkbean!=null)
			{
				//发射元组
				collector.emit(new Values(
						g4jkbasicjkbean.getStarttime(),
						g4jkbasicjkbean.getLasttime(),
						g4jkbasicjkbean.getUser_name(),
						g4jkbasicjkbean.getUser_type(),
						g4jkbasicjkbean.getUri(),
						g4jkbasicjkbean.getApp_type(),
						g4jkbasicjkbean.getApp_sub_type(),
						g4jkbasicjkbean.getUp_flux(),
						g4jkbasicjkbean.getDown_flux(),
						g4jkbasicjkbean.getSuccess(),
						g4jkbasicjkbean.getUser_ip(),
						g4jkbasicjkbean.getDest_ip(),
						g4jkbasicjkbean.getDest_port(),
						g4jkbasicjkbean.getResponse_time()
				));
			}
		}
		//释放内存
		g4jkbasicjkbean=null;
		collector.ack(tuple);
	}

	//自定义方法区域
	/*
	 * @param tuple:原始String类型的一条会话流
	 */
	public void process_tuple(String tuple){
		g4jkbasicjkbean=new Yunguan_G4JK_BasicJKBean();
		String attr_value=new String("");
		String get_subattr=new String("");
		String[] fields_set=tuple.split("\t");//按照TAB作为间隔划分字段
		String[] get_subfields=null;
		if(fields_set==null){
			g4jkbasicjkbean=null;
			return;
		}
		
		if(fields_set.length>0)
		{
			//字段1.获取日期并做格式转换
			attr_value=TimeFormatter.Tra_realdate2(fields_set[0]);
			g4jkbasicjkbean.setStarttime(attr_value);
			
		}
	}
	
	//对发射出去的元组进行字段的声明
	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		//字段说明，如果execute有后续处理需求，发射后可以依赖以下字段进行标记
		outputFieldsDeclarer.declare(new Fields(
			Yunguan_G4JK_BasicJKFields.STARTTIME,
			Yunguan_G4JK_BasicJKFields.LASTTIME,
			Yunguan_G4JK_BasicJKFields.USER_NAME,
			Yunguan_G4JK_BasicJKFields.USER_TYPE,
			Yunguan_G4JK_BasicJKFields.URI,
			Yunguan_G4JK_BasicJKFields.APP_TYPE,
			Yunguan_G4JK_BasicJKFields.APP_SUB_TYPE,
			Yunguan_G4JK_BasicJKFields.UP_FLUX,
			Yunguan_G4JK_BasicJKFields.DOWN_FLUX,
			Yunguan_G4JK_BasicJKFields.SUCCESS,
			Yunguan_G4JK_BasicJKFields.USER_IP,
			Yunguan_G4JK_BasicJKFields.DEST_IP,
			Yunguan_G4JK_BasicJKFields.DEST_PORT,
			Yunguan_G4JK_BasicJKFields.RESPONSE_TIME
		));
	}

}
