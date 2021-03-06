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

import cm.storm.g4jk.Beans.Yunguan_G4JK_BasicATDTBean;
import cm.storm.g4jk.Beans.Yunguan_G4JK_BasicATDTFields;
import cm.storm.g4jk.Commons.TimeFormatter;

/*
 * 2016-09-05 对4G网分开关机位置变动记录字段拆分解析
 * nicolashsu
 */
public class Yunguan_G4JK_BasicATDTSplitter extends BaseRichBolt {
	//代码自动生成的类序列号
	private static final long serialVersionUID = 8681242084086825906L;
	
	//记录作业日志到storm的logs目录下对应的topology日志中
	public static Logger LOG=Logger.getLogger(Yunguan_G4JK_BasicATDTSplitter.class);
	
	//元组发射搜集器
	private OutputCollector collector;
				
	//元组存储结构
	private Yunguan_G4JK_BasicATDTBean g4jkbasicatdtbean=null;

	//初始化bolt元组搜集器，用于存放需要发射元组
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, 
			TopologyContext topologyContext, 
			OutputCollector outputCollector) {
		this.collector = outputCollector;
	}

	//从Spoutwfatdt获取字段信息进行字段解析,发射
	@Override
	public void execute(Tuple tuple) {
		String str_tuple=tuple.getString(0);		//获取tuple中的string组成的记录
		if(StringUtils.isBlank(str_tuple)==false)
		{
			//字段获取开始
			process_tuple(str_tuple);
			//字段获取结束
			
			if(g4jkbasicatdtbean!=null)
			{
				//发射元组
				collector.emit(new Values(
						g4jkbasicatdtbean.getTtime(),
						g4jkbasicatdtbean.getAttach_type(),
						g4jkbasicatdtbean.getImsi(),
						g4jkbasicatdtbean.getTac(),
						g4jkbasicatdtbean.getCi()
				));
			}
		}
		//释放内存
		g4jkbasicatdtbean=null;
		collector.ack(tuple);
	}

	//自定义方法区域
	/*
	 * @param tuple:原始String类型的一条会话流
	 */
	public void process_tuple(String tuple){
		g4jkbasicatdtbean=new Yunguan_G4JK_BasicATDTBean();
		String attr_value=new String("");
		String[] fields_set=tuple.split("\t");//按照TAB作为间隔划分字段
		if(fields_set==null){
			g4jkbasicatdtbean=null;
			return;
		}
		
		//一共五个字段
		//字段1.获取日期并做格式转换
		if(fields_set.length>0)
		{
			attr_value=TimeFormatter.Tra_realdate2(fields_set[0]);
			g4jkbasicatdtbean.setTtime(attr_value);	
		}
		
		//字段2，开关机类型
		if(fields_set.length>1){
			if(fields_set[1].length()>0)g4jkbasicatdtbean.setAttach_type(fields_set[1]);
		}
		
		//字段3，获取IMSI
		if(fields_set.length>2){
			if(fields_set[2].length()==15)g4jkbasicatdtbean.setImsi(fields_set[2].trim());
		}
		
		//字段4，获取基站TAC码，至少4位
		if(fields_set.length>3){
			if(fields_set[3].length()>=4)g4jkbasicatdtbean.setTac(fields_set[3].trim());
		}
		
		//字段5，获取基站小区码cell_id，或者填写数字，或者填写none
		if(fields_set.length>4){
			if(fields_set[4].length()>0)g4jkbasicatdtbean.setCi(fields_set[4].trim());
		}
	}
	
	//对发射出去的元组进行字段的声明
	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		//字段说明，如果execute有后续处理需求，发射后可以依赖以下字段进行标记
		outputFieldsDeclarer.declare(new Fields(
				Yunguan_G4JK_BasicATDTFields.TTIME,
				Yunguan_G4JK_BasicATDTFields.ATTACH_TYPE,
				Yunguan_G4JK_BasicATDTFields.IMSI,				
				Yunguan_G4JK_BasicATDTFields.TAC,
				Yunguan_G4JK_BasicATDTFields.CI
		));
	}

}
