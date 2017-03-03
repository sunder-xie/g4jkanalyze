package cm.storm.g4jk.Topologies;

import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.spout.SchemeAsMultiScheme;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

import cm.storm.g4jk.Bolts.Yunguan_G4JK_Basic4GSplitter;
import cm.storm.g4jk.Bolts.Yunguan_G4JK_BasicTAUSplitter;
import cm.storm.g4jk.Bolts.Yunguan_G4JK_ZhWordsCountToRedis;
import cm.storm.g4jk.Bolts.Yunguan_G4JK_ZhWordsToCountBolt;
//import cm.storm.g4jk.Bolts.Yunguan_G4JK_BasicATDTSplitter;
//import cm.storm.g4jk.Bolts.Yunguan_G4JK_BasicJKSplitter;
import cm.storm.g4jk.Bolts.Yunguan_G4JK_HmapAccToRedis;
import cm.storm.g4jk.Bolts.Yunguan_G4JK_HspAccToRedis;
import cm.storm.g4jk.Bolts.Yunguan_G4JK_SJJS093_93ToRedis;
//import cm.storm.g4jk.Bolts.Yunguan_G4JK_SJJS208_88ToRedis;
//import cm.storm.g4jk.Bolts.Yunguan_G4JK_SJJS245_87ToRedis;
import cm.storm.g4jk.Bolts.Yunguan_G4JK_TauAccToRedis;


/**
 * Created by Nick on 2016/9/1. 运管室4g与家宽网分数据实时处理
 * @author chinamobile 
 */
public class Yunguan_G4JK_Topology {
    /*
     * 运管室10.245.254.56，10.245.254.30，10.245.254.31 上/home/storm/netantdata/JKBR中的
     * attach_detach_info，gb_url_4g，gb_url_4g_2，gb_url_4g_3，gb_url_jk， tau_info文件夹下的数据会被flume实时获取，传给kafka，
     * 最后交给Yunguan_G4JK_Topology做实时计算 gb_url_4g 计算量很大，需要占用较多资源
     */
    public static void main(String[] args) {
		//Storm kafka Topology的创建是作为kafka consumer的进行相关的配置
	    //需要做的事情有两件，第一是明确Spout的类型，第二是明确bolt做什么处理，最终形成本段代码中的整个拓扑。
	    //其中Spout有Storm core和Trident两种类型，trident是流的批处理，确保每个消息被处理一次，保证事物型数据存储的持久化，以及支持流分析。在今后有使用时可以关注
	    
    	/*kafka spout配置开始*/
	    //brokerHosts用以获取Kafka broker和partition信息所在的zookeeper集群服务器ip和端口
		BrokerHosts brokerHosts = new ZkHosts("10.245.254.56:2181,10.245.254.30:2181,10.245.254.31:2181");
		
		//配置Storm Kafka订阅的相关信息参数
	    //brokerHosts：用以获取Kafka broker和partition的信息
	    //topic：参数用于标识kafka上所要读取的消息所在的topic，4G网分数据wf4g，wfjk，wftau，wfatdt
	    //zkroot：用Zookeeper来记录KafkaSpout的处理进度，在topology重新提交或者task重启后继续之前的处理进度。
	    //id：如果想要一个topology从另一个topology之前的处理进度继续处理，它们需要有相同的id，这个相当于kafka consumer在zookeeper上的id，存放在zkroot之下。
		SpoutConfig spoutConfigwf4g = new SpoutConfig(brokerHosts, "wf4g", "/kafkaspoutwf4g", "kafkaspoutwf4g");
	    SpoutConfig spoutConfigwfjk = new SpoutConfig(brokerHosts, "wfjk", "/kafkaspoutwfjk", "kafkaspoutwfjk");
	    SpoutConfig spoutConfigwftau = new SpoutConfig(brokerHosts, "wftau", "/kafkaspoutwftau", "kafkaspoutwftau");
	    SpoutConfig spoutConfigwfatdt = new SpoutConfig(brokerHosts, "wfatdt", "/kafkaspoutwfatdt", "kafkaspoutwfatdt");
	    
	    //MultiScheme是一个接口，用于规定将从kafka获取的byte[]转换成 storm元组格式，
	    //storm-kafka实现了StringScheme，KeyValueStringScheme等等Scheme，这些Scheme主要负责从消息流中解析出所需要的数据格式信息。
	    //用户也可以继承Schema接口，自定义对应的消息流处理Scheme，从消息流中解析出所要的元组信息，最后构成元组。
	    //具体参考链接：https://storm.apache.org/javadoc/apidocs/backtype/storm/spout/Scheme.html，涉及的具体接口方法是deserialize(byte[] ser)和getOutputFields()
	    //可以在deserialize中将byte[]消息转成对应的字符格式流，并利用getOutputFields命名好sheild区域，比如StringScheme的命名区域是str，将bytes转成string
	    //默认会将字符串转成UTF8
	    spoutConfigwf4g.scheme = new SchemeAsMultiScheme(new StringScheme()); 
	    spoutConfigwfjk.scheme = new SchemeAsMultiScheme(new StringScheme());
	    spoutConfigwftau.scheme = new SchemeAsMultiScheme(new StringScheme()); 
	    spoutConfigwfatdt.scheme = new SchemeAsMultiScheme(new StringScheme());
	    
	    // 从 kafka 的最新记录读取，设置如下 -1：最新记录，-2：最老记录，注释掉：默认从 zk 读取
	 	// 避免 kafka.common.OffsetOutOfRangeException 问题
	    spoutConfigwf4g.ignoreZkOffsets=true;
	    spoutConfigwf4g.startOffsetTime=-1;
	    spoutConfigwfjk.ignoreZkOffsets=true;
	    spoutConfigwfjk.startOffsetTime=-1;
	    spoutConfigwftau.ignoreZkOffsets=true;
	    spoutConfigwftau.startOffsetTime=-1;
	    spoutConfigwfatdt.ignoreZkOffsets=true;
	    spoutConfigwfatdt.startOffsetTime=-1;
	
	    //接着是实例化spout，
	    //可以参考链接https://storm.apache.org/javadoc/apidocs/backtype/storm/spout/ISpout.html
	    //程序员可以利用其中的nextTuple()方法对已有的元组数据进行进一步的字段筛选，处理，然后交给collector进行发射emit，也可以将这个预处理过程添加到对应的Bolt中的excute方法来进行，具体针对需求而定
	    KafkaSpout kspout_wf4g = new KafkaSpout(spoutConfigwf4g);
	    KafkaSpout kspout_wftau = new KafkaSpout(spoutConfigwftau);
//	    KafkaSpout kspout_wfjk = new KafkaSpout(spoutConfigwfjk);
//	    KafkaSpout kspout_wfatdt = new KafkaSpout(spoutConfigwfatdt);
	    /*kafka spout配置结束*/
	    
	    /*拓扑Topology设置开始*/
        TopologyBuilder Tpbuilder = new TopologyBuilder();
        //设置spout，在storm运行过程中对应的id，来源的kafka，以及对应所需的slot数量
        Tpbuilder.setSpout("Spoutwf4g", kspout_wf4g, 3);
        Tpbuilder.setSpout("Spoutwftau", kspout_wftau, 1);
//      Tpbuilder.setSpout("Spoutwfjk", kspout_wfjk, 1);
//      Tpbuilder.setSpout("Spoutwfatdt", kspout_wfatdt, 1);
        
        //设置bolt
        //初始bolt分别spout的流进行字段解析
        Tpbuilder.setBolt("SplitterBoltwf4g", new Yunguan_G4JK_Basic4GSplitter(),3).shuffleGrouping("Spoutwf4g");
        Tpbuilder.setBolt("SplitterBoltwftau", new Yunguan_G4JK_BasicTAUSplitter(),1).shuffleGrouping("Spoutwftau");
//      Tpbuilder.setBolt("SplitterBoltwfjk", new Yunguan_G4JK_BasicJKSplitter(),1).shuffleGrouping("Spoutwfjk");
//      Tpbuilder.setBolt("SplitterBoltwfatdt", new Yunguan_G4JK_BasicATDTSplitter(),1).shuffleGrouping("Spoutwfatdt");
        
        //业务bolt，关于4G网分数据的实时解析，
        //统计热点区域人流量，标签，上网标签人数，热点区域流量数据，15分钟累计一次，将数据直接入库redis
        //基站周边人数，流量累计，15分钟累计一次，将数据直接入库redis
        //统计TAU中的人流量信息补充，15分钟累计一次，将数据直接入库redis
        Tpbuilder.setBolt("HspAccBoltwfg4", new Yunguan_G4JK_HspAccToRedis(),22).shuffleGrouping("SplitterBoltwf4g");
        Tpbuilder.setBolt("HmapAccBoltwfg4", new Yunguan_G4JK_HmapAccToRedis(),30).shuffleGrouping("SplitterBoltwf4g");
        Tpbuilder.setBolt("TauAccBoltwfg4", new Yunguan_G4JK_TauAccToRedis(),6).shuffleGrouping("SplitterBoltwftau");
        Tpbuilder.setBolt("SJJS093Bolt93wfg4", new Yunguan_G4JK_SJJS093_93ToRedis(),12).shuffleGrouping("SplitterBoltwf4g");
//        Tpbuilder.setBolt("SJJS208Bolt88wfg4", new Yunguan_G4JK_SJJS208_88ToRedis(),12).shuffleGrouping("SJJS093Bolt93wfg4");
        Tpbuilder.setBolt("ZhToCountBoltwfg4", new Yunguan_G4JK_ZhWordsToCountBolt(),6).shuffleGrouping("SplitterBoltwf4g");
        Tpbuilder.setBolt("ZhCountBoltwfg4", new Yunguan_G4JK_ZhWordsCountToRedis(),6).shuffleGrouping("ZhToCountBoltwfg4");
        /*拓扑执行*/
        //Configuration
  		Config conf = new Config();
  		//conf.put("wordsFile", args[0]);
  		conf.setDebug(false);
  		conf.put(Config.TOPOLOGY_ACKER_EXECUTORS,0);//防止出现大量重传的最后手段，其余处理效率问题需要通过调整代码，添加并行数量来提升效率
  		conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 300); //将对应的延迟时间延长，防止大量fail出现，造成大量元组的重发送
  		//conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
  		
  		// 执行 Topology
  		try{
  			if(args!=null && args[0]!=null && args[0].trim().toLowerCase().equals("one"))
  			{
  				//集群模式
  				StormSubmitter.submitTopology("Count-YunguanG4JK-Topology", conf,
  						Tpbuilder.createTopology());
  			}
  			else if(args!=null && args[0]!=null && args[0].trim().toLowerCase().equals("test"))
  			{
  				//本地模式，测试
  				LocalCluster cluster = new LocalCluster();
  				cluster.submitTopology("Count-YunguanG4JK-Topology-With-LocalCluster", conf,
  						Tpbuilder.createTopology());
  				Thread.sleep(2000);
  				cluster.shutdown();
  			}
  			else
  			{
  				//集群模式
  				int cnt=-1;
  				try{ cnt=Integer.parseInt(args[0]); }catch(Exception exc){}
  				if(cnt<0)
  					cnt=6;			// 默认值
  				
  				conf.put(Config.TOPOLOGY_WORKERS,cnt);
  				//conf.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE, 16); 								// default is 8
  				//conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384); 		// batched; default is 1024
  				//conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 16384); 			// individual tuples; default is 1024

  				StormSubmitter.submitTopology("Count-YunguanG4JK-Topology", conf,
  						Tpbuilder.createTopology());
  			}
  		}catch(Exception exc)
  		{
  			exc.printStackTrace();
  		}
  		/*拓扑执行结束*/
	    
    }
	
}
