package cm.storm.g4jk.Topologies;

import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.ZkHosts;



/**
 * Created by Nick on 2016/9/1. 运管室4g与家宽网分数据实时处理
 * nicolashsu
 */
public class Yunguan_G4JK_Topology {
    /*
     * 运管室10.245.254.56，10.245.254.30，10.245.254.31 上/home/storm/netantdata/JKBR中的
     * attach_detach_info，gb_url_4g，gb_url_4g_2，gb_url_4g_3，gb_url_jk， tau_info文件夹下的数据会被flume实时获取，传给kafka，
     * 最后交给Yunguan_G4JK_Topology做实时计算 gb_url_4g 计算量很大，需要占用较多资源
     */
	//Storm kafka Topology的创建是作为kafka consumer的进行相关的配置
    //需要做的事情有两件，第一是明确Spout的类型，第二是明确bolt做什么处理，最终形成本段代码中的整个拓扑。
    //其中Spout有Storm core和Trident两种类型，trident是流的批处理，确保每个消息被处理一次，保证事物型数据存储的持久化，以及支持流分析。在今后有使用时可以关注
    /*kafka spout配置开始*/
	
    //brokerHosts用以获取Kafka broker和partition信息所在的zookeeper集群服务器ip和端口
	BrokerHosts brokerHosts = new ZkHosts("10.245.254.56:2181,10.245.254.30:2181,10.245.254.31:2181");
	
	
	
}
