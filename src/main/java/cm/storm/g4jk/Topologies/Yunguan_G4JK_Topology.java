package cm.storm.g4jk.Topologies;


/**
 * Created by Nick on 2016/9/1. 运管室4g与家宽网分数据实时处理
 * nicolashsu
 */
public class Yunguan_G4JK_Topology {
    /*
     * 运管室10.245.254.56，10.245.254.30，10.245.254.31 上/home/storm/netantdata/JKBR中的
     * attach_detach_info，gb_url_4g，gb_url_jk， tau_info文件夹下的数据会被flume实时获取，传给kafka，
     * 最后交给Yunguan_G4JK_Topology做实时计算 gb_url_4g 计算量很大，需要占用较多资源
     */
	
}
