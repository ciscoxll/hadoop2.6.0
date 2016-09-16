package cn.com.csiic.rpc;

import org.apache.hadoop.ipc.VersionedProtocol;

/**
 * 客户端和服务端共有的接口，该接口中定义的方法，会被客户端调用
 * @author Think
 *
 */
public interface MyCall extends VersionedProtocol{
	public static final long versionID = 0L;
	
	String sendHeartbeat(String msg);
}