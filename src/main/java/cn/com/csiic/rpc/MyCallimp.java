package cn.com.csiic.rpc;

import java.io.IOException;

import org.apache.hadoop.ipc.ProtocolSignature;




public class MyCallimp implements MyCall{

	@Override
	public long getProtocolVersion(String protocol, long clientVersion)
			throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public ProtocolSignature getProtocolSignature(String protocol,
			long clientVersion, int clientMethodsHash) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String sendHeartbeat(String msg) {
		// TODO Auto-generated method stub
		return null;
	}

	

	
	
}
