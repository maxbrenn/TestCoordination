package distribution;

import java.net.InetAddress;
import java.net.UnknownHostException;


public class Node<N> {

	protected String name;
	protected InetAddress inetAddress;
	protected NodeData nodeData;

	
	
	public Node(String _name, InetAddress _inetAddress) {

		this.name = _name;
		this.inetAddress = _inetAddress;
	}

	public Node(String _name, String _inetAddressString) {

		this.name = _name;

		try {
			this.inetAddress = InetAddress.getByName(_inetAddressString);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}

	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public InetAddress getInetAddress() {
		return inetAddress;
	}

	public void setInetAddress(InetAddress inetAddress) {
		this.inetAddress = inetAddress;
	}

	public NodeData getNodeData() {
		return nodeData;
	}

	public void setNodeData(NodeData nodeData) {
		this.nodeData = nodeData;
	}

	
	
	
	

}
