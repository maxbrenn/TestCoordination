package org.kit.tecs;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;

public class ExecutionRequestObserver implements Watcher {

	private ConnectionHandler conHandl;
	private Properties coProps;
	private Properties nodeProps;
	private String watchPath;

	public ExecutionRequestObserver() {

		coProps = parseProperties("conf/coordination.conf");

		try {
			conHandl = new ConnectionHandler();

			conHandl.connect(coProps
					.getProperty("coordination.zookeeper.hosts"));

			nodeProps = retreiveRemoteProperties();

			watchPath = nodeProps
					.getProperty("distribution.folderpath.cluster")
					+ nodeProps.getProperty("distribution.folderpath.nodes")
					+ "/"
					+ nodeProps.getProperty("node.name")
					+ nodeProps.getProperty("distribution.folderpath.execreq");

			observerExecutionRequests();

		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}

	}

	private Properties retreiveRemoteProperties() {

		String pathString = "";
		try {
			pathString = coProps
					.getProperty("coordination.nodeidentity.folderpath")
					+ "/"
					+ InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}

		String remotePropsString = conHandl.readNodeData(pathString, null);

		Properties props = parseStringProperties(remotePropsString);

		return props;
	}

	private Properties parseStringProperties(String _remotePropsString) {

		Properties props = new Properties();

		String[] subString = _remotePropsString.split("\\;");

		for (int i = 0; i < subString.length; i++) {

			if (!subString[i].equalsIgnoreCase("")) {

				String[] subSubString = subString[i].split("\\=");

				props.setProperty(subSubString[0], subSubString[1]);

			}

		}

		return props;
	}

	private InetAddress getLocalhostAddress() {

		Pattern IPv4Pattern = Pattern
				.compile("^(25[0-5]|2[0-4]\\d|[0-1]?\\d?\\d)(\\.(25[0-5]|2[0-4]\\d|[0-1]?\\d?\\d)){3}$");
		InetAddress returnAddress = null;

		try {

			Enumeration<NetworkInterface> n = NetworkInterface
					.getNetworkInterfaces();
			for (; n.hasMoreElements();) {
				NetworkInterface e = n.nextElement();

				if (e.getName().equalsIgnoreCase("eth0")) {

					Enumeration<InetAddress> a = e.getInetAddresses();
					for (; a.hasMoreElements();) {
						InetAddress address = a.nextElement();

						if (IPv4Pattern.matcher(address.getHostAddress())
								.matches()) {
							returnAddress = address;
						}
					}
				}

			}

		} catch (SocketException e) {

			e.printStackTrace();
		}

		return returnAddress;

	}

	public void observerExecutionRequests() {

		List<String> nodes = conHandl.getNodeChildren(watchPath, this);

		if (!nodes.isEmpty()) {
			for (String node : nodes) {
				System.out.println("Execution Request: " + node);
				
				
				System.out.println(conHandl.readNodeData(watchPath + "/" + node, null));
				
				conHandl.deleteNode(watchPath + "/" + node);
			}

		}

	}

	@Override
	public void process(WatchedEvent event) {
		if (event.getType() == EventType.NodeChildrenChanged) {
			
			observerExecutionRequests();
			
		}
	}

	private Properties parseProperties(String _propFilePath) {

		Properties props = new Properties();

		try {
			props.loadFromXML(new FileInputStream(_propFilePath));
		} catch (IOException e) {
			e.printStackTrace();
		}

		return props;
	}

	public static void main(String[] args) {

		ExecutionRequestObserver ero = new ExecutionRequestObserver();
		ero.observerExecutionRequests();

		try {
			Thread.sleep(Long.MAX_VALUE);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}