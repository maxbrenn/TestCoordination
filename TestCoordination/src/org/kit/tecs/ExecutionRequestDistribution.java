package org.kit.tecs;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;

import org.kit.tecs.data.Node;
import org.kit.tecs.data.NodeList;

public class ExecutionRequestDistribution {

	private Properties coProps;
	private Properties ycsbProps;
	private NodeList nodes;
	private ConnectionHandler conHandl;

	public ExecutionRequestDistribution(String _ycsbPropsFilePath) {
		

		coProps = parseProperties("conf/coordination.conf");

		ycsbProps = parseProperties(_ycsbPropsFilePath);

		nodes = parseNodes(ycsbProps);

		try {
			conHandl = new ConnectionHandler();
			conHandl.connect(coProps
					.getProperty("coordination.zookeeper.hosts"));
			System.out.println("connected");
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}

	}

	private void distributeNodeIdentity() {

		String stringProps = "";

		conHandl.createPath(coProps
				.getProperty("coordination.nodeidentity.folderpath"));

		for (Node node : nodes) {

			stringProps = "node.name="
					+ node.getName()
					+ ";"
					+ "distribution.folderpath.cluster="
					+ ycsbProps.getProperty("distribution.folderpath.cluster")
					+ ";"
					+ "distribution.folderpath.nodes="
					+ ycsbProps.getProperty("distribution.folderpath.nodes")
					+ ";"
					+ "distribution.folderpath.workloads="
					+ ycsbProps
							.getProperty("distribution.folderpath.workloads")
					+ ";" + "distribution.folderpath.execreq="
					+ ycsbProps.getProperty("distribution.folderpath.execreq");
			conHandl.createNode(
					coProps.getProperty("coordination.nodeidentity.folderpath")
							+ "/" + node.getInetAddress().getHostAddress(),
					stringProps, true);

		}

	}

	private void distributeExecPath() {

		String execPath = "";

		for (Node node : nodes) {

			execPath = ycsbProps.getProperty("distribution.folderpath.cluster")
					+ ycsbProps.getProperty("distribution.folderpath.nodes")
					+ "/" + node.getName()
					+ ycsbProps.getProperty("distribution.folderpath.execreq");

			conHandl.createPath(execPath);

		}

	}

	public void distributeRequest(String _operationString, String _filePath) {

		String dataString = "";
		
		try {

			BufferedReader fileReader = new BufferedReader(new FileReader(
					_filePath));
			String line = "";
			

			while (line != null) {

				line = fileReader.readLine();

				dataString = dataString + line + "\n";

				}

		} catch (IOException e) {

			e.printStackTrace();
		}

		for (Node node : nodes) {
			String execPath = ycsbProps
					.getProperty("distribution.folderpath.cluster")
					+ ycsbProps.getProperty("distribution.folderpath.nodes")
					+ "/"
					+ node.getName()
					+ ycsbProps.getProperty("distribution.folderpath.execreq");
			

			conHandl.createNode(execPath + "/" + _operationString, dataString , true);
			
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

	private NodeList parseNodes(Properties _props) {

		NodeList _nodes = new NodeList();
		String host;
		String name;

		if (_props.getProperty("nodecount") != null) {

			for (int i = 1; i <= Integer.parseInt(_props
					.getProperty("nodecount")); i++) {

				host = "";
				name = "";

				if (_props.getProperty("node." + i + ".host") != null) {

					host = _props.getProperty("node." + i + ".host");

					if (_props.getProperty("node." + i + ".name") != null) {
						name = _props.getProperty("node." + i + ".name");
					} else {
						name = "node" + i;
					}

					_nodes.add(new Node(name, host));

				} else {
					System.out.println("node." + i + ".host property not set!");
					System.exit(1);
				}

			}

		}

		return _nodes;

	}

	private void printCluster(NodeList _nodes) {
		for (Node node : _nodes) {
			System.out.println(node.getName() + " - "
					+ node.getInetAddress().getHostAddress());

		}
	}

	public static void main(String[] args) throws Exception {

		ExecutionRequestDistribution erd = new ExecutionRequestDistribution(
				args[0]);

		switch (args[1]) {

		case "init":
			System.out.println("init");
			erd.distributeNodeIdentity();
			erd.distributeExecPath();

			break;

		case "distr":
			System.out.println("distr");
			erd.distributeRequest(args[2], args[3]);
			break;

		}

	}
}