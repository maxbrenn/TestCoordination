package distribution;

import java.nio.charset.Charset;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class ConnectionHandler extends Connector {

	private static final Charset CHARSET = Charset.forName("UTF-8");

	public void writeNodeData(String _path, String _value) {

		Stat stat;

		try {

			stat = zk.exists(_path, false);

			if (stat == null) {
				zk.create(_path, _value.getBytes(CHARSET), Ids.OPEN_ACL_UNSAFE,
						CreateMode.PERSISTENT);
			} else {
				zk.setData(_path, _value.getBytes(CHARSET), -1);
			}

		} catch (KeeperException | InterruptedException e) {

			e.printStackTrace();
		}

	}

	public String readNodeData(String _path, Watcher _watcher) {

		byte[] data;

		try {

			data = zk.getData(_path, _watcher, null);
			return new String(data, CHARSET);

		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
			return null;
		}

	}

	public List<String> getNodeChildren(String _path, Watcher _watcher) {

		try {

			return zk.getChildren(_path, _watcher);

		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
			return null;
		}

	}

	public void createNode(String _path, String _value , boolean _isOverwrite) {

		try {
			
			if(zk.exists(_path, null)==null) {
				zk.create(_path, _value.getBytes(CHARSET), Ids.OPEN_ACL_UNSAFE,
						CreateMode.PERSISTENT);
				
			} else {
				if (_isOverwrite) {
					zk.setData(_path, _value.getBytes(CHARSET), -1);
				}
			}

			

		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void deleteNode(String _path) {

		try {

			zk.delete(_path, -1);

		} catch (InterruptedException | KeeperException e) {

			e.printStackTrace();
		}

	}

	public void createPath(String _path) {

		String[] subPath = _path.split("\\/");
		String createPath = "";

		for (int i = 1; i < subPath.length; i++) {

			createPath = createPath + "/" + subPath[i];

			try {
				if (zk.exists(createPath, false) == null) {
					zk.create(createPath, null, Ids.OPEN_ACL_UNSAFE,
							CreateMode.PERSISTENT);

					System.out.println("created node: " + createPath);
				}
			} catch (KeeperException | InterruptedException e) {
				e.printStackTrace();
			}

		}

	}

}