package ibm.developerworks.article;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import com.linkedin.norbert.javacompat.cluster.ZooKeeperClusterClient;
import com.linkedin.norbert.javacompat.network.NettyNetworkServer;
import com.linkedin.norbert.javacompat.network.NetworkServerConfig;

/**
 * Wraps the Netty based messaging server used for intra server communications within the
 * cluster
 * 
 * @author mukul.gupta
 * @author Paresh Paladiya
 * 
 */
public class MessagingServer
{
	private static com.linkedin.norbert.javacompat.network.NetworkServer networkServer;

	private static com.linkedin.norbert.javacompat.cluster.ZooKeeperClusterClient zkClusterClient = null;

	private static Logger logr = Logger.getLogger(MessagingServer.class);

	public static void init(QuorumPeerConfig config) throws UnknownHostException
	{

		logr.info("Starting Netty Server...");

		// client (wrapper) for zookeeper server - point to local / in process
		// zookeeper server
		String lsHost = "localhost" + ":" + config.getClientPortAddress().getPort();

		zkClusterClient = new com.linkedin.norbert.javacompat.cluster.ZooKeeperClusterClient(
		      "eai_sample_service", lsHost, 20000);

		zkClusterClient.awaitConnectionUninterruptibly();
		logr.debug("Norbert server - connected to zookeeper server");

		// url - is URL for local Netty server URL
		String nettyServerURL;
		int nodeId = Server.getServerId();

		nettyServerURL = String.format("%s:%d", InetAddress.getLocalHost().getHostName(),
		      Server.getNettyServerPort());

		logr.debug("Starting Netty server:Server id=" + nodeId + " server URL ="
		      + nettyServerURL);

		zkClusterClient.removeNode(nodeId);
		zkClusterClient.addNode(nodeId, nettyServerURL);

		// add cluster listener to monitor state
		zkClusterClient.addListener(new ClusterStateListener());

		// Norbert - Netty server config
		NetworkServerConfig norbertServerConfig = new NetworkServerConfig();

		// communication via norbert zookeeper cluster client
		norbertServerConfig.setClusterClient(zkClusterClient);

		// Threads required for processing requests
		norbertServerConfig.setRequestThreadMaxPoolSize(20);

		networkServer = new NettyNetworkServer(norbertServerConfig);

		// register message handler (identifies request and response types) and the
		// corresponding object serializer for the request and response
		networkServer.registerHandler(new AppMessageHandler(), new CommonSerializer());

		networkServer.bind(Server.getServerId());
	}

	/**
	 * Returns the ZooKeeper cluster client wrapped by the messaging server
	 * 
	 * @return
	 */
	public static ZooKeeperClusterClient getZooKeeperClusterClient()
	{
		return zkClusterClient;
	}
}
