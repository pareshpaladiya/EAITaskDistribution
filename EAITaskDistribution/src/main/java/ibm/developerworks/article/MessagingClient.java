package ibm.developerworks.article;

import ibm.developerworks.article.Clustermessage.ClusterMessage;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Message;
import com.linkedin.norbert.cluster.javaapi.Node;
import com.linkedin.norbert.cluster.javaapi.ZooKeeperClusterClient;
import com.linkedin.norbert.network.javaapi.NettyNetworkClient;
import com.linkedin.norbert.network.javaapi.NetworkClientConfig;
import com.linkedin.norbert.network.javaapi.RoundRobinLoadBalancerFactory;

/**
 * Wraps the netty based norbert client for this server (one per JVM)
 * 
 * @author mukul.gupta
 * 
 */
public class MessagingClient
{
   
   private static Logger logger = LoggerFactory.getLogger(MessagingClient.class);
         
	public static final String zooKeeperServiceName = "norbert";

	private static ZooKeeperClusterClient clusterClient = null;

	private static NettyNetworkClient nettyClient;

	private static Set<Node> CURRENT_NODES = null;

	/*
	 * need a map of current nodes since they might be accessed a lot. Represents the same set as
	 * <code>CURRENT_NODES</code>
	 */
	private static ConcurrentHashMap<Integer, Node> CURR_NODE_MAP = new ConcurrentHashMap<Integer, Node>(5);

	public MessagingClient()
	{

	}

	public static void initClient(QuorumPeerConfig config) throws UnknownHostException
	{

		if (clusterClient != null)
		{
			clusterClient.shutdown();

			System.out.println("Shutting down zookeeper netty client to reconnect");
		}

		// TODO -debug
		System.out.println("Starting Adv netty client");

		String lsHost = "127.0.0.1" + ":" + config.getClientPortAddress().getPort();
		// String lsHost = config.getClientPortAddress().toString();

		// clusterClient = new ZooKeeperClusterClient(zooKeeperServiceName, lsHost, config.getMaxSessionTimeout());

		clusterClient = new ZooKeeperClusterClient(zooKeeperServiceName, lsHost, 60000);

		clusterClient.awaitConnectionUninterruptibly();
		logger.debug("Started Netty Client:");

		String url;
		int nodeId = (int) config.getServerId();
		// We need to let the other cluster members know how to reach us
		// By default we use the hostname:port as the connection url
		try
		{
			url = String.format("%s:%d", InetAddress.getLocalHost().getHostName(), Utils.getProperty("messageServerPort"));
		}
		catch (UnknownHostException e)
		{
			throw new RuntimeException("Server cannot start as it cannot find its hostname");
		}
		// Update the cluster configuration
		clusterClient.removeNode(nodeId);
		clusterClient.addNode(nodeId, url);

		// add cluster listener
		clusterClient.addListener(new ClusterMessageListener());

		logger.debug("Netty client started");
	}

	public static void postInit()
	{
		if (nettyClient != null)
		{
			nettyClient.shutdown();

			System.out.println("Shutting down netty cluster client to reconnect");
		}

		NetworkClientConfig config = new NetworkClientConfig();
		config.setClusterClient(clusterClient);

		nettyClient = new NettyNetworkClient(config, new RoundRobinLoadBalancerFactory());
		
		/*
		 * all the internal message are serialized onto ProcessMessage.AdvMessage and then deserialized
		 * on the receiving end
		 */
		nettyClient.registerRequest(ClusterMessage.getDefaultInstance(),
		      ClusterMessage.getDefaultInstance());

		logger.debug("Netty client started");
	}


	public static ZooKeeperClusterClient getClusterClient()
	{
		return clusterClient;
	}

	public static NettyNetworkClient getMessageClient()
	{
		return nettyClient;
	}

	/**
	 * Called from cluster listener. Sets the current node set as well as make the same available for quick lookup in the
	 * corresponding map
	 * 
	 * @param foCurrentNodes
	 */
	public static void setCurrentNodeSet(Set<Node> foCurrentNodes)
	{
		//
		synchronized (MessagingClient.class)
		{
			CURRENT_NODES = foCurrentNodes;

			// will put in new ones
			CURR_NODE_MAP.clear();

			// TODO - fix debugging

			logger.debug("New Node list:" + foCurrentNodes.size() + " size");

			for (Iterator iterator = foCurrentNodes.iterator(); iterator.hasNext();)
			{
				Node node = (Node) iterator.next();
				CURR_NODE_MAP.put(node.getId(), node);
				logger.debug("Node active:" + node.getId() + " URL " + node.getUrl());

			}

			logger.debug("This server is leader in server cluster:" + PeerServer.isLeader());

		}
	}

	// public static void setClusterClient(ZooKeeperClusterClient foclusterClient)
	// {
	// clusterClient = foclusterClient;
	// }

	/**
	 * Checks to see if a specific port is available.
	 * 
	 * @param port
	 *           the port to check for availability
	 */
	public static boolean available(int port)
	{

		ServerSocket ss = null;
		DatagramSocket ds = null;
		try
		{
			ss = new ServerSocket(port);
			ss.setReuseAddress(true);
			ds = new DatagramSocket(port);
			ds.setReuseAddress(true);
			return true;
		}
		catch (IOException e)
		{
		}
		finally
		{
			if (ds != null)
			{
				ds.close();
			}

			if (ss != null)
			{
				try
				{
					ss.close();
				}
				catch (IOException e)
				{
					/* should not be thrown */
				}
			}
		}

		return false;
	}


	/**
	 * Sends passed message to the specified server; if server id is < 0 then send to a server in the cluster using round
	 * robin selection for the destination server
	 * 
	 * @param messg
	 *           the message to be sent
	 * @param serverId
	 *           the server id to which the message should be sent
	 * @return
	 * @throws Exception
	 *            If destination server is not available
	 * 
	 */
	public static Future<Message> sendMessage(ClusterMessage messg, int serverId) throws Exception
	{
	   logger.debug("Sending message to Server: " + serverId);
		
		if (serverId <= 0)
		{
			return nettyClient.sendMessage(messg);

		}
		else
		{
			Node destNode = CURR_NODE_MAP.get(serverId);
			if (destNode != null)
			{
				return nettyClient.sendMessageToNode(messg, destNode);
			}
			// TODO - implement - log the fact that the identified destination node is not available

			throw new Exception("Could not send message." + " Destination server[id]=" + serverId + " is not avaialble...");
		}

	}
}
