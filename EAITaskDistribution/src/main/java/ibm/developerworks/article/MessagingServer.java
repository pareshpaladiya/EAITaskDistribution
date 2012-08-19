package ibm.developerworks.article;

import ibm.developerworks.article.Clustermessage.ClusterMessage;

import java.net.UnknownHostException;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import com.linkedin.norbert.network.javaapi.NettyNetworkServer;
import com.linkedin.norbert.network.javaapi.NetworkServer;
import com.linkedin.norbert.network.javaapi.NetworkServerConfig;

/**
 * Wraps the netty based messaging server used for intra server communications within the cluster
 * 
 * @author mukul.gupta
 * 
 */
public class MessagingServer
{
	private static NetworkServer networkServer;

	public MessagingServer()
	{

	}

	public static void init(QuorumPeerConfig config) throws UnknownHostException
	{
		if (networkServer != null)
		{
			networkServer.shutdown();
			System.out.println("Shutting down netty server to reconnect");
		}

		MessagingClient.initClient(config);

		// Now all that's left is for this server to bind itself to the right
		// node id: nodeId
		// We use the following classes in the Norbert API
		NetworkServerConfig networkConfig = new NetworkServerConfig();

		// All communication is routed through the cluster client
		networkConfig.setClusterClient(MessagingClient.getClusterClient());

		// Let's have a reasonable number of threads available to process
		// requests
		networkConfig.setRequestThreadMaxPoolSize(30);

		networkServer = new NettyNetworkServer(networkConfig);

		// Indicate what messages we want to handle and how to serialize them
		// All messages sent to this server will be
		// ns.registerHandler(new ClusterMessageHandler(), new
		// ClusterMesssageSerializer());

		// Bind our server to the node that we previously configured. This tells
		// all other cluster
		// members that the server at nodeId is ready to receive messages

		networkServer.registerHandler(ClusterMessage.getDefaultInstance(),
		      ClusterMessage.getDefaultInstance(), new ClusterMessageHandler());

		networkServer.bind((int) config.getServerId());

		MessagingClient.postInit();

	}

}
