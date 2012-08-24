package ibm.developerworks.article;

import java.util.Set;

import org.apache.log4j.Logger;

import com.linkedin.norbert.javacompat.cluster.ClusterListener;
import com.linkedin.norbert.javacompat.cluster.Node;

//import com.linkedin.norbert.cluster.javaapi.ClusterListener;
//import com.linkedin.norbert.cluster.javaapi.Node;

public class ClusterStateListener implements ClusterListener
{
	private static Logger logr = Logger.getLogger(ClusterStateListener.class);

	public void handleClusterConnected(Set<Node> arg0)
	{
		logr.info("Server[id= " + Server.getServerId()
		      + "] connected to server cluster...");

	}

	public void handleClusterDisconnected()
	{
		logr.info("Server[id= " + Server.getServerId()
		      + "] disconnected from server cluster...");
	}

	public void handleClusterNodesChanged(Set<Node> currNodeSet)
	{
		logr.debug("Cluster topology changed...");

		MessagingClient.updateCurrentNodeSet(currNodeSet);

		// update status of nodes if the current server is the leader
		if (Server.isLeader() && !Server.isFilePollerRunning())
		{
         Server.startFilePoller();
		}

	}

	public void handleClusterShutdown()
	{
		logr.info("Server[id= " + Server.getServerId()
		      + "] view.  Cluster showdown");

	}

	

}