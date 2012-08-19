package ibm.developerworks.article;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.norbert.cluster.javaapi.ClusterListener;
import com.linkedin.norbert.cluster.javaapi.Node;

public class ClusterMessageListener implements ClusterListener
{
   private static Logger logger = LoggerFactory.getLogger(ClusterMessageListener.class);
   
   public void handleClusterConnected(Set<Node> arg0)
   {
      logger.debug("Cluster started");
      logger.debug("This server is leader:" + PeerServer.isLeader());

   }

   public void handleClusterDisconnected()
   {
      logger.debug("Cluster disconnected");
   }

   public void handleClusterNodesChanged(Set<Node> currNodeSet)
   {
      StringBuilder lsServerList = new StringBuilder(64);
      ArrayList<Integer> serverList = new ArrayList<Integer>();

      for (Iterator iterator = currNodeSet.iterator(); iterator.hasNext();)
      {
         Node node = (Node) iterator.next();
         lsServerList.append(node.getId()).append(",");
         serverList.add(node.getId());

      }

      logger.debug("Cluster topology changed: Active server list:" + lsServerList.substring(0, lsServerList.length() - 1));
      
      MessagingClient.setCurrentNodeSet(currNodeSet);

      // update status of nodes if the current server is the leader
      if (PeerServer.isLeader())
      {


      }

   }

   public void handleClusterShutdown()
   {
      // TODO Auto-generated method stub

   }

   private static boolean isNodeInServerSet(Set<Node> currNodeSet, int serverId)
   {
      boolean retVal = false;

      for (Iterator iterator = currNodeSet.iterator(); iterator.hasNext();)
      {
         Node node = (Node) iterator.next();
         if (node.getId() == serverId)
            return true; // found server in list

      }

      return false;

   }

}
