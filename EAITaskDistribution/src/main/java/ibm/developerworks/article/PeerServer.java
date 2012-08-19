package ibm.developerworks.article;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;

import org.apache.commons.configuration.Configuration;
import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

/**
 * This is singleton server class. This class must be initialized before
 * starting any remote communication.
 * 
 * @author Mukul Gupta
 * @author Paresh Paladiya
 *
 */
public class PeerServer
{
   private static QuorumPeer quorumPeer = null;

   private static QuorumPeerConfig localServerConfig = null;
   
   public static PeerServer instance = null;

   /**
    * Initializes the server
    * @param foServerClusterConfig
    * @param miServerId
    * @throws Exception 
    */
   public static void init(int miServerId) throws Exception
   {
      
         initConfig(miServerId);  
         

         if (instance ==null)
         {
         	instance = new PeerServer();
         }
         
         RunQuorumPeer loLocalServer = instance.new RunQuorumPeer(localServerConfig);
         loLocalServer.start();
         
         initMessagingServer();
                
   }

   private static void initConfig(int miServerId) throws Exception
   {
      localServerConfig = new QuorumPeerConfig();
      localServerConfig.parse(instance.getClass().getClassLoader().getResource("server" + miServerId + ".cfg").toString());
   }

   /**
    * Initialize message handling interfaces
    * @throws UnknownHostException
    */
   private static void initMessagingServer() throws UnknownHostException
   {
      //TODO Norbert implementation names get from config 
      MessagingServer.init(localServerConfig);
   }

   private static void runFromConfig(QuorumPeerConfig serverconfig) throws IOException
   {
      try
      {
      	NIOServerCnxn.Factory cnxnFactory = new NIOServerCnxn.Factory(serverconfig.getClientPortAddress(),
               serverconfig.getMaxClientCnxns());
      	 

         quorumPeer = new QuorumPeer();
         quorumPeer.setClientPortAddress(serverconfig.getClientPortAddress());
         quorumPeer.setTxnFactory(new FileTxnSnapLog( new File(serverconfig
               .getDataDir()), new File(serverconfig.getDataLogDir()) ));
         quorumPeer.setQuorumPeers(serverconfig.getServers());
         quorumPeer.setElectionType(serverconfig.getElectionAlg());
         quorumPeer.setMyid(serverconfig.getServerId());
         quorumPeer.setTickTime(serverconfig.getTickTime());
         quorumPeer.setMinSessionTimeout(serverconfig.getMinSessionTimeout());
         quorumPeer.setMaxSessionTimeout(serverconfig.getMaxSessionTimeout());
         quorumPeer.setInitLimit(serverconfig.getInitLimit());
         quorumPeer.setSyncLimit(serverconfig.getSyncLimit());
         quorumPeer.setQuorumVerifier(serverconfig.getQuorumVerifier());
         quorumPeer.setCnxnFactory(cnxnFactory);
         quorumPeer.setZKDatabase(new ZKDatabase(quorumPeer.getTxnFactory()));
         quorumPeer.setLearnerType(serverconfig.getPeerType());

         quorumPeer.start();        
         
         quorumPeer.join();
      }
      catch (Exception e)
      {
         // warn, but generally this is ok
         // TODO log this
         // LOG.warn("Quorum Peer interrupted", e);
      }
   }

   public static QuorumPeer getQuorumPeer()
   {
      return quorumPeer;
   }

   public static void setQuorumPeer(QuorumPeer foquorumPeer)
   {
      quorumPeer = foquorumPeer;
   }


   public static QuorumPeerConfig getConfig()
   {
      return localServerConfig;
   }

   public static void setConfig(QuorumPeerConfig foconfig)
   {
      localServerConfig = foconfig;
   }

   public static boolean isLeader()
   {
   	if (quorumPeer !=null)
   	{
   		return (quorumPeer.leader!=null);
   	}
   	
   	return false;
   }
   
   
   public class RunQuorumPeer extends Thread {

      QuorumPeerConfig serverconfig;

		public RunQuorumPeer(QuorumPeerConfig localServerConfig) {
			serverconfig = localServerConfig;
		}

		@Override
		public void run() {
			super.run();
			try {
				runFromConfig(serverconfig);
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

	}
}
