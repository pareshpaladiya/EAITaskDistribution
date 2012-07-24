package ibm.developerworks.article;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;

import org.apache.commons.configuration.Configuration;
import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeer;

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

   private static ServerClusterConfig localServerConfig = null;
   
   public static PeerServer instance = null;

   /**
    * Initializes the server
    * @param foServerClusterConfig
    * @param miServerId
    * @throws Exception 
    */
   public static void init(Configuration foServerClusterConfig, int miServerId) throws Exception
   {
      
         initConfig(foServerClusterConfig,miServerId);  
         

         if (instance ==null)
         {
         	instance = new PeerServer();
         }
         
         RunQuorumPeer loLocalServer = instance.new RunQuorumPeer(localServerConfig);
         loLocalServer.start();
         
         
         
         initMessagingServer();
                
   }

   private static void initConfig(Configuration foServerClusterConfig, int miServerId) throws Exception
   {
      localServerConfig = new ServerClusterConfig(miServerId);
      localServerConfig.init(foServerClusterConfig);
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

   private static void runFromConfig(ServerClusterConfig config) throws IOException
   {
      try
      {
      	
      	NIOServerCnxn.Factory cnxnFactory = new NIOServerCnxn.Factory(config.getClientPortAddress(),
               config.getMaxClientCnxns());
      	
        
         

         quorumPeer = new QuorumPeer();
         quorumPeer.setClientPortAddress(config.getClientPortAddress());
         quorumPeer.setTxnFactory(new FileTxnSnapLog( new File(config
               .getDataDir()), new File(config.getDataLogDir()) ));
         quorumPeer.setQuorumPeers(config.getServers());
         quorumPeer.setElectionType(config.getElectionAlg());
         quorumPeer.setMyid(config.getServerId());
         quorumPeer.setTickTime(config.getTickTime());
         quorumPeer.setMinSessionTimeout(config.getMinSessionTimeout());
         quorumPeer.setMaxSessionTimeout(config.getMaxSessionTimeout());
         quorumPeer.setInitLimit(config.getInitLimit());
         quorumPeer.setSyncLimit(config.getSyncLimit());
         quorumPeer.setQuorumVerifier(config.getQuorumVerifier());
         
         // server cluster will be up even if a single server is all that is functioning   
         //quorumPeer.setQuorumVerifier(new AdvSingleServerQuorumVerifier());
         quorumPeer.setCnxnFactory(cnxnFactory);
         quorumPeer.setZKDatabase(new ZKDatabase(quorumPeer.getTxnFactory()));
         quorumPeer.setLearnerType(config.getPeerType());

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


   public static ServerClusterConfig getConfig()
   {
      return localServerConfig;
   }

   public static void setConfig(ServerClusterConfig foconfig)
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

   	ServerClusterConfig serverconfig;

		public RunQuorumPeer(ServerClusterConfig foconfig) {
			serverconfig = foconfig;
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
