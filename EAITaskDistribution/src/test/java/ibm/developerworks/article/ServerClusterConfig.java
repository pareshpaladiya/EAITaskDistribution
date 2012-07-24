package ibm.developerworks.article;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.configuration.Configuration;
import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.flexible.QuorumHierarchical;

public class ServerClusterConfig extends QuorumPeerConfig
{
	int messagingServerPort = 0;

	public ServerClusterConfig(int miServerId)
	{
		serverId = miServerId;
	}

	/**
	 * Returns the messaging server (netty) port number
	 * 
	 * @return
	 */
	public int getMessagingServerPort()
	{
		return messagingServerPort;
	}

	/**
	 * Returns the messaging server (netty) port number
	 * 
	 * @return
	 */
	public int getNettyPort()
	{
		return messagingServerPort;
	}

	public void init(Configuration foConfig) throws Exception
	{
		int liNumberOfServers = foConfig.getInt("server.count");

		// int serverId = foConfig.getInt("server.current.id");

		Properties loServerProps = new Properties();

		Properties loProps1 = GeneralUtils.getProperties("server." + serverId, foConfig, true);
		GeneralUtils.addPropsFromSourceToTarget(loProps1, loServerProps);

		// need to extract messaging port here
		messagingServerPort = Integer.valueOf(loProps1.getProperty("messageServerPort"));

		for (int i = 1; i <= liNumberOfServers; i++)
		{
			String lsPropName = "server." + i;
			String lsValue = (String) foConfig.getProperty(lsPropName);
			loServerProps.setProperty(lsPropName, lsValue.trim());
		}

//		/*
//		 * create myid file
//		 */
//		File loMyFile = new File(loServerProps.getProperty("dataDir").toString(), "myid");
//
//		FileUtils.deleteQuietly(loMyFile);
//		FileUtils.writeStringToFile(loMyFile, String.valueOf(serverId));

		// parse properties
		parseConfigProperties(loServerProps);

	}

	/**
	 * Parse config from a Properties.
	 * 
	 * @param zkProp
	 *           Properties to parse from.
	 * @throws IOException
	 * @throws ConfigException
	 */
	private void parseConfigProperties(Properties zkProp) throws IOException, ConfigException
	{
		int clientPort = 0;
		String clientPortAddress = null;
		for (Entry<Object, Object> entry : zkProp.entrySet())
		{
			String key = entry.getKey().toString().trim();
			String value = entry.getValue().toString().trim();
			if (key.equals("dataDir"))
			{
				dataDir = value;
			}
			else if (key.equals("dataLogDir"))
			{
				dataLogDir = value;
			}
			else if (key.equals("clientPort"))
			{
				clientPort = Integer.parseInt(value);
			}
			else if (key.equals("clientPortAddress"))
			{
				clientPortAddress = value.trim();
			}
			else if (key.equals("tickTime"))
			{
				tickTime = Integer.parseInt(value);
			}
			else if (key.equals("maxClientCnxns"))
			{
				maxClientCnxns = Integer.parseInt(value);
			}
			else if (key.equals("minSessionTimeout"))
			{
				minSessionTimeout = Integer.parseInt(value);
			}
			else if (key.equals("maxSessionTimeout"))
			{
				maxSessionTimeout = Integer.parseInt(value);
			}
			else if (key.equals("initLimit"))
			{
				initLimit = Integer.parseInt(value);
			}
			else if (key.equals("syncLimit"))
			{
				syncLimit = Integer.parseInt(value);
			}
			else if (key.equals("electionAlg"))
			{
				electionAlg = Integer.parseInt(value);
			}
			else if (key.equals("peerType"))
			{
				if (value.toLowerCase().equals("observer"))
				{
					peerType = LearnerType.OBSERVER;
				}
				else if (value.toLowerCase().equals("participant"))
				{
					peerType = LearnerType.PARTICIPANT;
				}
				else
				{
					throw new ConfigException("Unrecognised peertype: " + value);
				}
			}
			else if (key.startsWith("server."))
			{
				int dot = key.indexOf('.');
				long sid = Long.parseLong(key.substring(dot + 1));
				String parts[] = value.split(":");
				if ((parts.length != 2) && (parts.length != 3) && (parts.length != 4))
				{
					// TODO - log fix
					// LOG.error(value
					// + " does not have the form host:port or host:port:port " +
					// " or host:port:port:type");
				}
				InetSocketAddress addr = new InetSocketAddress(parts[0], Integer.parseInt(parts[1]));
				if (parts.length == 2)
				{
					servers.put(Long.valueOf(sid), new QuorumServer(sid, addr));
				}
				else if (parts.length == 3)
				{
					InetSocketAddress electionAddr = new InetSocketAddress(parts[0], Integer.parseInt(parts[2]));
					servers.put(Long.valueOf(sid), new QuorumServer(sid, addr, electionAddr));
				}
				else if (parts.length == 4)
				{
					InetSocketAddress electionAddr = new InetSocketAddress(parts[0], Integer.parseInt(parts[2]));
					LearnerType type = LearnerType.PARTICIPANT;
					if (parts[3].toLowerCase().equals("observer"))
					{
						type = LearnerType.OBSERVER;
						observers.put(Long.valueOf(sid), new QuorumServer(sid, addr, electionAddr, type));
					}
					else if (parts[3].toLowerCase().equals("participant"))
					{
						type = LearnerType.PARTICIPANT;
						servers.put(Long.valueOf(sid), new QuorumServer(sid, addr, electionAddr, type));
					}
					else
					{
						throw new ConfigException("Unrecognised peertype: " + value);
					}
				}
			}
			else if (key.startsWith("group"))
			{
				int dot = key.indexOf('.');
				long gid = Long.parseLong(key.substring(dot + 1));

				numGroups++;

				String parts[] = value.split(":");
				for (String s : parts)
				{
					long sid = Long.parseLong(s);
					if (serverGroup.containsKey(sid))
						throw new ConfigException("Server " + sid + "is in multiple groups");
					else
						serverGroup.put(sid, gid);
				}

			}
			else if (key.startsWith("weight"))
			{
				int dot = key.indexOf('.');
				long sid = Long.parseLong(key.substring(dot + 1));
				serverWeight.put(sid, Long.parseLong(value));
			}
			else
			{
				System.setProperty("zookeeper." + key, value);
			}
		}

		if (dataDir == null)
		{
			throw new IllegalArgumentException("dataDir is not set");
			
		
		}
		if (dataLogDir == null)
		{
			dataLogDir = dataDir;
		}
		else
		{
			if (!new File(dataLogDir).isDirectory())
			{
				throw new IllegalArgumentException("dataLogDir " + dataLogDir + " is missing.");
			}
		}
		
		//clear data and log dirs
		GeneralUtils.deleteFilesInDir(dataDir);
		GeneralUtils.deleteFilesInDir(dataLogDir);
		
		if (clientPort == 0)
		{
			throw new IllegalArgumentException("clientPort is not set");
		}
		if (clientPortAddress != null)
		{
			this.clientPortAddress = new InetSocketAddress(InetAddress.getByName(clientPortAddress), clientPort);
		}
		else
		{
			this.clientPortAddress = new InetSocketAddress(clientPort);
		}

		if (tickTime == 0)
		{
			throw new IllegalArgumentException("tickTime is not set");
		}
		if (minSessionTimeout > maxSessionTimeout)
		{
			throw new IllegalArgumentException("minSessionTimeout must not be larger than maxSessionTimeout");
		}
		if (servers.size() == 0)
		{
			if (observers.size() > 0)
			{
				throw new IllegalArgumentException("Observers w/o participants is an invalid configuration");
			}
			// Not a quorum configuration so return immediately - not an error
			// case (for b/w compatibility), server will default to standalone
			// mode.
			return;
		}
		// else if (servers.size() == 1) {
		// if (observers.size() > 0) {
		// throw new IllegalArgumentException("Observers w/o quorum is an invalid configuration");
		// }
		//
		// // HBase currently adds a single server line to the config, for
		// // b/w compatibility reasons we need to keep this here.
		// LOG.error("Invalid configuration, only one server specified (ignoring)");
		// servers.clear();
		// }
		else if (servers.size() > 1)
		{
			if (servers.size() == 2)
			{
				// LOG.warn("No server failure will be tolerated. " +
				// "You need at least 3 servers.");
			}
			else if (servers.size() % 2 == 0)
			{
				// LOG.warn("Non-optimial configuration, consider an odd number of servers.");
			}
			if (initLimit == 0)
			{
				throw new IllegalArgumentException("initLimit is not set");
			}
			if (syncLimit == 0)
			{
				throw new IllegalArgumentException("syncLimit is not set");
			}
			/*
			 * If using FLE, then every server requires a separate election port.
			 */
			if (electionAlg != 0)
			{
				for (QuorumServer s : servers.values())
				{
					if (s.electionAddr == null)
						throw new IllegalArgumentException("Missing election port for server: " + s.id);
				}
			}

			/*
			 * Default of quorum config is majority
			 */
			if (servers.size() != serverGroup.size())
					throw new ConfigException("Every server must be in exactly one group");
			/*
			 * The deafult weight of a server is 1
			 */
			for (QuorumServer s : servers.values())
			{
				if (!serverWeight.containsKey(s.id))
					serverWeight.put(s.id, (long) 1);
			}

			/*
			 * Set the quorumVerifier to be QuorumHierarchical
			 */
			quorumVerifier = new QuorumHierarchical(numGroups, serverWeight, serverGroup);
			

			// Now add observers to servers, once the quorums have been
			// figured out
			servers.putAll(observers);

			// if server id is not already set
			if (serverId <= 0)
			{

				File myIdFile = new File(dataDir, "myid");
				if (!myIdFile.exists())
				{
					throw new IllegalArgumentException(myIdFile.toString() + " file is missing");
				}
				BufferedReader br = new BufferedReader(new FileReader(myIdFile));
				String myIdString;
				try
				{
					myIdString = br.readLine();
				}
				finally
				{
					br.close();
				}
				try
				{
					serverId = Long.parseLong(myIdString);
				}
				catch (NumberFormatException e)
				{
					throw new IllegalArgumentException("serverid " + myIdString + " is not a number");
				}

			}
		}
	}

}
