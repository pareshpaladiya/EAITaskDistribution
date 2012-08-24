package ibm.developerworks.article;

import org.apache.log4j.Logger;

import com.linkedin.norbert.javacompat.network.RequestHandler;

//import com.linkedin.norbert.network.javaapi.MessageHandler;

public class AppMessageHandler implements RequestHandler<AppRequestMsg, String>
{

	private static Logger logr = Logger.getLogger(AppMessageHandler.class);

	public String handleRequest(AppRequestMsg requestObj) throws Exception
	{
		logr.info("Received message:" + requestObj.toString() + " on Server:"
		      + Server.getServerId());
		return "ack_for_message_" + requestObj.getId();
	}

}
