package ibm.developerworks.article;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.Future;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.springframework.integration.Message;

import com.google.common.io.Files;

/**
 * Models the task distributor  
 * @author mukul.gupta
 * @author paresh.paladiya
 *
 */
public class TaskDistributor
{
	private static final Charset charset = Charset.forName("UTF-8");
	private static Logger logr = Logger.getLogger(TaskDistributor.class);
	
   public void processMessage(Message<File> polledMessg)
   {
      File polledFile = polledMessg.getPayload();
      
      String payload = null;
      try
      {
      	logr.info("Received file as input:" + polledFile.getCanonicalPath());
      	
      	//prefix file name and a delimiter for the rest of the payload
	      payload = polledFile.getName() + "|~" + Files.toString(polledFile, charset);
	      
	      logr.trace("sending message: payload:" + payload);
	      //create new message
	      AppRequestMsg newMessg = new AppRequestMsg(payload);
	      
	      //loadbalance the request to operating servers without
	      //targetting any one in particular
	      Future<String> retAck = MessagingClient.sendMessage(newMessg, -1);
	      
	      //block for acknowledgement - could have processed acknowledgement
	      //
	      String ack = retAck.get();
	      
	      FileUtils.deleteQuietly(polledFile);
	      logr.info("sent message and received acknowledgement:" + ack);
      }
      catch (IOException e)
      {
         logr.error("Error handling message in task distributor", e);
	      
      }
      catch (Exception e)
      {
         logr.error("Error handling message in task distributor", e);
      }
      

   }
}
