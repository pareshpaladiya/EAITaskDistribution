package ibm.developerworks.article;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.Future;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.springframework.integration.Message;

import com.google.common.io.Files;


public class MessageRelayer
{
	private static final Charset charset = Charset.forName("UTF-8");
	private static Logger logr = Logger.getLogger(MessageRelayer.class);
	
   public void processMessage(Message<File> foMessage)
   {
      File loFile = foMessage.getPayload();
      
      String payload = null;
      try
      {
      	logr.info("Received file as input:" + loFile.getCanonicalPath());
	      payload = Files.toString(loFile, charset);
	      
	      AppRequestMsg newMessg = new AppRequestMsg(payload);
	      Future<String> retAck = MessagingClient.sendMessage(newMessg, -1);
	      
	      String ack = retAck.get();
	      
	      FileUtils.deleteQuietly(loFile);
	      logr.info("sent message and received acknowledgement:" + ack);
      }
      catch (IOException e1)
      {
	      // TODO Auto-generated catch block
	      e1.printStackTrace();
      }
      catch (Exception e)
      {
	      // TODO Auto-generated catch block
	      e.printStackTrace();
      }
      
      
      		
      
      try
      {
         System.out.println("Got file to process: " + loFile.getCanonicalPath());
      }
      catch (IOException e)
      {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }
   }
}
