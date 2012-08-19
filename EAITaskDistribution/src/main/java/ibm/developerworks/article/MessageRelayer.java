package ibm.developerworks.article;

import java.io.File;
import java.io.IOException;

import org.springframework.integration.Message;

public class MessageRelayer
{
   public void processMessage(Message<File> foMessage)
   {
      File loFile = foMessage.getPayload();
      
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
