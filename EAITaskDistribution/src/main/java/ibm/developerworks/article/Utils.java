package ibm.developerworks.article;

import java.io.File;
import java.util.Iterator;
import java.util.Properties;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;

public class Utils
{

	/**
	 * 
	 */
	public static String RNTM_PARM_PREFIX ="$|";
	
	
	
	

	/**
	 * Returns all properties for the configuration
	 * @param fsNameSpace If not null then properties that start with <Name_Space.> are returned
	 * @param foConfig
	 * @param trimNSFromPropName
	 * @return
	 */
	public static Properties getProperties(String fsNameSpace, Configuration foConfig,boolean trimNSFromPropName)
	{
		Properties loProps = new Properties();

		//LogMgr.trace(logr,"getProperties called.. trim namespace=" + trimNSFromPropName,null);
		Iterator<String> loKeys = foConfig.getKeys();
		while (loKeys.hasNext())
		{
			String key = (String) loKeys.next();
			//LogMgr.trace(logr,"key=" + key,null);
			
			//if valid namespace is present and doesnt match
			if (!StringUtils.isBlank(fsNameSpace) && !StringUtils.startsWith(key, fsNameSpace+"."))
			{
				continue;
			}
			String value = (String) foConfig.getProperty(key);
			
			
			if (trimNSFromPropName)
			{
				key = StringUtils.remove(key, fsNameSpace+".");
				//LogMgr.debug(logr,"(post trim)key=" + key,null);
			}
			//LogMgr.debug(logr,"value=" + value,null);
			loProps.put(key, value);

		}

		return loProps;
	}
	
	
	/**
	 * 
	 * @param foSource All properties from this container added to target properties container
	 * @param foTarget The target properties container to which properties are added
	 */
	public static void addPropsFromSourceToTarget(Properties foSource, Properties foTarget)
	{
		//TODO - verify why needed
		Iterator<Object> loKeys = foSource.keySet().iterator();
		while (loKeys.hasNext())
      {
	      String lsKey = (String) loKeys.next();
	      foTarget.setProperty(lsKey, foSource.getProperty(lsKey));
	      
      }
	}//addPropsFromSourceToTarget
	

   /**
    * Removes just file from directory - no directories are removed
    * 
    * @param dir
    */
   public static void deleteFilesInDir(String dirPath)
   {
      File dir = new File(dirPath);

      if (dir.isDirectory())
      {
         String[] children = dir.list();
         for (int i = 0; i < children.length; i++)
         {
            deleteFilesInDir(new File(dir, children[i]).getAbsolutePath());

         }
      }
      else
      {

         // The 'directory' is a file
         FileUtils.deleteQuietly(dir);
      }
   }

}
