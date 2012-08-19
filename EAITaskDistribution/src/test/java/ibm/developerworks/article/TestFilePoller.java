package ibm.developerworks.article;

import static org.junit.Assert.fail;

import java.lang.reflect.InvocationTargetException;

import org.junit.Test;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.context.support.GenericApplicationContext;

public class TestFilePoller
{

   @Test
   public void testPolling() throws InterruptedException
   {
      /*
       * get global bean and register as parent context
       */
      //create parent BeanFactory
      DefaultListableBeanFactory parentBeanFactory = new DefaultListableBeanFactory();
      //register your pre-fabricated object in it
      parentBeanFactory.registerSingleton("server", Utils.getServerPropBean("server1"));
      
      //wrap BeanFactory inside ApplicationContext
      GenericApplicationContext parentContext = 
              new GenericApplicationContext(parentBeanFactory);
      
      parentContext.refresh();
      
      ClassPathXmlApplicationContext loContext = new ClassPathXmlApplicationContext( new String[] 
            {"FilePoller_spring.xml"}, parentContext);
      loContext.start();
      
      Thread.currentThread().sleep(5000);
   }

}
