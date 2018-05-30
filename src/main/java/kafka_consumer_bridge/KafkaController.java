package kafka_consumer_bridge;


import java.util.*;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
//import org.apache.kafka.clients.producer.ProducerRecord;

import kafka.producer.ProducerConfig;

 
public class KafkaController {
 
    public void sendMessage(String post){
    	
    	String msg = "";
//    	
              
        try {
        
        
        KeyedMessage<String, String> dataII = new KeyedMessage<String, String>(DisqusConsumer.outputTopic, null,post);
       
                
        DisqusConsumer.kproducer.send(dataII);
       
        
        
        }
        catch (Exception e) {
  			// TODO Auto-generated catch block
  			e.printStackTrace();
  			DisqusConsumer.kproducer.close();
  			DisqusConsumer.logger.error(e.getMessage().toString());
  		}
        
        
    }


}