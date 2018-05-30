package kafka_consumer_bridge;


import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka_consumer_bridge.DisqusConsumer;
import kafka.producer.ProducerConfig;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;

import org.joda.time.Interval;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.InterruptedIOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.DateTime;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.util.StringUtils;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

 
public class DisqusConsumer {
    
 
	public static  String username;
	public static  String password;
	
	public static String connectionString;
	public static String DGroupID;
	public static String streamURL;
	public static String outputqueue;
	public static String logFileDirectory;
	public static String logErrorFileDirectory;
	public static String adminEmail;
	public static String smtp;
	public static String smtpPort;
	public static String smtp_username;
	public static String smtp_password;
	
	public static Channel channel;
	public static Channel uchannel;
	public static Channel nchannel;
	public static com.rabbitmq.client.Connection connection;
		
	public static String splnkIP;
	public static String splnkPort;
	
	public static String inputTopic;
	public static String outputTopic;
	
	public static String blogID;
	public static String newsID;
	public static String forumSiteID;
	public static String postID;
	public static String flag;
	
	public static int postCount=0;
	public static int msgCount=0;
	public static int hashMapCount =0;
	
	public static File rfile;
	public static File efile;
	
	public static FileWriter rfop;
	public static FileWriter efop;
	
	public static HTable htable;
	
	public static HashMap<String, String> dupeList = new HashMap<String,String>(); 
	
	public static DriverManagerDataSource dataSource;
	 
	 
	/* kafka related variables*/
	public static Properties props = null;
	
	public static Properties propsII = null;
	
	public static Producer<String, String> kproducer=null;
	
	public static Producer<String, String> mproducer=null;
	 
	public static Logger logger = LoggerFactory.getLogger("disqus_deduper");
	
	static KafkaController kp = new KafkaController();
	
    private static ConsumerConfig createConsumerConfig(String a_zookeeper, String a_groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", a_zookeeper);
        props.put("group.id", a_groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "smallest");
 
        return new ConsumerConfig(props);
    }
    
    public static String createMsg(String post){
    	
    	String forumID = "";
    	String publicForumId = "";
    	String publicThreadId = "";
    	String publicId="";
    	String changed="";
    	String type_prev="";
    	String type_="";
    	    	
    	
    	String msgType = "";
    	String getDate = "";
    	String madeDate = "";
    	String finalMsg = "";
    	String id = "";
    	String postLink="";
    	String threadLink="";
    	String publicAuthorId="";
      	String newsId="";
      	String blogId="";
    	
      	
        String domain="";
      	String thread_url="";
      	String isBlog="";
      	String isNews="";
      	String isOther="";
      	String blogSiteID="";
      	String newsSiteID="";
      	String insertedDate="";
      	String modifiedDate="";
      	
      	String threadUrl="";
      	
    	String receivedDate = "";
    	String createdDate = "";
    	
        String flagType="";
    	
    	Integer recordsAffected = 0;
    	
    	if (post != null) {
    	try {
		  		ObjectMapper mapper = new ObjectMapper();
			   	mapper.setSerializationInclusion(Include.NON_NULL);
			   	String subStr = "message_body";
			   	int occurrence = (post.length() - post.replaceAll(Pattern.quote(subStr), "").length()) / subStr.length();
			   	String[] splitted = new String[3];
			   	splitted[1] = post;
			   	if(occurrence > 1) {
			   		splitted = post.split("message_body");
			   		post = splitted[1];
			   		if(post.endsWith("{\"")){
			   			post = post.substring(0, post.length() - 2);		   			
			   		}
			   		splitted[1] = post.replaceFirst("\":", "{\"message_body\":");
			   		//System.out.println(splitted[1]);
			   		post = splitted[2];
			   		if(post.endsWith("{\"")){
			   			post = post.substring(0, post.length() - 2);		   			
			   		}
			   		splitted[2] = post.replaceFirst("\":", "{\"message_body\":");
			   		//System.out.println(splitted[2]);
			   	}
			   	if(splitted!=null){
			   			   	
			   	for(int i=1;i<splitted.length-1;i++){
			        if(splitted[i] != null){
			        	post = splitted[i];
			        } else {
			        	break;
			        }
			        JsonNode messageNode = mapper.readTree(post);
				    if (!messageNode.isArray() && !messageNode.isObject()){
			    	try {
			    		throw new Exception("INVALID JSON");
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				    }else if( messageNode.isArray()){
				    	Iterator<JsonNode> commItems = messageNode.elements();	
				       	 while (commItems.hasNext()) {
							 JsonNode jobjNode = commItems.next();
							 String statid = jobjNode.get("id").asText();
							 byte[] currRecord;
				    	 }
				    }else if(messageNode.isObject()) {
				       	ObjectMapper m = new ObjectMapper();
			        	JsonNode rootNode;
						rootNode = m.readTree(post);
					   	
						JsonNode nameNodeI = rootNode.findValue("message_type");
				    	msgType=nameNodeI.textValue(); 
				       
				    	JsonNode nameNodeII = rootNode.path("message_body").findPath("public_forum_id");
				    	publicForumId =nameNodeII.textValue();	
				    	
				    	if(post.contains("changed")){	
					    	JsonNode nameNodeIII = rootNode.path("message_body").findPath("changed");
					    	if(nameNodeIII.isObject()){
					    	    ObjectMapper objm = new ObjectMapper();
					    		changed =nameNodeIII.toString();
					    	}
					    }
				    	
				    	if(post.contains("type_prev")){	
					    	JsonNode nameNodeIV = rootNode.path("message_body").findPath("type_prev");
					    	type_prev =nameNodeIV.textValue();
				    	}
				    	
				    	if(post.contains("public_id")){			    	 	
				       		JsonNode nameNodeV = rootNode.path("message_body").findPath("public_id");
				       		publicId=nameNodeV.textValue();
				    	}
				    
				    	JsonNode nameNodeVI = rootNode.path("message_body").findPath("public_thread_id");
				    	publicThreadId =nameNodeVI.textValue();
		   
				    	if(post.contains("type_")){	
					    	JsonNode nameNodeVII = rootNode.path("message_body").findPath("type");
					    	type_ =nameNodeVII.textValue();
				    	}
				    	
				    	if(msgType.equalsIgnoreCase("post") || msgType.equalsIgnoreCase("vote")){
				    		
				    		//System.out.println("Count of post msgs: " + msgCount);
				    		
				    	} else {
				    		return finalMsg;				    	
				    	}
				    	
				    	
				    	
				    String uniqueKey =publicId   + "_" + publicForumId;
				      	
			        if(!dupeList.containsKey(uniqueKey)){
			        	if(changed.equalsIgnoreCase("{}") && type_prev.equalsIgnoreCase(type_)){
        		    		//Thread.sleep(1000);
    			    		return "DUPLICATE";
    			    	}
				        dupeList.put(uniqueKey, uniqueKey);
				        hashMapCount +=1;
				    } else {
				        	return "DUPLICATE";
				    }
				      	
				    if(hashMapCount >= 50000){
				        	hashMapCount = 0;
				        	dupeList = new HashMap<String, String>();
				    }
						
				       /* if(recordsAffected ==0){
				      	  recordsAffected = check_message(publicId, publicForumId);
				      	  if(recordsAffected <= 0){
				      		  recordsAffected = 0;
				      		  return null;
				      	  }
				      	  recordsAffected = insert_message(publicForumId,domain,threadUrl,isBlog,isNews,isOther,blogSiteID,newsSiteID,insertedDate,modifiedDate);
				     	   
				         }*/
				         
				         
				         //System.out.println(" [x] Sent '" + msgCount + "' MESSAGES.");
				        
				        
				    	//CHECK HBASE IF SOURCE EXISTS
				    	//******************************************************
					  	byte[] currRecord;
	        		   	
				      	currRecord = get(publicForumId,publicId,publicThreadId);
	        		    if(currRecord != null && currRecord.length != 0 ) {
	        		    	if(changed.equalsIgnoreCase("{}") && type_prev.equalsIgnoreCase(type_) && msgType.equalsIgnoreCase("Post")){
	        		    		//Thread.sleep(1000);
	    			    		return "DUPLICATE";
	    			    	}
	        		  
	        		  	}else {
			 				 msgCount += 1;
							//INSERT NEW SOURCE
							flagType = "INSERT";
							put(publicForumId,publicId,publicThreadId);
							htable.flushCommits();
							if(!post.isEmpty()){
								kp.sendMessage(post);
							}
							//System.out.println("Delivered msgCount " + msgCount + " msg.");
						    
								 
			 					 //increment value before insert***
						}		 
	        		    
	        		    
				    }
				    	
			   	}
			   }
		    
		   	} catch (Exception e) {
				// TODO Auto-generated catch block
		   	  try {
				efop.write("ERROR:" + e.getMessage() +"  "  + DateTime.now().toString());
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
				
		   	}
		}
    	
    	return finalMsg;
    	
    	
    }

    public static void put(String publicForumId, String publicId, String publicThreadId) throws RetriesExhaustedWithDetailsException, InterruptedIOException {
    	try {
	    		Put put = new Put(Bytes.toBytes(publicId + "_" + publicForumId));
	    		
	    		//System.out.println("ADDED TO HBASE: " + publicId + "_" + publicForumId);
	    		
				put.add(Bytes.toBytes("forum data"),Bytes.toBytes("forum ID"),Bytes.toBytes(publicForumId)); 
	    		put.add(Bytes.toBytes("forum data"),Bytes.toBytes("public ID"),Bytes.toBytes(publicId));
			    
	    		htable.put(put);
 		
    	} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			try {
				efop.write("ERROR:" + e.getMessage() +"  "  + DateTime.now().toString());
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}		
		
	}
    
    public static byte[] get(String publicForumId, String publicId,String publicThreadId) throws IOException  {
		
    
    	
        Get g = new Get(Bytes.toBytes(publicId  + "_" + publicForumId));
        
        g.addColumn( Bytes.toBytes("forum data"), Bytes.toBytes("public Id"));
        byte [] value= "test".getBytes();
        byte [] valueII= "test".getBytes();
        
        try{
      	  
      	  Result r = htable.get(g);
  		  value = r.getValue(Bytes.toBytes("forum data"), Bytes.toBytes("public Id"));
  		  //valueII = r.getValue(Bytes.toBytes("forum data"), Bytes.toBytes("public Thread"));
  	 	  String valueStr = Bytes.toString(value);
      	   
  	 	  //System.out.println("RETRIEVED RECORD " + valueStr);
  	 	  
  	 	 
  	 	  
  	 	  
        }catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			efop.write("ERROR:" + e.getMessage() +"  "  + DateTime.now().toString());
		}
             	  
		
        return value;
	   		
	}
      
    
    public static int check_message(String forumID, String postID){
   	 Integer recReturn = 0;
   	    
   	    int recordsAffected = 0;
   		int type = 0;
   		
   		int likes = 0;
   		int wereHere = 0;
   		int tlkAbout = 0;
   		
   		//setDataSource();
   		Connection conn = null ;
   		
   		try {
   			
   			
   			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
   			String connectionUrl = "jdbc:sqlserver://192.168.4.201;" + 
   	                 "database=PowerTrack;" + 
   	                 "user=sa;" + 
   	                 "password=97CupChamps"; 
   			conn = DriverManager.getConnection(connectionUrl); 
   			
   			String SQL="INSERT INTO DisqusIDs(forumID, postID) VALUES(?,?)";
   		    			
   		    PreparedStatement preparedStmt = conn.prepareStatement(SQL);
   		      
   		    preparedStmt.setString (1, forumID);
   		    preparedStmt.setString (2, postID);
   		    
   		    
   		    recordsAffected = preparedStmt.executeUpdate();


   		   
   			
   			
   		    } catch (ClassNotFoundException | SQLException e) {
   			// TODO Auto-generated catch block
   			//e.printStackTrace();
   			if(e.getMessage().contains("Duplicate")){
   			   	try {
   					conn.close();
   				} catch (SQLException e1) {
   					// TODO Auto-generated catch block
   					e1.printStackTrace();
   				}												
   				return recordsAffected = 3;
   			}
   		} 
   		catch(Exception e)
   		{
   			e.printStackTrace();
   		  	try {
   				conn.close();
   			} catch (SQLException e1) {
   				// TODO Auto-generated catch block
   				e1.printStackTrace();
   			}	
   		}	
   			
   		//flag="NEW";
   		
   		return recordsAffected;
   	    
   	
   	
   	
   	
   }
    
    
    
    
    
    public static int insert_message(String forumID,String domain,String thread_url,String isBlog,String isNews, String isOther,String blogSiteID,String newsSiteId,String insertedDate,String modifiedDate){
    	
    Integer recReturn = 0;
    
    int recordsAffected = 0;
	int type = 0;
	
	int likes = 0;
	int wereHere = 0;
	int tlkAbout = 0;
	
	//setDataSource();
	Connection conn = null ;
	
	try {
		
		
		Class.forName("com.mysql.jdbc.Driver");
		String connectionUrl =""; 
		Properties info = new Properties( );
		info.put( "user", "portal_master" );
		info.put( "password", "vahh9Ugh2eiCh8" );
		conn = DriverManager.getConnection(connectionString,info); 
		
		//CallableStatement cstmnt = conn.prepareCall("{ call putNewFanPage(?,?,?,?,?,?,?,?,?,?,?,?) }");
	      
	    String sql = "insert into DisqusForums(forumID, domain,thread_url, isBlog, isNews,isOther,blogSiteID, newsSiteID,insertedDate,modifiedDate) values (?,?,?,?,?,?,?,?,?,?)";
	    
	    
	    Calendar cal2 = Calendar.getInstance();
        Date today = cal2.getTime();
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        insertedDate = formatter.format(cal2.getTime());
        modifiedDate = formatter.format(cal2.getTime());      
        	
        	              
	    PreparedStatement preparedStmt = conn.prepareStatement(sql);
	      
	    preparedStmt.setString (1, forumID);
	    preparedStmt.setString (2, domain);
	    preparedStmt.setString (3, thread_url);
	    preparedStmt.setString (4, isBlog);
	    preparedStmt.setString (5, isNews);
	    preparedStmt.setString (6, isOther);
	    preparedStmt.setString (7, blogSiteID);
	    preparedStmt.setString (8, newsSiteId);
	    preparedStmt.setString (9, insertedDate);
	    preparedStmt.setString (10, modifiedDate);
	      // execute the preparedstatement
	    preparedStmt.execute();
	      
	    conn.close();
	    
	   
		
		
	    } catch (ClassNotFoundException | SQLException e) {
		// TODO Auto-generated catch block
		//e.printStackTrace();
		if(e.getMessage().contains("Duplicate")){
		   	try {
				conn.close();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}												
			return recordsAffected = 3;
		}
	} 
	catch(Exception e)
	{
		e.printStackTrace();
	  	try {
			conn.close();
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}	
	}	
		
	flag="NEW";
	
	return recordsAffected;
    
    
    }
      
    public static int update_message(String forumID,String domain,String thread_url,String isBlog,String isNews, String isOther,String blogSiteID,String newsSiteId,String insertedDate,String modifiedDate){
    	
        Integer recReturn = 0;
        
        int recordsAffected = 0;
    	int type = 0;
    	
    	int likes = 0;
    	int wereHere = 0;
    	int tlkAbout = 0;
    	
    	//setDataSource();
    	Connection conn = null;
    	
    	try {
    		
    		
    		Class.forName("com.mysql.jdbc.Driver");
    		String connectionUrl =""; 
    		Properties info = new Properties( );
    		info.put( "user", "portal_master" );
    		info.put( "password", "vahh9Ugh2eiCh8" );
    		conn = DriverManager.getConnection(connectionString,info); 
    		
    		//CallableStatement cstmnt = conn.prepareCall("{ call putNewFanPage(?,?,?,?,?,?,?,?,?,?,?,?) }");
    	      
    	    String sql = "update DisqusForums set thread_url=?,modifiedDate=? where forumid=? and domain=?";
    	    
    	    
    	    Calendar cal2 = Calendar.getInstance();
            Date today = cal2.getTime();
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            insertedDate = formatter.format(cal2.getTime());
            
           modifiedDate = formatter.format(cal2.getTime());      
            		
            	              
    	    PreparedStatement preparedStmt = conn.prepareStatement(sql);
    	      
    	    if(forumID == ""){
    	    	Thread.sleep(1000);
    	    	
    	    }
    	    
    	    preparedStmt.setString (1, thread_url);
    	    preparedStmt.setString (2, modifiedDate);
    	    preparedStmt.setString (3, forumID);
    	    preparedStmt.setString (4, domain);
    	      // execute the preparedstatement
    	    preparedStmt.executeUpdate();
    	      
    	    conn.close();
    	    
    	   
    		
    		
    	    } catch (ClassNotFoundException | SQLException e) {
    		// TODO Auto-generated catch block
    		//e.printStackTrace();
    		if(e.getMessage().contains("Duplicate")){
    		  	try {
    				conn.close();
    			} catch (SQLException e1) {
    				// TODO Auto-generated catch block
    				e1.printStackTrace();
    			}													
    			return recordsAffected = 3;
    		}
    	} 
    	catch(Exception e)
    	{
    		e.printStackTrace();
    	  	try {
				conn.close();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}	
    	}	
    				
    	return recordsAffected;
        
        
        }
      
    public static String fetch_message(String forumID,String domain,String thread_url,String isBlog,String isNews, String isOther,String blogSiteID,String newsSiteId,String insertedDate,String modifiedDate){
    	
        Integer recReturn = 0;
        
        int recordsAffected = 0;
    	int type = 0;
    	
    	int likes = 0;
    	int wereHere = 0;
    	int tlkAbout = 0;
    	
    	//setDataSource();
    		
    	String recordID="";
    	Connection conn=null;
    	
    	
    	try {
    		
    		
    		Class.forName("com.mysql.jdbc.Driver");
    		String connectionUrl =""; 
    		Properties info = new Properties( );
    		info.put( "user", "portal_master" );
    		info.put( "password", "vahh9Ugh2eiCh8" );
    		 conn = DriverManager.getConnection(connectionString,info); 
    		
    		//CallableStatement cstmnt = conn.prepareCall("{ call putNewFanPage(?,?,?,?,?,?,?,?,?,?,?,?) }");
    	      
    	    String sql = "SELECT blogSiteID, newsSiteID FROM DisqusForums WHERE forumID=?";
    	    
    	    
    	    Calendar cal2 = Calendar.getInstance();
            Date today = cal2.getTime();
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            insertedDate = formatter.format(cal2.getTime());
            
            modifiedDate = formatter.format(cal2.getTime());      
            		
            	              
    	    PreparedStatement preparedStmt = conn.prepareStatement(sql);
    	      
    	    preparedStmt.setString(1, forumID);
    	    
    	      // execute the preparedstatement
    	    ResultSet rs = preparedStmt.executeQuery();
    	    while (rs.next()) {
    	    	String userid = rs.getString("blogSiteID");
    	    	String newsid = rs.getString("newsSiteID");
    	    	if(! userid.equals("") &&  userid != null){
    	    		forumSiteID = userid;
    	    		flag = "BLOG";
    	    	}else if(! newsid.equals("") && newsid != null){
    	    		forumSiteID = newsid;
    	    		flag = "NEWS";
    	    	}else if(newsid.equals("") && userid.equals("")){
    	    		forumSiteID="";
    	    		flag = "BLOG";
    	    	}
    	    		
    	    }
    	      
    	    conn.close();
    	    
    	     		
    		
    	    } catch (ClassNotFoundException | SQLException e) {
    		// TODO Auto-generated catch block
    		//e.printStackTrace();
    		if(e.getMessage().contains("Duplicate")){
    		  	try {
    				conn.close();
    			} catch (SQLException e1) {
    				// TODO Auto-generated catch block
    				e1.printStackTrace();
    			}													
    			return "";
    		}
    	} 
    	catch(Exception e)
    	{
    		e.printStackTrace();
    	  	try {
				conn.close();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}	
    	}	
    				
    	return forumSiteID;
        
        
        }
      
    public static void sendMessage(String post, String identifier, String flag){
    	String ipaddress =  "splunk01";
        int portnumber = 10057;
        String sentence;
        String modifiedSentence;
        Socket clientSocket =  new Socket();
        PrintWriter outStreamSplunk;
        
        try
        
        {

        
        	//RABBITMQ MESSAGE DELIVERY 
            /****************************************************************/
        	UUID uuid  = UUID.randomUUID();
            String guid = uuid.toString();
          
             Map<String,Object> headerMap = new HashMap<String, Object>();
             headerMap.put("source",identifier);
             headerMap.put("identifier", guid);
             headerMap.put("type", flag);
             
             
             String message = post;
             if(flag.equals("BLOG")){
             channel.basicPublish("blogs.discus.input", "blogs.discus",new AMQP.BasicProperties.Builder()
                    .headers(headerMap).build(),message.getBytes());
             nchannel.basicPublish("stream.disqus.test", "disqus.test", new AMQP.BasicProperties.Builder()
                   .headers(headerMap).build(),message.getBytes()); 
             nchannel.basicPublish("news.discus.input", "news.discus", new AMQP.BasicProperties.Builder()
                     .headers(headerMap).build(),message.getBytes());
            	 
             } else if(flag.equals("NEWS")){ 
             nchannel.basicPublish("news.discus.input", "news.discus", new AMQP.BasicProperties.Builder()
                     .headers(headerMap).build(),message.getBytes());
             } else if (flag.equals("UNKNOWN")){
            	 channel.basicPublish("blogs.discus.input", "blogs.discus",new AMQP.BasicProperties.Builder()
                         .headers(headerMap).build(),message.getBytes());
            	 nchannel.basicPublish("news.discus.input", "news.discus", new AMQP.BasicProperties.Builder()
                         .headers(headerMap).build(),message.getBytes());
             }
             
             
             System.out.println(" [x] Sent Rabbit '" + message + "'");
           
          /****************************************************************/
             //COMPLETED RABBITMQ MESSAGE DELIVERY 
        }
        catch (Exception exc)
        {
             exc.printStackTrace();
             exc.getMessage();
        }
       
    }
            
    public static void main(String[] args) {
        //String zooKeeper = "192.168.217.152:2181,192.168.217.119:2181,192.168.217.154:2181
    	getConfigurations();
		
		String groupId = DGroupID;
    	  	
    	String zooKeeper = "192.168.7.124:2181,192.168.7.125:2181,192.168.7.126:2181";
    	String zooKeeperSysops = "192.168.7.127:2181, 192.168.7.128:2181, 192.168.7.129:2181";
        String topic = inputTopic;
        int threads = 1;

		/**********************************************
		
					STARTING DISQUS PRODUCER
		
		***********************************************/
		
		try {
			
			
			//errorReporting("*STARTING DISQUS STREAM CONSUMER*");
			initializeAppLogs();
	
			rfop.write("*STARTING DISQUS STREAM CONSUMER*"  + "  "  + DateTime.now().toString());
			efop.write("*STARTING DISQUS STREAM CONSUMER*"  + "  "  + DateTime.now().toString());
			
			props = new Properties();
			props.put("metadata.broker.list", "192.168.7.124:9092,192.168.7.125:9092,192.168.7.126:9092");
		   	props.put("serializer.class", "kafka.serializer.StringEncoder");
		    props.put("broker.list","192.168.7.124:9092,192.168.7.125:9092,192.168.7.126:9092");
		    props.put("request.required.acks", "1");
		    props.put("auto.offset.reset", "smallest");
		    
		    /*propsII = new Properties();
			propsII.put("metadata.broker.list", "192.168.7.127:9092, 192.168.7.128:9092, 192.168.7.129:9092");
		   	propsII.put("serializer.class", "kafka.serializer.StringEncoder");
		    propsII.put("broker.list","192.168.7.124:9092,192.168.7.125:9092,192.168.7.126:9092");
		    propsII.put("request.required.acks", "1");
		    propsII.put("auto.offset.reset", "smallest");*/
			
			/*SET HBASE CONNECTION AND CONFIGURATION*/
			Configuration conf = HBaseConfiguration.create();
			HBaseAdmin admin = new HBaseAdmin(conf);
			for (Entry<String, String> e: conf) {
				  System.out.println(e.getKey() + " ------> " + e.getValue());
			}
			rfop.write("Create hbase connection."  + "  "  + DateTime.now().toString()); 
			ProducerConfig config = new ProducerConfig(props);
			kproducer = new Producer<String, String>(config);
			rfop.write("kafka producer initiated"  + "  "  + DateTime.now().toString());
			//ProducerConfig sysConfig = new ProducerConfig(propsII);
			//mproducer = new Producer<String, String>(sysConfig);
			
			
			  
			htable =new HTable(conf,"Disqus_Firehose");
			htable.setAutoFlushTo(true);
			/*SET HBASE CONNECTION AND CONFIGURATION*/  
			
						
		} catch (MessagingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			//errorReporting(e.getMessage().toString() + " " + " MAIN FUNCTION.");
			try {
				efop.write("ERROR MAIN - "  + e.getMessage() + " "  + DateTime.now().toString());
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
			
		} catch (IOException e) {
			
			// TODO Auto-generated catch block
			e.printStackTrace();
			try {
				efop.write("ERROR MAIN - "  + e.getMessage() + " "  + DateTime.now().toString());
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		} 
		

        
        try
   	    {
        	
       	
       // errorReporting("CONSUMER-KAFKA CONNECTION INITIATING...");	
        	
        ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                createConsumerConfig(zooKeeper, groupId));
        
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(1));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
        
        rfop.write("Start consuming kafka topic: "  + inputTopic +"  "  + DateTime.now().toString());
		
        
        for ( KafkaStream stream : streams) {
        	
        	 ConsumerIterator<byte[], byte[]> it = stream.iterator();
        	 
        	 //errorReporting("...CONSUMER-KAFKA CONNECTION SUCCESSFUL!");
        	 
        	 
           while (it.hasNext())
             {
            	 
            	 try{
            	 
		            	 String mesg = new String(it.next().message());
		            	 if (mesg.isEmpty() || mesg ==""){
		            		 continue;
		            	 }
		            	 
		            	 //System.out.println( mesg);
		            	 // TEST FUNCTION TO RECREATE JSON FOR KIBANA/SQL/SPLUNK
		            	  mesg = createMsg(mesg);
		            	 // END OF REST FUNCTION
		            	 //SEND MESSAGE VIA TCP CONNECTION
		                 //sendMessage(mesg);
            	 }  		
                 catch(Exception e)
                 {
                	 efop.write("ERROR :"  +e.getMessage()+  "  "  + DateTime.now().toString());
         			
                 	continue;
                 }
                 
             }
                 
             System.out.println("MESSAGE TRANSMISSION SUCCESSFUL!");
         }
       
        htable.close();
        
        
        
        }
        catch(Exception e)
        {
        	e.printStackTrace();
        }
        }
        
    private static void getConfigurations(){
		
		 Properties fileProp = new Properties();
		 try {
				
				
				
		    fileProp.load(DisqusConsumer.class.getClassLoader().getResourceAsStream("config.properties"));
		    //System.out.println("READING CONFIG PROPERTIES FILE...");
	   		
		 
		    connectionString = fileProp.getProperty("MySQLConnectionString");
			logFileDirectory = fileProp.getProperty("logFileDirectory");
			DGroupID=fileProp.getProperty("groupID");
			inputTopic = fileProp.getProperty("inputTopic");
			outputTopic = fileProp.getProperty("outputTopic");
			logErrorFileDirectory = fileProp.getProperty("logErrorFileDirectory");
			adminEmail = fileProp.getProperty("adminEmail");
			smtp = fileProp.getProperty("smtp");
			smtpPort = fileProp.getProperty("smtpPort");
			smtp_username = fileProp.getProperty("smtp_username");
			smtp_password = fileProp.getProperty("smtp_password");
			splnkIP = fileProp.getProperty("splnkIP");
			splnkPort = fileProp.getProperty("splnkPort");
			username = fileProp.getProperty("username");
			password = fileProp.getProperty("password");
			streamURL =fileProp.getProperty("streamUrl");
			
			
		} catch (Exception io) {
			 try {
				efop.write("Error:" + io.getMessage()+ "  "  + DateTime.now().toString());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				//e.printStackTrace();
			}
				
		} 		
			
		}
          
    public static void initializeAppLogs() throws MessagingException{
		  
		  String logPath = logFileDirectory;
		  String logErrPath = logErrorFileDirectory;
		  
		  SimpleDateFormat ft = new SimpleDateFormat("yyyyMMddhhmmssa");
		  Date dNow = new Date();
		  
		  rfile = new File(logPath + "\\DISQUS_CONSUMER_" + ft.format(dNow) + ".txt");
		  efile = new File(logErrPath + "\\DISQUS_CONSUMER_ERR_" + ft.format(dNow) + ".txt");
		  
		  try {
			  
			rfop = new FileWriter(rfile);
			efop = new FileWriter(efile);
			
			rfile.createNewFile();
			efile.createNewFile();
	       
		} catch (IOException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			///errorReporting(e.getMessage().toString());
		}
		  
	 }
        
    private static void errorReporting(String errMessage) throws MessagingException{
		// email client handling error messages
		String to_email="zfareed@boardreader.com";
		String from_email="zfareed@boardreader.com";
		String host_server="192.168.5.7";
		
		Properties properties = System.getProperties();
		properties.setProperty("mail.smtp.host",host_server);
		properties.setProperty("mail.smtp.user","rainmaker");
		properties.setProperty("mail.smtp.password", "97CupChamps");
		properties.setProperty("mail.smtp.port","25");
		
		Session session= Session.getDefaultInstance(properties);
		
		MimeMessage message = new MimeMessage(session);
		message.setFrom(new InternetAddress(from_email));
		message.addRecipient(Message.RecipientType.TO, new InternetAddress(to_email));
		message.setSubject("DISQUS_PRODUCER");
		message.setContent(errMessage,"text/html");
		Transport.send(message);
		System.out.println("MESSAGE SENT - WOO WOO!");
		
	}
    
    }




