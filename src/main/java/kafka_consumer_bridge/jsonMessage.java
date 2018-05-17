package kafka_consumer_bridge;

import java.util.Date;
import java.util.List;
import java.io.Serializable;


public class jsonMessage {
  
	
	private String id;
	private String name;
	private String URL;
	private String category;
	private String location;
	private String msgType;
	private String _type;
	private String _host;
	private String receiveddate;
	private String createddate;
	private String language;
	
	private String publicID;
	private String publicThreadID;
	private String threadLink;
	private String messageType;
	private String mintedDate;
	private String pubDate;
	
	
	private Date createdTime;
	private Date fetchedTime;
	
	private Integer dstream;
	private long avgLatency;
	
	
	
	public jsonMessage() {
		
	}
	
	public String getPublicID() {
		return publicID;
	}
	
	public String getPublicThreadID() {
		return publicThreadID;
	}
	
	public String getThreadLink() {
		return threadLink;
	}
	
	public String getMsgType(){
		return messageType;
	}
	
	public String getMinted(){
		return mintedDate;
	}
	
	
	
	
	public String getID() {
		return id;
	}
	
	public String getUrl() {
		return URL;
	}
	
	public String getName() {
		return name;
	}
	
	public String getDateCreated() {
		return createddate;
	}
	
	public String getDateReceived() {
		return receiveddate;
	}
	
		
	public String getLanguage() {
		return language;
	}
	
	public Integer getDstream() {
		return dstream;
	}
	
	public long getAvgLatency() {
		return avgLatency;
	}
	
		
	public String get_type() {
		return _type;
	}
	
	public String get_host() {
		return _host;
	}

	public String getMessageType() {
		return msgType;
	}
	
	
	public void setName(String fname) {
		this.name = fname;
	}
	
	public void setLanguage(String lang) {
		if(lang == null){
			lang = "unknown";
		}
		this.language = lang ;
	}
	
	public void setLink(String link) {
		this.URL = link ;
	}
	
	public void setID(String fid) {
		this.id = fid ;
	}
	
	public void setAvgLatency(long glatency) {
		this.avgLatency = glatency ;
	}
	
		
	public void setMessageType(String mtype) {
		this.msgType= mtype;
	}
	
	public void set_Type(String type) {
		this._type= type;
	}
	
	public void set_Host(String chost) {
		this._host= chost;
	}
	
		
	public void setDStream(Integer stream) {
		this.dstream = stream;
	}
	
	public void setDate(String rdate) {
		this.receiveddate= rdate;
	}
	
	public void setcDate(String cdate) {
		this.createddate=cdate;
	}
	
	public void  setPublicID(String public_id) {
		this.publicID = public_id;
	}
	
	public void setPublicThreadID(String public_ThreadId) {
		this.publicThreadID =  public_ThreadId;
	}
	
	public void setThreadLink(String thread_link) {
		this.threadLink = thread_link;
	}
	
	public void setMsgType(String msg_type){
	 this.messageType= msg_type;
	}
	
	public void setMinted(String minted_date){
		this.mintedDate = minted_date;
		
	}

	public String toMsg() {
		String sMsg = "{\"id\":" + "\"" + getID()  + "\"," + "\n";
		//s+= "\"msgurl\":" + "\"" + getUrl() + "\"," + "\n";
	
		//s+= "\"type\":" + "\"" + "streams_facebook_prod_out"  + "\"," + "\n";
		//s+= "\"host\":" + "\"" + "192.168.5.41" + "\"}" +  "\n";
		
		
		
		
		return sMsg;
		
	}
	
	
	public String toString() {
		String s = "{\"id\":" + "\"" + getID()  + "\"," + "\n";
		s+= "\"msgurl\":" + "\"" + getUrl() + "\"," + "\n";
		s+= "\"name\":" + "\"" + getName()  + "\"," + "\n";
		s+= "\"message_type\":" + "\"" + getMessageType()   + "\"," + "\n";
		s+= "\"received_date\":" + "\"" + getDateReceived()  + "\"," + "\n";
		s+= "\"created_date\":" + "\"" + getDateCreated()  + "\"," + "\n";
		s+= "\"language\":" + "\"" + getLanguage()  + "\"," + "\n";
		s+= "\"dstream\":" + "\"" + getDstream()  + "\","  + "\n";
		s+= "\"avglatency\":"  + getAvgLatency()  + ","  + "\n";
		s+= "\"@timestamp\":" + "\"" + getDateReceived()  + "\","  + "\n";
		s+= "\"_type\":" + "\"" + "streams_facebook_prod_out"  + "\"," + "\n";
		s+= "\"type\":" + "\"" + "streams_facebook_prod_out"  + "\"," + "\n";
		s+= "\"host\":" + "\"" + "192.168.5.41" + "\"}" +  "\n";
		
		
		
		
		
		return s;
		
	}
}
