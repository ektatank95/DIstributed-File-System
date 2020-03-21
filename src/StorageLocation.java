import java.io.Serializable;


public class StorageLocation implements Serializable{
	
	private static final long serialVersionUID = -4113307750760738108L;
	
	private String address;
	private int id;
	private boolean isAlive;
	
	public StorageLocation(int id, String address, boolean isAlive) {
		this.id = id;
		this.address = address;
		this.isAlive = isAlive;
	}
	
	boolean isAlive(){
		return isAlive;
	}
	
	int getId(){
		return id;
	}
	
	void setAlive(boolean isAlive){
		this.isAlive = isAlive;
	}
	
	String getAddress(){
		return address;
	}
	
}
