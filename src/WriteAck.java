import java.io.Serializable;

/**
 * Class is used as a response object from naming server to Client with storage Server location having file which client is wishing to right
 */

public class WriteAck implements Serializable{
	
	private static final long serialVersionUID = -4764830257785399352L;
	
	private long transactionId;
	private long timeStamp;
	private StorageLocation loc;

	
	public WriteAck(long tid, long timeStamp, StorageLocation storageLocation) {
		this.transactionId = tid;
		this.timeStamp = timeStamp;
		this.loc = storageLocation;
	}


	public long getTransactionId() {
		return transactionId;
	}


	public long getTimeStamp() {
		return timeStamp;
	}


	public StorageLocation getLoc() {
		return loc;
	}
}
