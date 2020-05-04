import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.List;


public class Client {


	NamingServerClientInterface namingServerStub;
	static Registry registry;
	int regPort = Configurations.REGISTRATION_PORT;
	String regAddr = Configurations.REGISRATION_ADDRESS;
	int chunkSize = Configurations.CHUNK_SIZE; // in bytes 
	
	public Client() {
		try {
			//create client object and register in locateRegistry
			registry = LocateRegistry.getRegistry(regAddr, regPort);
			//lookup and get stub of naming server
			namingServerStub =  (NamingServerClientInterface) registry.lookup("NamingServerClientInterface");
			System.out.println("[@client] Naming server  Stub fetched successfuly");
		} catch (RemoteException | NotBoundException e) {
			// fatal error .. no registry could be linked
			e.printStackTrace();
		}
	}



	public byte[] read(String fileName) throws IOException, NotBoundException{
		List<StorageLocation> locations = namingServerStub.read(fileName);
		System.out.println("[@client] Naming Server Granted read operation");
		
		// TODO fetch from all and verify 
		StorageLocation replicaLoc = locations.get(0);
         //get storageServerstub of storoage server where file is specified..
		StorageServerClientInterface replicaStub = (StorageServerClientInterface) registry.lookup("ReplicaClient"+replicaLoc.getId());
		FileContent fileContent = replicaStub.read(fileName);
		System.out.println("[@client] read operation completed successfuly");
		System.out.println("[@client] data:");
		
		System.out.println(new String(fileContent.getData()));
		return fileContent.getData();
	}
	


	//doubt
	public void write (String fileName, byte[] data) throws IOException, NotBoundException, MessageNotFoundException{
		//client will request namingServer to give write access to particular File .This  request is done to get location of storage server
		WriteAck ackMsg = namingServerStub.write(fileName);
		StorageServerClientInterface storageServerStub = (StorageServerClientInterface) registry.lookup("ReplicaClient"+ackMsg.getLoc().getId());
		
		System.out.println("[@client] Naming Server  granted write operation to Client");
		
		int segN = (int) Math.ceil(1.0*data.length/chunkSize);
		FileContent fileContent = new FileContent(fileName);
		ChunkAck chunkAck;
		byte[] chunk = new byte[chunkSize];
		
		for (int i = 0; i < segN-1; i++) {
			System.arraycopy(data, i*chunkSize, chunk, 0, chunkSize);
			fileContent.setData(chunk);
			do { 
				chunkAck = storageServerStub.write(ackMsg.getTransactionId(), i, fileContent);
			} while(chunkAck.getSeqNo() != i);
		}

		// Handling last chunk of the file < chunk size
		int lastChunkLen = chunkSize;
		if (data.length%chunkSize > 0)
			lastChunkLen = data.length%chunkSize; 
		chunk = new byte[lastChunkLen];
		System.arraycopy(data, segN-1, chunk, 0, lastChunkLen);
		fileContent.setData(chunk);
		do { 
			chunkAck = storageServerStub.write(ackMsg.getTransactionId(), segN-1, fileContent);
		} while(chunkAck.getSeqNo() != segN-1 );
		
		
		System.out.println("[@client] write operation complete");
		storageServerStub.commit(ackMsg.getTransactionId(), segN);
		System.out.println("[@client] commit operation complete");
	}
	
	/*public void commit(String fileName, long txnID, long seqN) throws MessageNotFoundException, IOException, NotBoundException{
		StorageLocation primaryLoc = namingServerStub.locatePrimaryReplica(fileName);
		StorageServerClientInterface primaryStub = (StorageServerClientInterface) registry.lookup("ReplicaClient"+primaryLoc.getId());
		primaryStub.commit(txnID, seqN);
		System.out.println("[@client] commit operation complete");
	}
	*/
/*	public void batchOperations(String[] cmds){
		System.out.println("[@client] batch operations started");
		String cmd ;
		String[] tokens;
		for (int i = 0; i < cmds.length; i++) {
			cmd = cmds[i];
			tokens = cmd.split(", ");
			try {
				if (tokens[0].trim().equals("read"))
					this.read(tokens[1].trim());
				else if (tokens[0].trim().equals("write"))
					this.write(tokens[1].trim(), tokens[2].trim().getBytes());
				else if (tokens[0].trim().equals("commit"))
						this.commit(tokens[1].trim(), Long.parseLong(tokens[2].trim()), Long.parseLong(tokens[3].trim()));
			}catch (IOException | NotBoundException | MessageNotFoundException e){
				System.err.println("Operation "+i+" Failed");
			}
		}
		System.out.println("[@client] batch operations completed");
	}*/


	public static void launchClients(){
		try {
			Client c = new Client();
			char[] ss = "File 1 test test END ".toCharArray();
			byte[] data = new byte[ss.length];
			for (int i = 0; i < ss.length; i++)
				data[i] = (byte) ss[i];

			c.write("file1", data);
			byte[] ret = c.read("file1");
			System.out.println("file1: " + ret);

			c = new Client();
			ss = "File 1 Again Again END ".toCharArray();
			data = new byte[ss.length];
			for (int i = 0; i < ss.length; i++)
				data[i] = (byte) ss[i];

			c.write("file1", data);
			ret = c.read("file1");
			System.out.println("file1: " + ret);

			c = new Client();
			ss = "File 2 test test END ".toCharArray();
			data = new byte[ss.length];
			for (int i = 0; i < ss.length; i++)
				data[i] = (byte) ss[i];

			c.write("file2", data);
			ret = c.read("file2");
			System.out.println("file2: " + ret);

		} catch (NotBoundException | IOException | MessageNotFoundException e) {
			e.printStackTrace();
		}
	}
}
