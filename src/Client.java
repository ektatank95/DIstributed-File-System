import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
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
		} catch (RemoteException | NotBoundException e) {
			// fatal error .. no registry could be linked
			e.printStackTrace();
		}
	}



	public void read(String fileName) throws IOException, NotBoundException{
		List<StorageLocation> locations = namingServerStub.read(fileName);
		System.out.println("Naming Server Granted read operation for file"+fileName);
		// TODO fetch from all and verify 
		StorageLocation storageLocation = locations.get(0);
         //get storageServerstub of storoage server where file is specified..
		StorageServerClientInterface storageServerStub = (StorageServerClientInterface) registry.lookup("StorageServer_"+storageLocation.getId());
		FileContent fileContent = storageServerStub.read(fileName);
		System.out.println(" ---- Content of File are as Given below...\n");
		System.out.println("--------------------------------Read Start------------------------------------------------------------");
		System.out.println(new String(fileContent.getData()));
		System.out.println("---------------------------------Read End -----------------------------------------------------------");
	}
	


	//doubt
	public void write (String fileName, byte[] data) throws IOException, NotBoundException, MessageNotFoundException{
		//client will request namingServer to give write access to particular File .This  request is done to get location of storage server

		WriteAck ackMsg = namingServerStub.write(fileName);
		StorageServerClientInterface storageServerStub = (StorageServerClientInterface) registry.lookup("StorageServer_" +ackMsg.getLoc().getId());

		System.out.println(" Naming Server granted write operation to Client and give storage server information with required file\n");

		//write in segN parts
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
		
		
		System.out.println(" write operation completed successfully\n");
		storageServerStub.commit(ackMsg.getTransactionId(), segN);
		System.out.println("commit operation completed successfully\n");
	}

	public static void launchClients(){
		try {
			Client c = new Client();
			char[] ss = "I am writing in new File without creating it ".toCharArray();
			byte[] data = new byte[ss.length];
			for (int i = 0; i < ss.length; i++)
				data[i] = (byte) ss[i];

			c.write("bhautik", data);
			 c.read("bhautik");


			c = new Client();
			ss = "File 1 Again Again END ".toCharArray();
			data = new byte[ss.length];
			for (int i = 0; i < ss.length; i++)
				data[i] = (byte) ss[i];

			c.write("file1", data);
			 c.read("file1");

			c = new Client();
			ss = "File 2 test test END ".toCharArray();
			data = new byte[ss.length];
			for (int i = 0; i < ss.length; i++)
				data[i] = (byte) ss[i];

			c.write("file2", data);
			c.read("file2");

		} catch (NotBoundException | IOException | MessageNotFoundException e) {
			e.printStackTrace();
		}
	}

	public void createNewFile(String fileName) throws RemoteException {
		namingServerStub.createNewEmptyFile(fileName);
	}

    public String deleteFile(String fileName) throws IOException, NotBoundException {
        List<StorageServerClientInterface> storageServers = findStorageLocation(fileName);
        if (storageServers==null){
        	return "File does not exist with Name "+fileName;
		}
        for (int i=0;i<storageServers.size();i++) {
            storageServers.get(i).deleteFile(fileName);
        }
        return "File deleted successfully\n";
    }

    private List<StorageServerClientInterface> findStorageLocation(String fileName) throws RemoteException, NotBoundException {
        List<StorageLocation> storageLocations = namingServerStub.fileStorageLocation(fileName);

        List<StorageServerClientInterface> storageServers=new ArrayList<>();
        if (storageLocations==null || storageLocations.size()==0){
        	return null;
		}
        for(int i=0;i<storageLocations.size();i++) {
        	int id=storageLocations.get(i).getId();
            storageServers.add((StorageServerClientInterface) registry.lookup("StorageServer_" +id));
        }
        return storageServers;
    }
}
