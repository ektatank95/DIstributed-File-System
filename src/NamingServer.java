import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeSet;


public class NamingServer implements Remote, NamingServerClientInterface {

	private int nextTID;
	private int statusCheckRate = Configurations.STATUS_CHECK_RATE;
	private int replicationFactor = Configurations.REPLICATION_FACTOR; // number of file replicas
	private Timer statusTimer;
	private Random randomGen;

	private Map<String,	 List<StorageLocation> > filesLocationMap;
	private Map<String, StorageLocation> primaryStorageServerMap;
	//	private Map<Integer, String> activeTransactions; // active transactions <ID, fileName>
	private List<StorageLocation> stoargeServersLocationList;
	private List<StorageNamingServerInterface> storageServersStubsList;


	public NamingServer() {
		//step 3 b). while creating naming server it will require to keep track of some information regarding files of DFS and storage server

		//Hashmap to keep info of file location of DFS
		filesLocationMap = new HashMap<String, List<StorageLocation>>();

		//one file will have one primary Storage location, thiss will nmaintain by naming server...
		primaryStorageServerMap = new HashMap<String, StorageLocation>();
//		//activeTransactions = new HashMap<Integer, String>();

		//List to storageServer location
		stoargeServersLocationList = new ArrayList<StorageLocation>();

		//List of storageServer Location
		storageServersStubsList = new ArrayList<StorageNamingServerInterface>();

		nextTID = 0;
		randomGen = new Random();

		//stebs 3c statusTimer to check status of storage location
		statusTimer = new Timer();  //At this line a new Thread will be created
		statusTimer.scheduleAtFixedRate(new StorageServerStatusCheckTask(), 0, statusCheckRate); //delay in milliseconds
	}

	/**
	 * elects a new primary replica for the given file
	 * @param fileName
	 */
	private void assignNewPrimaryStorageServerForFile(String fileName){
		List<StorageLocation> replicas = filesLocationMap.get(fileName);
		boolean newPrimaryAssigned = false;
		for (StorageLocation replicaLoc : replicas) {
			if (replicaLoc.isAlive()){
				newPrimaryAssigned = true;
				primaryStorageServerMap.put(fileName, replicaLoc);
				try {
					storageServersStubsList.get(replicaLoc.getId()).takeCharge(fileName, filesLocationMap.get(fileName));
				} catch (RemoteException | NotBoundException e) {
					e.printStackTrace();
				}
				break;
			}
		}

		if (!newPrimaryAssigned){
			//TODO

		}
	}

	/**
	 * creates a new file @ N replica servers that are randomly chosen
	 * elect the primary replica at random
	 * @param fileName
	 */
	private void createNewFile(String fileName){
		System.out.println("[@Naming Server] Creating new file initiated");
		int luckyServers[] = new int[replicationFactor];
		List<StorageLocation> storageServers = new ArrayList<StorageLocation>();
		Set<Integer> chosenStorageServers = new TreeSet<Integer>();
		for (int i = 0; i < luckyServers.length; i++) {

			// TODO if no replica alive enter infinte loop
			do {
				luckyServers[i] = randomGen.nextInt(replicationFactor);
//				System.err.println(luckyServers[i] );
//				System.err.println(stoargeServersLocationList.get(luckyServers[i]).isAlive());
			} while(!stoargeServersLocationList.get(luckyServers[i]).isAlive() || chosenStorageServers.contains(luckyServers[i]));

			chosenStorageServers.add(luckyServers[i]);
			// add the lucky storage location to the list of storageServers maintaining the file
			storageServers.add(stoargeServersLocationList.get(luckyServers[i]));
			// create the file at the lucky storageServers
			try {
				//find stub from list for choosen storage server and createFile on that server
				storageServersStubsList.get(luckyServers[i]).createFile(fileName);
			} catch (IOException e) {
				// failed to create the file at replica server 
				e.printStackTrace();
			}

		}

		// the primary replica is the first lucky replica picked
		int primary = luckyServers[0];
		try {
			storageServersStubsList.get(primary).takeCharge(fileName, storageServers);
		} catch (RemoteException | NotBoundException e) {
			// couldn't assign the master replica
			e.printStackTrace();
		}

		filesLocationMap.put(fileName, storageServers);
		primaryStorageServerMap.put(fileName, stoargeServersLocationList.get(primary));

	}

	
	@Override
	public List<StorageLocation> read(String fileName) throws FileNotFoundException,
	IOException, RemoteException {
		List<StorageLocation> replicaLocs = filesLocationMap.get(fileName);
		if (replicaLocs == null)
			throw new FileNotFoundException();
		return replicaLocs;
	}

	//client request will propage here to get storage server object
	@Override
	public WriteAck write(String fileName) throws RemoteException, IOException {
		System.out.println("[@Naming Server] write request initiated");
		long timeStamp = System.currentTimeMillis();
		//naming server will check its registry to get details of storage server having file requested by client..
        // it will give list of storage server..
		List<StorageLocation> storageServerLocationWithFile= filesLocationMap.get(fileName);
		int tid = nextTID++;
		//list of storage server can be null...create new file and assign on random storage servers
		if (storageServerLocationWithFile == null)	// file not found
			createNewFile(fileName);

		StorageLocation primaryStorageLocationForFile = primaryStorageServerMap.get(fileName);

		if (primaryStorageLocationForFile == null)
			throw new IllegalStateException("No primary storage server found");

		// if the primary storage location is down .. elect a new storage location
		if (!primaryStorageLocationForFile.isAlive()){
			assignNewPrimaryStorageServerForFile(fileName);
			primaryStorageLocationForFile = primaryStorageServerMap.get(fileName);
		}

		return new WriteAck(tid, timeStamp,primaryStorageLocationForFile);
	}

	@Override
	public StorageLocation locatePrimaryReplica(String fileName)
			throws RemoteException {
		
		return primaryStorageServerMap.get(fileName);
	}
	

	/**
	 * registers new replica server @ the master by adding required meta data
	 * @param replicaLoc
	 * @param replicaStub
	 */
	public void registerStorageServer(StorageLocation replicaLoc, Remote replicaStub){
		stoargeServersLocationList.add(replicaLoc);
		storageServersStubsList.add( (StorageNamingServerInterface) replicaStub);
	}
	static NamingServer createNamingServer() throws AccessException, RemoteException{

		//step 3 a) to go no-arg constructor of NamingServer..
		NamingServer namingServer = new NamingServer();
		//step 4: generate namingServerStub and bind with registry
		NamingServerClientInterface namingServerStub=
				(NamingServerClientInterface) UnicastRemoteObject.exportObject(namingServer, 0);
		DFSMain.registry.rebind("NamingServerClientInterface", namingServerStub);
		System.err.println("Naming Server is ready to accept register storage server and to serve client read write request");
		return namingServer;
	}

	class StorageServerStatusCheckTask extends TimerTask {

		@Override
		public void run() {
			// check storage server is alive or not at certain interval....
			for (StorageLocation storageServerLocation : stoargeServersLocationList) {
				try {
					storageServersStubsList.get(storageServerLocation.getId()).isAlive();
				} catch (RemoteException e) {
					storageServerLocation.setAlive(false);
					e.printStackTrace();
				}
			}
		}
	}
}
