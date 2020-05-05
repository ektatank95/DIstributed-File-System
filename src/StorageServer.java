import java.io.*;
import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StorageServer implements StorageServerClientInterface,
		StorageNamingServerInterface, StorageStorageInterface, Remote {

	
	private int regPort = Configurations.REGISTRATION_PORT;
	private String regAddr = Configurations.REGISRATION_ADDRESS;
	static String storageInfoFile = Configurations.STORAGE_INFO_FILE;
	
	private int id;
	private String dir;
	private Registry registry;
	private Map<Long, String> activeTxn; // map between active transactions and file names
	private Map<Long, Map<Long, byte[]>> txnFileMap; // map between transaction ID and corresponding file chunks
	private Map<String,	 List<StorageStorageInterface> > filesReplicaMap; //replicas where files that this replica is its master are replicated
	private Map<Integer, StorageLocation> stoargeServersLocation; // Map<ReplicaID, replicaLoc>
	private Map<Integer, StorageStorageInterface> storageServersStubs; // Map<ReplicaID, replicaStub>
	private ConcurrentMap<String, ReentrantReadWriteLock> locks; // locks objects of the open files

	//ellobrate....
	public StorageServer(int id, String dir) {
		this.id = id;
		this.dir = dir+"/StorageServer_"+id+"/";
		txnFileMap = new TreeMap<Long, Map<Long, byte[]>>();
		activeTxn = new TreeMap<Long, String>();
		filesReplicaMap = new TreeMap<String, List<StorageStorageInterface>>();
		stoargeServersLocation = new TreeMap<Integer, StorageLocation>();
		storageServersStubs = new TreeMap<Integer, StorageStorageInterface>();
		locks = new ConcurrentHashMap<String, ReentrantReadWriteLock>();
		File file = new File(this.dir);
		if (!file.exists()){
			file.mkdir();
		}
		
		try  {
			registry = LocateRegistry.getRegistry(regAddr, regPort);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}


	/**
	 * Storage servers and register replicas at naming server
	 * @param namingServer
	 * @throws IOException
	 */
	static void createStorageServer(NamingServer namingServer)throws IOException{
		System.out.println("creating storage server as per requirment of DFS");
		BufferedReader br = new BufferedReader(new FileReader(storageInfoFile));

		int n = Integer.parseInt(br.readLine().trim());
		StorageLocation storageLocation;
		String s;
		for (int i = 0; i < n; i++) {
			s = br.readLine().trim();
			// give id to server as per num of line
			storageLocation = new StorageLocation(i, s.substring(0, s.indexOf(':')) , true);
			StorageServer rs = new StorageServer(i, "./");

			//create serverstub for each storageserver
			Remote storageserverStub = (Remote) UnicastRemoteObject.exportObject(rs, 0);
			DFSMain.registry.rebind("StorageServer_"+i, storageserverStub);

			//register this storage server with registry
			namingServer.registerStorageServer(storageLocation, storageserverStub);
			System.out.println(" Storage Server created and registered with naming server with id: "+rs.id+" and status: "+rs.isAlive());
		}
		br.close();
	}


    @Override
	public void createFile(String fileName) throws IOException {

		//created a file on storage server with  whole file path= dir+fileName
		File file = new File(dir+fileName);
		locks.putIfAbsent(fileName, new ReentrantReadWriteLock());
		//creating file is a sync task so lock task before file creation, to gain thread safety
		ReentrantReadWriteLock lock = locks.get(fileName);
		lock.writeLock().lock();
		file.createNewFile();
		lock.writeLock().unlock();
	}

	@Override
	public String deleteFile(String fileName) throws IOException {

		//delete a file on storage server
		File file = new File(dir+fileName);
		locks.putIfAbsent(fileName, new ReentrantReadWriteLock());
		//creating file is a sync task so lock task before file creation, to gain thread safety
		ReentrantReadWriteLock lock = locks.get(fileName);
		lock.writeLock().lock();
		file.delete();
		lock.writeLock().unlock();
		return "File deleted successfully";
	}


	@Override
	public FileContent read(String fileName) throws FileNotFoundException,
			RemoteException, IOException {
		File f = new File(dir+fileName);
		
		locks.putIfAbsent(fileName, new ReentrantReadWriteLock());
		ReentrantReadWriteLock lock = locks.get(fileName);

		@SuppressWarnings("resource")
		BufferedInputStream br = new BufferedInputStream(new FileInputStream(f));
		
		// assuming files are small and can fit in memory
		byte data[] = new byte[(int) (f.length())];
		
		lock.readLock().lock();
		br.read(data);
		lock.readLock().unlock();
		
		FileContent content = new FileContent(fileName, data);
		return content;
	}

	@Override
	public ChunkAck write(long txnID, long msgSeqNum, FileContent data)
			throws RemoteException, IOException {
		System.out.println("[@StorageServer] write "+msgSeqNum);
		// if this is not the first message of the write transaction
		if (!txnFileMap.containsKey(txnID)){
			txnFileMap.put(txnID, new TreeMap<Long, byte[]>());
			activeTxn.put(txnID, data.getFileName());
		}
		Map<Long, byte[]> chunkMap =  txnFileMap.get(txnID);
		chunkMap.put(msgSeqNum, data.getData());
		return new ChunkAck(txnID, msgSeqNum);
	}

	@Override
	public boolean commit(long txnID, long numOfMsgs)
			throws MessageNotFoundException, RemoteException, IOException {
		System.out.println("[@Storage Server] commit intiated");
		Map<Long, byte[]> chunkMap = txnFileMap.get(txnID);
		if (chunkMap.size() < numOfMsgs)
			throw new MessageNotFoundException();
		
		String fileName = activeTxn.get(txnID);
		List<StorageStorageInterface> slaveStorageServer = filesReplicaMap.get(fileName);
		
		for (StorageStorageInterface replica : slaveStorageServer) {
			boolean sucess = replica.reflectUpdate(txnID, fileName, new ArrayList<>(chunkMap.values()));
			if (!sucess) {
				// TODO handle failure 
			}
		}

		BufferedOutputStream bw =new BufferedOutputStream(new FileOutputStream(dir+fileName, true));
		locks.putIfAbsent(fileName, new ReentrantReadWriteLock());
		ReentrantReadWriteLock lock = locks.get(fileName);
		lock.writeLock().lock();
		for (Iterator<byte[]> iterator = chunkMap.values().iterator(); iterator.hasNext();) 
			bw.write(iterator.next());
		bw.close();
		lock.writeLock().unlock();
		for (StorageStorageInterface replica : slaveStorageServer)
			replica.releaseLock(fileName);
		activeTxn.remove(txnID);
		txnFileMap.remove(txnID);
		return false;
	}

	@Override
	public boolean abort(long txnID) throws RemoteException {
		activeTxn.remove(txnID);
		filesReplicaMap.remove(txnID);
		return false;
	}


	@Override
	public boolean reflectUpdate(long txnID, String fileName, ArrayList<byte[]> data) throws IOException{
		System.out.println("[@Slave Storage Server] reflect update initiated");
		BufferedOutputStream bw =new BufferedOutputStream(new FileOutputStream(dir+fileName, true));
		locks.putIfAbsent(fileName, new ReentrantReadWriteLock());
		ReentrantReadWriteLock lock = locks.get(fileName);
		lock.writeLock().lock(); // don't release lock here .. making sure coming reads can't proceed
		for (Iterator<byte[]> iterator = data.iterator(); iterator.hasNext();) 
			bw.write(iterator.next());
		bw.close();
		activeTxn.remove(txnID);
		return true;
	}

	@Override
	public void releaseLock(String fileName) {
		ReentrantReadWriteLock lock = locks.get(fileName);
		lock.writeLock().unlock();
	}

	@Override
	public void takeCharge(String fileName, List<StorageLocation> slaveStorageServers) throws AccessException, RemoteException, NotBoundException {
		System.out.println("[@Storage Server "+ this.id+ "] taking charge of file: "+fileName);
		System.out.println(slaveStorageServers);
		
		List<StorageStorageInterface> slaveStorageServersStubs = new ArrayList<StorageStorageInterface>(slaveStorageServers.size());
		
		for (StorageLocation loc : slaveStorageServers) {
			// if the current locations is this replica .. ignore
			if (loc.getId() == this.id)
				continue;
			  
			// if this is a new replica generate stub for this replica
			if (!stoargeServersLocation.containsKey(loc.getId())){
				stoargeServersLocation.put(loc.getId(), loc);
				StorageStorageInterface stub = (StorageStorageInterface) registry.lookup("StorageServer_"+loc.getId());
				storageServersStubs.put(loc.getId(), stub);
			}
			StorageStorageInterface replicaStub = storageServersStubs.get(loc.getId());
			slaveStorageServersStubs.add(replicaStub);
		}
		
		filesReplicaMap.put(fileName, slaveStorageServersStubs);
	}
	

	@Override
	public boolean isAlive() {
		return true;
	}
	
}
