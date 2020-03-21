import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;


public interface StorageStorageInterface extends Remote {
	
	public boolean reflectUpdate(long txnID, String fileName, ArrayList<byte[]> data) throws RemoteException, IOException;
	
	public void releaseLock(String fileName)throws RemoteException;
}
