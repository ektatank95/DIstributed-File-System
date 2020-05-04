import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.List;


public class DFSMain {

    /*
    static object to provide registration port and registry
     */
    static int registrationPort = Configurations.REGISTRATION_PORT;
    static Registry registry ;

    public static void main(String[] args) throws IOException {


        try {
            //step 1:
            //create registry to do Remote opertion using RMI
            LocateRegistry.createRegistry(registrationPort);
            registry = LocateRegistry.getRegistry(registrationPort);

            //step 2: create Naming server: go to NamingServer Class file to create it.
            NamingServer namingServer = NamingServer.createNamingServer();
            StorageServer.createStorageServer(namingServer);

//			customTest();
            Client.launchClients();

        } catch (RemoteException   e) {
            System.err.println("Server exception: " + e.toString());
            e.printStackTrace();
        }
    }

}
