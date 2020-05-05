import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.InputMismatchException;
import java.util.Scanner;


public class DFSMain {

    /*
    static object to provide registration port and registry
     */
    static int registrationPort = Configurations.REGISTRATION_PORT;
    static Registry registry;

    public static void main(String[] args) throws IOException {

        try {

            //create registry to do Remote opertion using RMI
            LocateRegistry.createRegistry(registrationPort);
            registry = LocateRegistry.getRegistry(registrationPort);

            // create Naming server: go to NamingServer Class file to create it.
            NamingServer namingServer = NamingServer.createNamingServer();

            // create storage server by taking information from file
            StorageServer.createStorageServer(namingServer);

            //this method is used to create  dummy some files
            //  Client.launchClients();

            while (true) {
                System.out.println("\nEnter operation you want to do");
                System.out.println("1. Create  a file");
                System.out.println("2. Delete a file");
                System.out.println("3. Read a file");
                System.out.println("4. Write a file");
                System.out.println("5. Exit");
                Scanner sc = new Scanner(System.in);
                int input = 0;
                try {
                    input = sc.nextInt();
//                    if (input == 5) {
//                        break;
//                    }
                } catch (InputMismatchException e) {
                    System.out.println("  Input can be in numbers only  ");
                }
                Client c = new Client();
                if (input == 1) {
                    System.out.println("Enter File name to create");
                    String fileName = sc.next();
                    c.createNewFile(fileName);
                }
               else if (input == 2) {
                    System.out.println("Enter File name to delete");
                    String fileName = sc.next();
                    System.out.println(c.deleteFile(fileName));
                }
                else if (input == 3) {
                    try {
                        System.out.println("Enter File name to read");
                        String fileName = sc.next();
                        System.out.println("File is going to print\n ");
                        c.read(fileName);
                    } catch (FileNotFoundException e) {
                        System.out.println("File does not exist...please enter a valid file name");
                    }
                } else if (input == 4) {
                    System.out.println("Enter File name to write");
                    String fileName = sc.next();
                    System.out.println("Enter Details want to write");
                    Scanner newSc = new Scanner(System.in);
                    String s = newSc.nextLine();
                    char[] ss = s.toCharArray();
                    byte[] data = new byte[ss.length];
                    for (int i = 0; i < ss.length; i++)
                        data[i] = (byte) ss[i];
                    try {
                        c.write(fileName, data);
                    } catch (MessageNotFoundException e) {
                        e.printStackTrace();
                    }
                } else if (input == 5) {
                    System.out.println("Exited successfully");
                    break;
                } else {
                    System.out.println("Wrong input..");
                }
            }

        } catch (RemoteException e) {
            System.err.println("Server exception: " + e.toString());
            e.printStackTrace();
        } catch (NotBoundException e) {
            e.printStackTrace();
        }
    }

}
