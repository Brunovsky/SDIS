package dbs;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface ClientInterface extends Remote {
    void backup(String pathname, int replicationDegree) throws RemoteException;
    void restore(String pathname) throws RemoteException;
    void delete(String pathname) throws RemoteException;
    void reclaim(int maxDiskSpaceChuncks) throws RemoteException;
    String state() throws RemoteException;
}