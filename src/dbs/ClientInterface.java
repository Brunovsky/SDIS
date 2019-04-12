package dbs;

import org.jetbrains.annotations.NotNull;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface ClientInterface extends Remote {

  void backup(@NotNull String pathname, int replicationDegree) throws RemoteException;

  void restore(@NotNull String pathname) throws RemoteException;

  void delete(@NotNull String pathname, boolean runEnhancedVersion) throws RemoteException;

  void reclaim(long maxDiskSpace) throws RemoteException;

  String state() throws RemoteException;
}