package org.remus.cassandra;

import java.util.NoSuchElementException;

import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.commons.pool.ObjectPool;
import org.apache.thrift.TException;

abstract class ThriftCaller<T> {
	
	ThriftClientPool clientPool;
	public ThriftCaller(ThriftClientPool clientPool) {
		this.clientPool = clientPool;
	}
	
	public T call() throws Exception {
		T out = null;
		boolean done = false;
		int retryCount = 3;
		Exception outE = null;
		do {
			outE = null;
			Client client = null;
			try {
				client = (Client)clientPool.borrowObject();
				out = request( client );
				done = true;
			} catch (InvalidRequestException e) {	
				outE = e;
			} catch (UnavailableException e) {
				outE = e;
				try {
					clientPool.invalidateObject(client);
				} catch (Exception e2) {						
				}
				client = null;					
				try {
					Thread.sleep(4000);
				} catch (InterruptedException e1) {						
				}				
			} catch (TimedOutException e) {
				outE = e;
				try {
					Thread.sleep(4000);
				} catch (InterruptedException e1) {						
				}
			} catch (NoSuchElementException e) {
				outE = e;
				done = true;
			} catch (IllegalStateException e) {
				outE = e;
			} catch (Exception e) {
				outE = e;
				try {
					clientPool.invalidateObject(client);
				} catch (Exception e2) {						
				}
				client = null;					
				try {
					Thread.sleep(4000);
				} catch (InterruptedException e1) {						
				}				
			} finally {
				try {
					if ( client != null )
						clientPool.returnObject(client);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			retryCount--;
		} while ( retryCount > 0 && !done );		
		if ( outE != null ) {
			throw outE;
		}
		return out;
	}		
	protected abstract T request( Client client ) throws InvalidRequestException, UnavailableException, TimedOutException, TException ;
}