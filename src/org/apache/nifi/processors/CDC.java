package org.apache.nifi.processors;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class CDC  {
	
	private static AtomicLong fromUpdateTs;  //should be persisted outside jvm
	private static AtomicLong fromPartition; //should be persisted outside jvm
	private static AtomicLong toUpdateTs;  	 //should be persisted outside jvm
	private static AtomicLong toPartition;   //should be persisted outside jvm
	private static boolean hasPartitionEndPoint = false;
	private static boolean hasUpdateEndPoint = false;
	
	public static void main (String[] args) {
		
		// 1) get input args from NiFi, context.getProperty(TABLE_NAME).getValue()
		String driverClass = "oracle.jdbc.driver.OracleDriver";
		String jdbcUrl = "";
		String user = "";
		String password = "";
		String updateColName = "update_ts";
		String partitionColName = "partition_ts";
		String selectColumns = "*";  // comma delimited list of column names
		String tableName = "db.table";
		long initialUpdateState = new Date().getTime();
		long initialPartitionState = new Date().getTime();
		long endUpdateState = 0L;
		long endPartitionState = 0L;
		
		
		TimeUnit timeUnit = TimeUnit.SECONDS;
		long initialDelay = 10;
		long delay = 10;
		
		
		// 2) Validate Pre-conditions
		try {
			   Class.forName(driverClass);
			}
			catch(ClassNotFoundException ex) {
			   System.out.println("Error: unable to load driver class!"); // warning back to NiFi
			   System.exit(1);
			}
		
		if (endUpdateState > 0) {
			if (endUpdateState > fromUpdateTs.get() ) {
				hasUpdateEndPoint = true;
			}
			else {
				System.out.println("End point is before starting point, please remove end point or modify it to be > starting point");
			}
		}
		if (endPartitionState > 0) {
			if (endPartitionState > fromPartition.get()) {
				hasPartitionEndPoint = true;
			}
			else {
				System.out.println("End point is before starting point, please remove end point or modify it to be > starting point");
			}
		}
		
		if (initialUpdateState > 0 || initialPartitionState > 0) {
			if (initialUpdateState > fromUpdateTs.get() && initialPartitionState > fromPartition.get()) {
				System.out.println("skipping ahead might have data gaps");
				fromUpdateTs.set(initialUpdateState);
				fromPartition.set(initialPartitionState);
			}
			else {
				System.out.println("Starting point is before current point, replaying rows, might get collisions on target db");
			}
		}
		
		// 3) Start Processor
		
		final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);  // nifi concurrent input param, don't allow more than 1 thread for this processor
		
		@SuppressWarnings("unused")
		final ScheduledFuture<?> queryHandler = scheduler.scheduleWithFixedDelay(new CDC().new RunQuery(jdbcUrl,user,password, updateColName, partitionColName, selectColumns, tableName), initialDelay, delay, timeUnit);

	}
	
	
	public class RunQuery implements Runnable {
		
		private String jdbcUrl;
		private String user;
		private String password;
		private String updateColName;
		private String partitionColName;
		private String selectColumns;
		private String tableName;
		
		public RunQuery (String jdbcUrl, String user, String password, String updateColName, String partitionColName, String selectColumns, String tableName) {
			this.jdbcUrl = jdbcUrl;
			this.user = user;
			this.password = password;
			this.updateColName = updateColName;
			this.partitionColName = partitionColName;
			this.selectColumns = selectColumns;
			this.tableName = tableName;
		}
			
		
		@Override
		public void run() {
			
			System.out.println("Log entry starting Query");
			Connection con = null;
			PreparedStatement stmt = null;
			
			try {
			
				con = DriverManager.getConnection(jdbcUrl , user,  password);
				
				// If you have an end date or ending partition in mind, meaning you don't want to load data forever.
				// This can happen in a re-run or window of loading time pattern.
				String toPartitionSQL= "";
				if (hasPartitionEndPoint) {
					toPartitionSQL = " AND "+ partitionColName + " <= ? ";
				}
				String toUpdateSQL= "";
				if (hasUpdateEndPoint) {
					toUpdateSQL = " AND "+ updateColName + " <= ? ";
				}
				
	            stmt = con.prepareStatement("select "+ selectColumns +" from "+ tableName +" where "+updateColName+" > ? and "+partitionColName+" >= ? " + toPartitionSQL + toUpdateSQL);
	            System.out.println("DB Connection successful....");
	            
	            stmt.setLong(1, fromUpdateTs.get());
	            stmt.setLong(2,  fromPartition.get());
	            if (hasPartitionEndPoint) {
	            	stmt.setLong(3, toPartition.get());
	            }
	            
	            if (hasUpdateEndPoint) {
	            	stmt.setLong(3, toUpdateTs.get());
	            }
	            
	            
	            ResultSet rs = stmt.executeQuery();
	            
	            long localLastUpdateState = fromUpdateTs.get();
	            long localPartitionState = fromPartition.get();
	            while (rs.next()) {
	             // stream into a NiFi queue
	            	
	            	long updateTs = rs.getLong(updateColName);
	            	if (updateTs > localLastUpdateState ) {
	            		localLastUpdateState = updateTs;
	            		localPartitionState = rs.getLong(partitionColName);
	            	}
	            	
	            }
	            
	            // synchronized setter back to managed state variable
	            fromUpdateTs.set(localLastUpdateState);
	            fromPartition.set(localPartitionState);
			}
			catch(SQLException se) {
				se.printStackTrace();
				
			}
			
			finally {
				if (stmt != null) {
					try {
						stmt.close();
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
				
				if (con != null) {
					try {
						con.close();
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
				
				System.out.println("Log entry ending Query");
			}
	}
	
}
	
	/**
	 * QueryDatabase processor captures state, and has a feature to clear state
	 */
	public static void clearState() {
		fromUpdateTs.set(0L);
		fromPartition.set(0L);
	}
	
	/**
	 * setState so that you don't have to start from day 1,
	 * input your starting parameters.
	 * @param lastUpdateState
	 * @param partitionState
	 */
	public static void setState(long lastUpdateState, long partitionState) {
		CDC.fromUpdateTs.set(lastUpdateState);
		CDC.fromPartition.set(partitionState);
	}
	
}
