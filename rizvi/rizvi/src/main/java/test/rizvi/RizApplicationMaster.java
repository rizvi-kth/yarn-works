package test.rizvi;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import test.rizvi.RizApplicationMaster;
import test.rizvi.RizApplicationMaster.NMCallbackHandler;
import test.rizvi.RizApplicationMaster.RMCallbackHandler;


public class RizApplicationMaster {

	// Configuration
	private Configuration conf;
	private NMCallbackHandler containerListener;
	private NMClientAsync nmClient;
	private AMRMClientAsync rmClient;
	
	private String command;
	private String scriptPath;
	// Launch threads
	private List<Thread> launchedThreads = new ArrayList<Thread>();
	private boolean done; 
	
	
	public RizApplicationMaster() {
	    // Set up the configuration
	    conf = new YarnConfiguration();
	}
	
	public static void main(String[] args) throws Exception{
			RizApplicationMaster appMaster = new RizApplicationMaster();
			System.out.println("AM>>>Running MyApplicationMaster");      
	      
			appMaster.run(args);
	}
	
	public void run(String[] args) throws Exception {		
		// For command with no param
		this.command = args[0];
		this.scriptPath = args[1];
		final int n = Integer.valueOf(args[2]);
		
		

		// Initialize clients to ResourceManager and NodeManagers
		Configuration conf = new YarnConfiguration();

		// AMRMClient<ContainerRequest> rmClient =
		// AMRMClient.createAMRMClient();
		AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();
		rmClient = AMRMClientAsync.createAMRMClientAsync(1000,allocListener);
		rmClient.init(conf);
		rmClient.start();

		//NMClient nmClient = NMClient.createNMClient();
		containerListener = createNMCallbackHandler();		
		nmClient = new NMClientAsyncImpl(containerListener);
		nmClient.init(conf);
		nmClient.start();

		// Register with ResourceManager
		
		RegisterApplicationMasterResponse response = rmClient.registerApplicationMaster("", 0, "");
		int mem = response.getMaximumResourceCapability().getMemory();
		int cor = response.getMaximumResourceCapability().getVirtualCores();
		System.out.println("AM>>> registerApplicationMaster 0 with memory "+ mem + " and cores " + cor);
		
		// Priority for worker containers - priorities are intra-application
		Priority priority = Records.newRecord(Priority.class);
		priority.setPriority(0);

		// Resource requirements for worker containers
		Resource capability = Records.newRecord(Resource.class);
		capability.setMemory(128);
		capability.setVirtualCores(1);

		// Make container requests to ResourceManager
		done = false;
		for (int i = 0; i < n; ++i) {
			ContainerRequest containerAsk = new ContainerRequest(capability,null, null, priority);
			System.out.println("AM>>> Making container request res-req " + (i + 1));
			rmClient.addContainerRequest(containerAsk);
		}		
		
		// Wait for the containers to complete
		while (!done) {
		  try {
		        Thread.sleep(200);
		      } catch (InterruptedException ex) {}
		}
		
		// Join all launched threads needed for when we time out and we need to release containers
	    for (Thread launchThread : this.launchedThreads) {
	      try {
	        launchThread.join(10000);
	      } catch (InterruptedException e) {
	        //LOG.info("Exception thrown in thread join: " + e.getMessage());
	        e.printStackTrace();
	      }
	      }
		
	    
	    // When the application completes, it should stop all running containers
	    System.out.println("AM>>> Application completed. Stopping running containers");
	    nmClient.stop();

	    // When the application completes, it should send a finish application
	    System.out.println("AM>>> Application completed. Signalling finish to RM.");
	    // signal to the RM	    
	    try {
	    	rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "Application finished with ", null);
	      } catch (YarnException ex) {
	        //LOG.error("Failed to unregister application", ex);
	      } catch (IOException e) {
	        //LOG.error("Failed to unregister application", e);
	      }
	      
	    rmClient.stop();
		
	}

	
	public class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {

		private int CompletedContainerCount = 0;
		private int StartedContainerCount = 0;
		
		
		public float getProgress() {
			// TODO Auto-generated method stub
			return 0;
		}

		public void onContainersAllocated(List<Container> allocatedContainers) {
			System.out.println("AM>>>Got response from RM for container ask, allocated Containers=" + allocatedContainers.size());
			for (Container allocatedContainer : allocatedContainers) {
				System.out.println("AM>>>Launching shell command on a new container."+ ", containerId=" + allocatedContainer.getId()+ ", containerNode=" + allocatedContainer.getNodeId().getHost()+ ":" + allocatedContainer.getNodeId().getPort()			            + ", containerNodeURI=" + allocatedContainer.getNodeHttpAddress()			            + ", containerResourceMemory"			            + allocatedContainer.getResource().getMemory()			            + ", containerResourceVirtualCores"			            + allocatedContainer.getResource().getVirtualCores());
				
				LaunchContainerRunnable runnableLaunchContainer = new LaunchContainerRunnable(allocatedContainer, containerListener);
			    Thread containerLaunchThread = new Thread(runnableLaunchContainer);

			        // launch and start the container on a separate thread to keep
			        // the main thread unblocked
			        // as all containers may not be allocated at one go.
			    launchedThreads.add(containerLaunchThread);
			    containerLaunchThread.start();		
			    StartedContainerCount++;
			}			

		}

		public void onContainersCompleted(List<ContainerStatus> completedContainers) {
			System.out.println("AM>>> <onContainersCompleted> invoked for " + completedContainers.size() + " containers.");
			for (ContainerStatus containerStatus : completedContainers) {
				System.out.println("AM>>> Got container status for containerID="+ containerStatus.getContainerId() + ", state="+ containerStatus.getState() + ", exitStatus="+ containerStatus.getExitStatus() + ", diagnostics="+ containerStatus.getDiagnostics());
				
				if (containerStatus.getExitStatus() == 0 )
					CompletedContainerCount++;
			}
			
			if(CompletedContainerCount == StartedContainerCount){
				done = true;
			}
			
			

		}

		public void onError(Throwable arg0) {
			// TODO Auto-generated method stub

		}

		public void onNodesUpdated(List<NodeReport> arg0) {
			// TODO Auto-generated method stub

		}

		public void onShutdownRequest() {
			// TODO Auto-generated method stub

		}

	}

	NMCallbackHandler createNMCallbackHandler() {
		return new NMCallbackHandler(this);
	}

	static class NMCallbackHandler implements NMClientAsync.CallbackHandler {

		private ConcurrentMap<ContainerId, Container> containers = new ConcurrentHashMap<ContainerId, Container>();
		private final RizApplicationMaster rizApplicationMaster;

		public NMCallbackHandler(RizApplicationMaster rizApplicationMaster) {
			this.rizApplicationMaster = rizApplicationMaster;
		}
		
		public void addContainer(ContainerId containerId, Container container) {
		      containers.putIfAbsent(containerId, container);
		}

		public void onContainerStarted(ContainerId arg0,
				Map<String, ByteBuffer> arg1) {
			// TODO Auto-generated method stub

		}

		public void onContainerStatusReceived(ContainerId arg0,
				ContainerStatus arg1) {
			// TODO Auto-generated method stub

		}

		public void onContainerStopped(ContainerId arg0) {
			// TODO Auto-generated method stub

		}

		public void onGetContainerStatusError(ContainerId arg0, Throwable arg1) {
			// TODO Auto-generated method stub

		}

		public void onStartContainerError(ContainerId arg0, Throwable arg1) {
			// TODO Auto-generated method stub

		}

		public void onStopContainerError(ContainerId arg0, Throwable arg1) {
			// TODO Auto-generated method stub

		}
	}
	
	 /**
	   * Thread to connect to the {@link ContainerManagementProtocol} and launch the container
	   * that will execute the shell command.
	   */
	  private class LaunchContainerRunnable implements Runnable {

	    // Allocated container
	    Container container;
	    NMCallbackHandler containerListener;

	    
	    public LaunchContainerRunnable(Container lcontainer, NMCallbackHandler containerListener) 
	    {
	      this.container = lcontainer;
	      this.containerListener = containerListener;
	    }

		public void run() {
			System.out.println("AM>>>Launching a container with containerid # " + container.getId());
			
			// Set the local resources
		    Map<String, LocalResource> _localResources = new HashMap<String, LocalResource>();
		    List<String> _commands = new ArrayList<String>();
		    try{
			    //URL yarnUrl = ConverterUtils.getYarnUrlFromURI(new URI("hdfs:///apps/simple/script_riz.sh"));		    	
		    	URL yarnUrl = ConverterUtils.getYarnUrlFromURI(new URI(scriptPath));
			    
			    FileSystem fs = FileSystem.get(conf);
			    FileStatus shellFileStatus = fs.getFileStatus(new Path(scriptPath) );
			    String hdfsShellScriptLocation = shellFileStatus.getPath().toString();
			    long hdfsShellScriptLen = shellFileStatus.getLen();			    
			    long hdfsShellScriptTimestamp = shellFileStatus.getModificationTime();
			    
			    
			    System.out.println("AM>>> Location: "+ hdfsShellScriptLocation);
			    System.out.println("AM>>> Size: "+ hdfsShellScriptLen);
			    System.out.println("AM>>> Timestamp: "+ hdfsShellScriptTimestamp);
			    
			    LocalResource shellRsrc = LocalResource.newInstance(yarnUrl,LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,hdfsShellScriptLen, hdfsShellScriptTimestamp);
			    _localResources.put("script_riz.sh", shellRsrc);
		    }
		    catch(Exception e){
		    	System.out.println("AM>>> URL handling error!! ");
		    }
		    
		    
		    
		    
		    // 	TODO Run a shell script
		    
		    String _command = command 
		    					+ " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" 
		    					+ " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr";
		    _commands.add(_command);
		    // Creating Container Launch Context
		    ContainerLaunchContext ctx = ContainerLaunchContext.newInstance(_localResources, null, _commands, null, null, null);
		    
		    System.out.println("AM>> Launcing command: " + _command);
		    
		    containerListener.addContainer(container.getId(), container);
		    nmClient.startContainerAsync(container, ctx);
		    
			
			
		}
	  }

}
