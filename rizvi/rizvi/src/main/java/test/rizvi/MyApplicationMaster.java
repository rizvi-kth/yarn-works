package test.rizvi;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;


public class MyApplicationMaster {

	   
	  public static void main(String[] args) throws Exception {

		  	// For command with a param
		    //final String command = args[0] + " " + args[1];
		    //final int n = Integer.valueOf(args[2]);
		  	
		    // For command with no param		  
		    //final String command = args[0];
		    //final String command = "find / -name rizviapp.jar";
		    //final String command = "/bin/bash script_riz_lnk_nm.sh";
		  final String command = "cat script_riz_lnk_nm.sh";
		    //final String command = "which bash";
		    //final String command = "ls -R -l";
		    final Path scriptPath = new Path(args[1]);
		    final int n = Integer.valueOf(args[2]);

		    
		    // Initialize clients to ResourceManager and NodeManagers
		    Configuration conf = new YarnConfiguration();
		    
		    
		    AMRMClient<ContainerRequest> rmClient = AMRMClient.createAMRMClient();
		    rmClient.init(conf);
		    rmClient.start();

		    NMClient nmClient = NMClient.createNMClient();
		    nmClient.init(conf);
		    nmClient.start();

		    // Register with ResourceManager
		    System.out.println("AM>>>registerApplicationMaster 0");
		    rmClient.registerApplicationMaster("", 0, "");
		    System.out.println("AM>>>registerApplicationMaster 1");
		    
		    // Priority for worker containers - priorities are intra-application
		    Priority priority = Records.newRecord(Priority.class);
		    priority.setPriority(0);

		    // Resource requirements for worker containers
		    Resource capability = Records.newRecord(Resource.class);
		    capability.setMemory(128);
		    capability.setVirtualCores(1);

		    // Make container requests to ResourceManager
		    for (int i = 0; i < n; ++i) {
		      ContainerRequest containerAsk = new ContainerRequest(capability, null, null, priority);
		      System.out.println("AM>>>Making container request res-req " + i);
		      rmClient.addContainerRequest(containerAsk);
		    }

		    // Obtain allocated containers, launch and check for responses
		    int responseId = 0;
		    int completedContainers = 0;
		    while (completedContainers < n) {
		        AllocateResponse response = rmClient.allocate(responseId++);
		        for (Container container : response.getAllocatedContainers()) {
		            // Launch container by create ContainerLaunchContext
		            ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
		            //RIzvi
		            
		            // Setup script for ApplicationMaster
		    		LocalResource scriptLocalResource = Records.newRecord(LocalResource.class);
		    		setupScriptLocalResource(scriptPath, scriptLocalResource, conf);
		            
		            // Set Local resource to the container-launch-context
		            Map<String, LocalResource> localRes = new HashMap<String, LocalResource>();		    		
		    		localRes.put("script_riz_lnk_nm.sh", scriptLocalResource);
		            ctx.setLocalResources(localRes);
		            
		            // Set environment
		            Map<String, String> appMasterEnv = new HashMap<String, String>();
		    		setupAppMasterEnv(appMasterEnv, conf);
		            ctx.setEnvironment(appMasterEnv);
		            //rizvi
		            
		            ctx.setCommands(
		                    Collections.singletonList(
		                            command +
		                                    " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
		                                    " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
		                    ));
		            
		            
		            System.out.println("AM>>>Launching container " + container.getId());
		            nmClient.startContainer(container, ctx);
		        }
		        for (ContainerStatus status : response.getCompletedContainersStatuses()) {
		            ++completedContainers;
		            System.out.println("AM>>>Completed container " + status.getContainerId());
		        }
		        Thread.sleep(100);
		    }

		    // Un-register with ResourceManager
		    rmClient.unregisterApplicationMaster(
		        FinalApplicationStatus.SUCCEEDED, "", "");
		  }
	  
	  private static void setupScriptLocalResource(Path scriptPath, LocalResource scriptLocalResource,Configuration conf)
				throws IOException {
			FileStatus jarStat = FileSystem.get(conf).getFileStatus(scriptPath);
			scriptLocalResource.setResource(ConverterUtils.getYarnUrlFromPath(scriptPath));
			scriptLocalResource.setSize(jarStat.getLen());
			scriptLocalResource.setTimestamp(jarStat.getModificationTime());
			scriptLocalResource.setType(LocalResourceType.FILE);
			scriptLocalResource.setVisibility(LocalResourceVisibility.PUBLIC);
		}
	  
	  private static void setupAppMasterEnv(Map<String, String> appMasterEnv,Configuration conf) {
			for (String c : conf.getStrings(
					YarnConfiguration.YARN_APPLICATION_CLASSPATH,
					YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
				Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(),
						c.trim());
			}
			Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(),
					Environment.PWD.$() + File.separator + "*");
		}

}
