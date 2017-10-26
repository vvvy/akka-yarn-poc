package yakka

import scala.collection.JavaConverters._
import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.yarn
import yarn.api.ApplicationConstants
import yarn.api.records._
import yarn.util.Records
import yarn.client.api._
import AMRMClient.ContainerRequest
import yarn.conf.YarnConfiguration

object AM {
  def main(args: Array[String]): Unit = {
    println("Running ApplicationMaster")
    val (shellCommand, numOfContainers) = (args(0), args(1).toInt)
    val conf = new YarnConfiguration()

    // Point #2
    println("Initializing AMRMCLient")
    val rmClient: AMRMClient[ContainerRequest] = AMRMClient.createAMRMClient()
    rmClient.init(conf)
    rmClient.start()

    println("Initializing NMCLient")
    val nmClient = NMClient.createNMClient()
    nmClient.init(conf)
    nmClient.start()

    // Point #3
    System.out.println("Register ApplicationMaster")
    rmClient.registerApplicationMaster(NetUtils.getHostname(), 0, "")

    // Point #4
    val priority = Records.newRecord(classOf[Priority])
    priority.setPriority(0)

    println("Setting Resource capability for Containers")
    val capability = Records.newRecord(classOf[Resource])
    capability.setMemory(128)
    capability.setVirtualCores(1)
    for (i <- 0 until numOfContainers) {
      val containerRequested = new ContainerRequest(capability, null, null, priority, true)
      // Resource, nodes, racks, priority and relax locality flag
      rmClient.addContainerRequest(containerRequested)
    }

    //---------------------------------------------------------------------------------------------------------------

    val logdir = ApplicationConstants.LOG_DIR_EXPANSION_VAR
    var allocatedContainers = 0
    var completedContainers = 0

    while (completedContainers < numOfContainers) {
      val response = rmClient.allocate(completedContainers.toFloat / numOfContainers)

      for (container <- response.getAllocatedContainers().asScala) {
        allocatedContainers += 1
        // Launch container by creating ContainerLaunchContext
        val ctx = Records.newRecord(classOf[ContainerLaunchContext])
        ctx.setCommands(List(s"$shellCommand 1>$logdir/stdout 2>$logdir/stderr").asJava)
        println(s"Starting container `${container.getId()}` on node : ${container.getNodeHttpAddress()}")
        nmClient.startContainer(container, ctx)
      }

      for (status <- response.getCompletedContainersStatuses().asScala) {
        completedContainers += 1
        println(s"Container completed : ${status.getContainerId()}")
        println(s"Completed container $completedContainers")
      }
      Thread.sleep(100)
    }

    //---------------------------------------------------------------------------------------------------------------
    rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "")
    println("AM exiting")
  }

}
