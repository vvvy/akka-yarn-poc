package yakka

import scala.collection.JavaConverters._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
//import org.apache.hadoop.net.NetUtils
//import org.apache.hadoop.security.SecurityInfo
import org.apache.hadoop.yarn
import yarn.api.ApplicationConstants
import yarn.api.records._
import yarn.util.{Apps, ConverterUtils, Records}
import yarn.client.api._
import yarn.conf.YarnConfiguration
import ApplicationConstants.Environment

object Main {
  def main(args: Array[String]): Unit = {
    val (command, n, jarPath) = (args(0), args(1).toInt, new Path(args(2)))

    val conf = new Configuration()
    val yarnConf = new YarnConfiguration(conf)

    /**
      * Create yarnClient
      */
    val yarnClient = YarnClient.createYarnClient()
    yarnClient.init(yarnConf)
    yarnClient.start()


    /**
      * Create application via yarnClient
      */
    val app = yarnClient.createApplication()

    /**
      * Set up the container launch context for the application master
      */
    val amContainer/*: ContainerLaunchContext*/ = Records.newRecord(classOf[ContainerLaunchContext])

    val ldir = ApplicationConstants.LOG_DIR_EXPANSION_VAR
    val cls = "yakka.AM"

    amContainer.setCommands(List(
      s"$$JAVA_HOME/bin/java -Xmx256M $cls $command $n 1>$ldir/stdout 2>$ldir/stderr"
    ).asJava)

    /**
      * Setup jar for ApplicationMaster
      */
    val appMasterJar = Records.newRecord(classOf[LocalResource])
    val jarStat = FileSystem.get(conf).getFileStatus(jarPath)
    appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(jarPath))
    appMasterJar.setSize(jarStat.getLen())
    appMasterJar.setTimestamp(jarStat.getModificationTime())
    appMasterJar.setType(LocalResourceType.FILE)
    appMasterJar.setVisibility(LocalResourceVisibility.PUBLIC)
    amContainer.setLocalResources(Map("yakka.jar" -> appMasterJar).asJava)

    /**
      * Setup CLASSPATH for ApplicationMaster
      */
    val cpsep = ":"
    val appMasterEnv = new java.util.HashMap[String, String]
    for { c <- yarnConf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH) } {
      Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(), c.trim(), cpsep)
    }
    Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(),
      Environment.PWD.$() + java.io.File.separator + "*", cpsep)

    amContainer.setEnvironment(appMasterEnv)

    /**
      * Set up resource type requirements for ApplicationMaster
      */
    val capability = Records.newRecord(classOf[Resource])
    capability.setMemory(256)
    capability.setVirtualCores(1)


    /**
      * Finally, set-up ApplicationSubmissionContext for the application
      */
    val appContext = app.getApplicationSubmissionContext()
    appContext.setApplicationName("yakka-poc") // application name
    appContext.setAMContainerSpec(amContainer)
    appContext.setResource(capability)
    appContext.setQueue("default") // queue

    /**
      * Submit application
      */
    val appId = appContext.getApplicationId()
    println(s"Submitting application $appId")
    yarnClient.submitApplication(appContext)

    @annotation.tailrec
    def amloop(): Unit = {
      val appReport = yarnClient.getApplicationReport(appId)
      val appState = appReport.getYarnApplicationState()
      appState match {
        case YarnApplicationState.FINISHED | YarnApplicationState.KILLED | YarnApplicationState.FAILED =>
          println(s"Application $appId finished with state $appState at ${appReport.getFinishTime()}")
          ()
        case _ =>
          Thread.sleep(1000)
          amloop()
      }
    }

    amloop()
  }
}
