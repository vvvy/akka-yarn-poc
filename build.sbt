name := "akka-yarn-poc"

version := "0.1-h" + hadoopVersion

//dc-dev version
//scalaVersion := "2.12.3"
//quckstart vm version (forced by java version 1.7)
scalaVersion := "2.11.11"
scalacOptions += "-target:jvm-1.7"

//dc-dev version
lazy val hadoopVersion = "2.6.0-cdh5.8.3"
//quckstart vm version
//lazy val hadoopVersion = "2.6.0-cdh5.12.0"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-yarn-client" % hadoopVersion % Provided,
  "org.apache.hadoop" % "hadoop-common" % hadoopVersion % Provided
)

resolvers ++= Seq(
  "cloudera-release-repo" at "http://repository.cloudera.com/cloudera/cloudera-release-repo/",
  "ext-release-local" at "http://repository.cloudera.com/cloudera/ext-release-local/"
)