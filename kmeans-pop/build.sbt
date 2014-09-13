import AssemblyKeys._

assemblySettings

scalaVersion := "2.10.4"

resolvers += "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"

libraryDependencies ++= Seq(
  "org.bdgenomics.adam" % "adam-core" % "0.13.1-SNAPSHOT",
  "org.apache.spark" % "spark-mllib_2.10" % "1.1.0"
)


excludedJars in assembly <<= (fullClasspath in assembly) map { cp => 
  cp.filter(_.data.getName == "spark-mllib_2.10.jar")
  .filter {_.data.getName == "spark-core_2.10-1.0.0.jar" }
}