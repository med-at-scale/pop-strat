
scalaVersion := "2.10.4"

resolvers += "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"

libraryDependencies ++= Seq(
  "org.bdgenomics.adam" % "adam-core" % "0.14.0",
  "org.apache.spark" % "spark-mllib_2.10" % "1.1.0"
)


