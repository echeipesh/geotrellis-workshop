name := "geotrellis-workshop"
organization := "geotrellis.batch"
version := "0.1.0"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "com.monovore" %% "decline" % "0.6.2",
  "org.locationtech.geotrellis" %% "geotrellis-spark" % "2.3.0",
  "org.locationtech.geotrellis" %% "geotrellis-s3" % "2.3.0",
  "com.azavea.geotrellis" %% "geotrellis-contrib-vlm" % "2.11.0",
  "com.azavea.geotrellis" %% "geotrellis-contrib-gdal" % "2.11.0",
  "org.apache.spark" %% "spark-core" % "2.4.0" % Provided,
  "org.apache.spark" %% "spark-sql" % "2.4.0" % Provided,
  "org.apache.spark" %% "spark-hive" % "2.4.0" % Provided
)

Test / fork := true
Test / outputStrategy := Some(StdoutOutput)

resolvers ++= Seq(
  "LocationTech Snapshots" at "https://repo.locationtech.org/content/groups/snapshots",
  "LocationTech Releases" at "https://repo.locationtech.org/content/groups/releases",
  Resolver.url("typesafe", url("http://repo.typesafe.com/typesafe/ivy-releases/"))(Resolver.ivyStylePatterns),
  Resolver.bintrayRepo("azavea", "geotrellis")
)

initialCommands in console :=
"""
import java.net._
import geotrellis.raster._
import geotrellis.vector._
import geotrellis.vector.io._
import geotrellis.spark._
import geotrellis.spark.tiling._
import geotrellis.contrib.vlm._
import geotrellis.contrib.vlm.geotiff._
import geotrellis.contrib.vlm.gdal._
import geotrellis.contrib.vlm.avro._
""".stripMargin

// Settings for sbt-assembly plugin which builds fat jars for spark-submit
assemblyMergeStrategy in assembly := {
  case "reference.conf"   => MergeStrategy.concat
  case "application.conf" => MergeStrategy.concat
  case PathList("META-INF", xs @ _*) =>
    xs match {
      case ("MANIFEST.MF" :: Nil) =>
        MergeStrategy.discard
      case ("services" :: _ :: Nil) =>
        MergeStrategy.concat
      case ("javax.media.jai.registryFile.jai" :: Nil) | ("registryFile.jai" :: Nil) | ("registryFile.jaiext" :: Nil) =>
        MergeStrategy.concat
      case (name :: Nil) if name.endsWith(".RSA") || name.endsWith(".DSA") || name.endsWith(".SF") =>
        MergeStrategy.discard
      case _ =>
        MergeStrategy.first
      }
  case _ => MergeStrategy.first
}

// Settings from sbt-lighter plugin that will automate creating and submitting this job to EMR
import sbtlighter._

sparkEmrRelease := "emr-5.23.0"
sparkAwsRegion := "us-east-1"
sparkClusterName := "geotrellis-workshop"
sparkEmrApplications := Seq("Spark", "Zeppelin", "Ganglia")
sparkJobFlowInstancesConfig := sparkJobFlowInstancesConfig.value.withEc2KeyName("geotrellis-emr")
sparkS3JarFolder := "s3://geotrellis-test/jobs/jars"
sparkS3LogUri := Some("s3://geotrellis-test/jobs/logs")
sparkMasterType := "m4.xlarge"
sparkCoreType := "m4.xlarge"
sparkInstanceCount := 5
sparkMasterPrice := Some(0.5)
sparkCorePrice := Some(0.5)
sparkEmrBootstrap := List(
  BootstrapAction("Install GDAL + dependencies",
                  "s3://geotrellis-test/usbuildings/bootstrap.sh",
                  "s3://geotrellis-test/usbuildings",
                  "v1.0"))
sparkEmrServiceRole := "EMR_DefaultRole"
sparkInstanceRole := "EMR_EC2_DefaultRole"
sparkEmrConfigs := List(
  EmrConfig("spark").withProperties(
    "maximizeResourceAllocation" -> "true"
  ),
  EmrConfig("spark-defaults").withProperties(
    "spark.driver.maxResultSize" -> "3G",
    "spark.dynamicAllocation.enabled" -> "true",
    "spark.shuffle.service.enabled" -> "true",
    "spark.rdd.compress" -> "true",
    "spark.driver.extraJavaOptions" -> "-Djava.library.path=/usr/local/lib",
    "spark.executor.extraJavaOptions" -> "-Djava.library.path=/usr/local/lib -XX:+UseParallelGC",
    "spark.executorEnv.LD_LIBRARY_PATH" -> "/usr/local/lib"
  ),
  EmrConfig("spark-env").withProperties(
    "LD_LIBRARY_PATH" -> "/usr/local/lib"
  ),
  EmrConfig("yarn-site").withProperties(
    "yarn.resourcemanager.am.max-attempts" -> "1",
    "yarn.nodemanager.vmem-check-enabled" -> "false",
    "yarn.nodemanager.pmem-check-enabled" -> "false"
  )
)