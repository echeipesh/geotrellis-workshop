package geotrellis.batch

import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import geotrellis.raster.render._
import geotrellis.raster.resample._
import geotrellis.raster.reproject._
import geotrellis.proj4._

import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.file._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.index._
import geotrellis.spark.pyramid._
import geotrellis.spark.reproject._
import geotrellis.spark.tiling._
import geotrellis.spark.render._
import geotrellis.spark.io.file.cog.FileCOGLayerWriter
import geotrellis.contrib.vlm.spark._
import geotrellis.contrib.vlm.gdal._

import geotrellis.vector._


import cats.implicits._
import com.monovore.decline._

import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd._

import scala.util.Properties

object VLMCogIngest extends CommandApp(
  name = "geotrellis-vlm-ingest",
  header = "Use RasterSource to ingest some things",
  main = {
    val inputsOpt = Opts.options[String]("input", help = "The path that points to data that will be read")
    val outputOpt = Opts.option[String]("output", help = "The path of the output catlaog")
    val numPartitionsOpt = Opts.option[Int]("numPartitions", help = "The number of partitions to use").withDefault(100)

    (inputsOpt, outputOpt, numPartitionsOpt).mapN { (inputs, output, numPartitions) =>
      val conf = new SparkConf()
        .setIfMissing("spark.master", "local[*]")
        .setAppName("GeoTrellis SimpleIngest")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")
        .set("spark.executionEnv.AWS_PROFILE", Properties.envOrElse("AWS_PROFILE", "default"))
        .set("spark.driver.bindAddress", "127.0.0.1")

      implicit val spark = SparkSession.builder.config(conf).enableHiveSupport.getOrCreate
      implicit val sc = spark.sparkContext

      // Here we work on metadata only
      val sources = inputs.map(GDALRasterSource(_)).toList
      val summary = RasterSummary.fromSeq(sources)
      val scheme = ZoomedLayoutScheme(WebMercator, 2048)
      val LayoutLevel(zoom, layout) = summary.levelFor(scheme)

      // Now we can resamle
      val layerRdd: MultibandTileLayerRDD[SpatialKey] =
        RasterSourceRDD(
          sources = sources.map(_.reprojectToGrid(WebMercator, layout)),
          layout = layout)

      val cogRasters: RDD[(SpatialKey, Raster[MultibandTile])] =
        layerRdd.toRasters

      val cogs: RDD[(SpatialKey, GeoTiff[MultibandTile])] =
        cogRasters.mapValues { case raster =>
          GeoTiff(raster.tile, raster.extent, WebMercator)
          .withOverviews(NearestNeighbor)
        }

      val id = LayerId("landat-my-cogs", zoom - 3)
      val keyToPath: SpatialKey => String =
        SaveToHadoop.spatialKeyToPath(id, output)

      cogs.saveToHadoop(keyToPath){ case (key, geotiff) => geotiff.toByteArray }

      spark.stop()
    }
  }
)
