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
import geotrellis.spark.io.file.cog.FileCOGLayerWriter
import geotrellis.spark.tiling._
import geotrellis.spark.pyramid._
import geotrellis.spark.reproject._
import geotrellis.spark.tiling._
import geotrellis.spark.render._

import geotrellis.vector._


import cats.implicits._
import com.monovore.decline._

import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd._

import scala.util.Properties
import java.net.URI

object CogLayerIngest extends CommandApp(
  name = "geotrellis-cog-ingest",
  header = "Ingest GeoTrellis COG Layer",
  main = {
    val inputsOpt = Opts.option[String]("input", help = "The path that points to data that will be read")
    val outputOpt = Opts.option[String]("output", help = "The path of the output catlaog")
    val numPartitionsOpt = Opts.option[Int]("numPartitions", help = "The number of partitions to use").withDefault(16)

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

      // Read the geotiff in as a single image RDD,
      // using a method implicitly added to SparkContext by
      // an implicit class available via the
      // "import geotrellis.spark.io.hadoop._ " statement.
      val inputRdd: RDD[(ProjectedExtent, MultibandTile)] =
        sc.hadoopMultibandGeoTiffRDD(inputs)

      // Use the "TileLayerMetadata.fromRdd" call to find the zoom
      // level that the closest match to the resolution of our source image,
      // and derive information such as the full bounding box and data type.
      val (_, rasterMetaData) =
        TileLayerMetadata.fromRDD(inputRdd, FloatingLayoutScheme(256))

      // Use the Tiler to cut our tiles into tiles that are index to a floating layout scheme.
      // We'll repartition it so that there are more partitions to work with, since spark
      // likes to work with more, smaller partitions (to a point) over few and large partitions.
      val tiled: RDD[(SpatialKey, MultibandTile)] =
        inputRdd
          .tileToLayout(rasterMetaData.cellType, rasterMetaData.layout, Bilinear)
          .repartition(numPartitions)

      // We'll be tiling the images using a zoomed layout scheme
      // in the web mercator format (which fits the slippy map tile specification).
      // We'll be creating 256 x 256 tiles.
      val layoutScheme = ZoomedLayoutScheme(WebMercator, tileSize = 256)

      // We need to reproject the tiles to WebMercator
      val (zoom, reprojected): (Int, MultibandTileLayerRDD[SpatialKey]) =
        MultibandTileLayerRDD(tiled, rasterMetaData)
          .reproject(WebMercator, layoutScheme, Bilinear)

      // Create the attributes store that will tell us information about our catalog.
      // Note: we're using SPI to select appropriate implementation
      val attributeStore = AttributeStore(output)

      // Create the writer that we will use to store the tiles in the local catalog.
      // Note: AttributeStore and LayerWriter don't have to use the same thing
      val writer = new FileCOGLayerWriter(attributeStore, new URI(output).getPath)

      // We don't have to call the Pyramid function since COGs overlap with the concept
      // all the zoom levels will be written out for us
      writer.write("landsat-cog", reprojected, zoom, ZCurveKeyIndexMethod)

      spark.stop()
    }
  }
)