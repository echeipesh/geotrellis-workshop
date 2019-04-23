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

import geotrellis.vector._


import cats.implicits._
import com.monovore.decline._

import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd._

import scala.util.Properties

object CogIngest extends CommandApp(
  name = "geotrellis-cog-ingest",
  header = "Transform as TIFF to tiled COGs",
  main = {
    val inputsOpt = Opts.option[String]("input", help = "The path that points to data that will be read")
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

      val inputRdd: RDD[(ProjectedExtent, MultibandTile)] =
        sc.hadoopMultibandGeoTiffRDD(inputs)

      val (_, rasterMetaData) =
        TileLayerMetadata.fromRDD(inputRdd, FloatingLayoutScheme(256))

      val tiled: RDD[(SpatialKey, MultibandTile)] =
        inputRdd
          .tileToLayout(rasterMetaData.cellType, rasterMetaData.layout, Bilinear)
          .repartition(numPartitions)

      val layoutScheme = ZoomedLayoutScheme(WebMercator, tileSize = 2048)

      val (zoom, reprojected): (Int, RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]) =
        MultibandTileLayerRDD(tiled, rasterMetaData)
          .reproject(WebMercator, layoutScheme, Bilinear)

      val cogRasters: RDD[(SpatialKey, Raster[MultibandTile])] = reprojected.toRasters

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
