package rc

import org.apache.hadoop.hive.ql.io.RCFileInputFormat
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable
import org.apache.hadoop.io.LongWritable

import org.apache.hadoop.hbase.util.Bytes
import java.util
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._

/*
  * Default source should some kind of relation provider
  */
class DefaultSource extends TableProvider {

  override def inferSchema(caseInsensitiveStringMap: CaseInsensitiveStringMap): StructType =
    getTable(null, Array.empty[Transform], caseInsensitiveStringMap.asCaseSensitiveMap()).schema()

  override def getTable(structType: StructType, transforms: Array[Transform], map: util.Map[String, String]): Table = {
    val path = map.get("path")
    new RCBatchTable(path)
  }

}

object SchemaUtils {
  def getSchema(path: String): StructType = {
    val sparkContext = SparkSession.builder.getOrCreate().sparkContext
    val rdd = sparkContext.hadoopFile(path, classOf[RCFileInputFormat[LongWritable, BytesRefArrayWritable]], classOf[LongWritable],
      classOf[BytesRefArrayWritable], 2
    ).map(pair => {
      val cols = pair._2
      (0 until cols.size()).map(i=>{
        val brw = cols.get(i)
        Bytes.toString(brw.getData(), brw.getStart(), brw.getLength())
      }).mkString("\t")
    }).setName(path)
    val firstLine = rdd.first
    val columnNames = firstLine.split("\t")
    val structFields = columnNames.map(value ⇒ StructField(value, StringType))
    StructType(structFields)
  }
}
/*
  Defines Read Support and Initial Schema
 */

class RCBatchTable(path: String) extends Table with SupportsRead {
  override def name(): String = this.getClass.toString

  override def schema(): StructType = SchemaUtils.getSchema(path)

  override def capabilities(): util.Set[TableCapability] = Set(TableCapability.BATCH_READ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap) = new RCScanBuilder(path)
}


/*
   Scan object with no mixins
 */
class RCScanBuilder(path: String) extends ScanBuilder {
  override def build() = new RCScan(path)
}


// simple class to organise the partition
case class RCPartition(partitionNumber: Int, path: String, header: Boolean = true) extends InputPartition


/*
    Batch Reading Support

    The schema is repeated here as it can change after column pruning etc
 */

class RCScan(path: String) extends Scan with Batch {
  override def readSchema(): StructType = SchemaUtils.getSchema(path)

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    val sc = SparkSession.builder.getOrCreate().sparkContext
    val rdd = sc.hadoopFile(path, classOf[RCFileInputFormat[LongWritable, BytesRefArrayWritable]], classOf[LongWritable],
      classOf[BytesRefArrayWritable], 2
    )
    val partitions = (0 until rdd.partitions.length).map(value => RCPartition(value, path))
    partitions.toArray

  }

  override def createReaderFactory() = new RCPartitionReaderFactory()
}

// reader factory
class RCPartitionReaderFactory extends PartitionReaderFactory {
  override def createReader(partition: InputPartition) = new
      RCPartitionReader(partition.asInstanceOf[RCPartition])
}


// parathion reader
class RCPartitionReader(inputPartition: RCPartition) extends PartitionReader[InternalRow] {

  var iterator: Iterator[String] = null

  @transient
  def next = {
    if (iterator == null) {

      val sc = SparkSession.builder.getOrCreate().sparkContext

      val rdd = sc.hadoopFile(inputPartition.path, classOf[RCFileInputFormat[LongWritable, BytesRefArrayWritable]], classOf[LongWritable],
        classOf[BytesRefArrayWritable], 2
      ).map(pair => {
        val cols = pair._2
        (0 until cols.size()).map(i=>{
          val brw = cols.get(i)
          Bytes.toString(brw.getData(), brw.getStart(), brw.getLength())
        }).mkString("\t")
      }).setName(inputPartition.path)
      val filterRDD = if (inputPartition.header) {
        val firstLine = rdd.first
        rdd.filter(_ != firstLine)
      }
      else rdd
      val partition = filterRDD.partitions(inputPartition.partitionNumber)
      iterator = filterRDD.iterator(partition, org.apache.spark.TaskContext.get())
    }
    iterator.hasNext
  }

  def get = {
    val line = iterator.next()
    InternalRow.fromSeq(line.split("\t").map(value => UTF8String.fromString(value)))
  }

  def close() = Unit

}
