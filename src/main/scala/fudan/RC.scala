package fudan

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.ql.io.RCFile
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.LongWritable
import org.apache.spark.sql.{SparkSession}
//import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.connector.catalog
//import org.apache.spark.sql.sourcses.v2


object RC extends App {
  val dir = System.getProperty("user.home")
  System.setProperty("hadoop.home.dir", dir)

  val conf = new Configuration()
  val src = new Path("./rcfile")
  val column = 4
  //createRcFile(src, conf)
  //readRcFile(src, conf);
  val sparkSession = SparkSession.builder
    .master("local[2]")
    .appName("example")
    .getOrCreate()

  val simpleCsvDf = sparkSession.read
    .format("fudan")
    .load("src/main/resources/adult.csv")

  simpleCsvDf.printSchema()
  simpleCsvDf.show()
  println(
    "number of partitions in simple csv source is " + simpleCsvDf.rdd.getNumPartitions)
  sparkSession.stop()

  private def ccreateRcFile(src: Path, conf: Configuration) {
    conf.setInt(RCFile.COLUMN_NUMBER_CONF_STR, column); // 列数
    conf.setInt(RCFile.Writer.COLUMNS_BUFFER_SIZE_CONF_STR, 4 * 1024 * 1024); // 决定行数参数一
    conf.setInt(RCFile.RECORD_INTERVAL_CONF_STR, 3); // 决定行数参数二
    val fs = FileSystem.get(conf)
    val writer = new RCFile.Writer(fs, conf, src)
    val cols = new BytesRefArrayWritable(column); // 列数，可以动态获取
    var count = 0;
    val strings = List("1,true,123.123,2012-10-24 08:55:00",
      "2,false,1243.5,2012-10-25 13:40:00",
      "3,false,24453.325,2008-08-22 09:33:21.123",
      "4,false,243423.325,2007-05-12 22:32:21.33454",
      "5,true,243.325,1953-04-22 09:11:33")
    strings.map(x => {
      val splits = x.split(",")
      count = 0
      splits.map(split => {
        val byte = Bytes.toBytes(split)
        val col = new BytesRefWritable(byte, 0, byte.length)
        println(f"count: $count, col: $col")
        cols.set(count, col)
        count += 1
      })
      writer.append(cols)
    })
    writer.close()
  }

  /**
   * 读取解析一个RCF file
   *
   * @param src
   * @param conf
   * @throws IOException
   */
  private def readRcFile(src: Path, conf: Configuration) {
    // 需要获取的列，必须指定，具体看ColumnProjectionUtils中的设置方法
    ColumnProjectionUtils.setFullyReadColumns(conf);
    val fs = FileSystem.get(conf);
    val reader = new RCFile.Reader(fs, src, conf);
    // readerByRow(reader);
    readerByCol(reader);
    reader.close();
  }

  def readerByRow(reader: RCFile.Reader) {
    // 已经读取的行数
    val rowID = new LongWritable();
    // 一个行组的数据
    val cols = new BytesRefArrayWritable();
  }

  def readerByCol(reader: RCFile.Reader) {
    // 一个行组的数据
    var cols = new BytesRefArrayWritable();
    var count = 0
    var brw: BytesRefWritable = null;
    while (reader.nextBlock()) {
      for (count <- 0 to 3) {
        cols = reader.getColumn(count, cols);
        val sb = new StringBuilder();
        for (i <- 0 to (cols.size() - 1)){
          brw = cols.get(i)
          // 根据start 和 length 获取指定行-列数据
          sb.append(Bytes.toString(brw.getData(), brw.getStart(),
            brw.getLength()));
          if (i < cols.size() - 1) {
            sb.append("\t")
          }
        }
        println(sb.toString())
      }
    }
  }
}