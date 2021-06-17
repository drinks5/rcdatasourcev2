package rc

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.ql.io.RCFile
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.LongWritable
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.SparkSession



object RC extends App {


  val dir = System.getProperty("user.home")
  System.setProperty("hadoop.home.dir", dir)

  val conf = new Configuration()
  val path = "src/main/resources/rc/**"
  val src = new Path(path)
  //createRcFile(src, conf)

  val session = SparkSession.builder
    .master("local[2]")
    .appName("rc")
    .getOrCreate()
  val sc = session.sparkContext
  Logger.getRootLogger.setLevel(Level.INFO)
  Logger.getLogger("spark").setLevel(Level.ERROR)
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.sparkproject").setLevel(Level.WARN)
  Logger.getLogger("sparkproject").setLevel(Level.WARN)
  LogManager.getRootLogger.setLevel(Level.ERROR)
  sc.setLogLevel("ERROR")


  /*
  val schema = StructType(Array(
    StructField("project", StringType, true),
    StructField("article", BooleanType, true),
    StructField("requests", DoubleType, true),
    StructField("bytes_served", DateType, true))
  )
  */
  val df = session.read
    .format("rc")
    .load(path)
  println("name\n-----\n")
  df.select("name").show()

  println("print schema\n-----\n")
  df.printSchema()
  println("show\n-----\n")
  df.show()
  println(
    "number of partitions in simple csv source is " + df.rdd.getNumPartitions)
  session.stop()

  //readRcFile(src, conf);

  private def createRcFile(src: Path, conf: Configuration) {
    val column = 4
    conf.setInt(RCFile.COLUMN_NUMBER_CONF_STR, column); // 列数
    conf.setInt(RCFile.Writer.COLUMNS_BUFFER_SIZE_CONF_STR, 4 * 1024 * 1024); // 决定行数参数一
    conf.setInt(RCFile.RECORD_INTERVAL_CONF_STR, 3); // 决定行数参数二
    val fs = FileSystem.get(conf)
    val writer = new RCFile.Writer(fs, conf, src)
    val cols = new BytesRefArrayWritable(column); // 列数，可以动态获取
    var count = 0;
    val strings = List("name,age,gender,province",
      "黄俊,28,male,湖南",
      "叶从周,25,male,上海",
      "周舟,25,male,江苏",
      "罗力宇,25,male,上海")
    strings.map(x => {
      val splits = x.split(",")
      count = 0
      splits.map(split => {
        val byte = Bytes.toBytes(split)
        val col = new BytesRefWritable(byte, 0, byte.length)
        println(f"count: $count, col: $split")
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
     readerByRow(reader);
    //readerByCol(reader);
    val metadata = reader.getMetadata
      println("metadata is :" + metadata)
    reader.close();
  }

  def readerByRow(reader: RCFile.Reader) {
    // 已经读取的行数
    val rowID = new LongWritable();
    // 一个行组的数据
    val cols = new BytesRefArrayWritable();
    var brw: BytesRefWritable = null;
    while (reader.next(rowID)) {
      reader.getCurrentRow(cols)
      brw = null
      val sb = new StringBuilder()
      for (i <- 0 to (cols.size() - 1))
      {
        brw = cols.get(i);
        // 根据start 和 length 获取指定行-列数据
        sb.append(Bytes.toString(brw.getData(), brw.getStart(),
          brw.getLength()));
        if (i < cols.size() - 1) sb.append("\t")
      }
      println(sb.toString());
    }
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