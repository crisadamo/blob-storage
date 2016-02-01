import java.io.{PrintWriter, FileOutputStream, FileInputStream}
import java.nio.channels.FileChannel
import java.util.concurrent.{ConcurrentHashMap}
import scala.annotation.tailrec
import scala.util.Try
import sys.process._
import scala.io.{Source, StdIn}


object BlobStorage {
  var blockId: Int = 0
  val metadataStore = new MetadataStore()

  def main(args: Array[String]) {
    println("Blobstorage: usage")
    println("  - add /path/to/file")
    println("  - get filename")
    println("  - show-keys")
    println("  - exit")

    metadataStore.readJournal("/home/cristian/blobstorage/")
    BlobBlockManager.allocateBlockId(blockId)
    loop()
  }

  @tailrec
  def loop(): Unit = {
    val addCommand = "add\\s+(.*)".r
    val getCommand = "get\\s+(.*)".r

    val line = StdIn.readLine("Enter a command :> ")
    line match {
      case addCommand(path) =>
        println(s"  >> Writing blob ($path) to block ID $blockId...")
        val blobData = BlobBlockManager.write(path, blockId)
        val data: (Long, Long) = blobData.getOrElse(-1L, -1L)

        val blobKey = path.substring(path.lastIndexOf("/"))
        println(s"  >>>> Writing blob metadata: k: $blobKey Id: $blockId Size: ${data._1} Offset: ${data._2}")
        metadataStore.put(path.substring(path.lastIndexOf("/")+1), blockId, data._1, data._2)

        println(s"  >> Done.")
        loop()
      case getCommand(filename) =>
        println(s"  << Getting blob named $filename from block ID $blockId...")

        metadataStore.get(filename) match {
          case Some((bid, s, l)) =>
            BlobBlockManager.read("/home/cristian/blobstorage/tmp/"+filename, bid, l, s)
            println("  << block extracted!")
          case _ => println(s"  << No such file $filename")
        }
        loop()
      case "show-keys" =>
        println("Keys :> " + metadataStore.getKeys())

      case "exit" =>
        println("creating metadata journals...")
        metadataStore.writeJournal("/home/cristian/blobstorage/")
        println("Goodbye!")
        sys.exit(0)
      case _ => println("Wrong command!")
    }
  }
}

object BlobBlockManager {

  val blobstorePath = "/home/cristian/blobstorage/"

  def blobBlockNameFormat(id: Int) = s"block-$id.blob"

  def allocateBlockId(id: Int): Boolean = {
    val okay = s"fallocate -l 256m $blobstorePath/block-$id.blob" !

    if (okay == 0) true else false
  }

  // Zero copy
  def write(source: String, blockId: Int): Option[(Long, Long)] = {
    val destination = blobstorePath + blobBlockNameFormat(blockId)
    zeroCopy(source, destination)
  }

  def read(destination: String, blockId: Int, offset: Long, size: Long) = {
    val source = blobstorePath + blobBlockNameFormat(blockId)
    zeroCopy(source, destination, offset, Some(size))
  }

  def zeroCopy(source: String, destination: String, offset: Long = 0L, size: Option[Long] = None): Option[(Long, Long)] = {
    var sourceChannel: FileChannel = null
    var destinationChannel: FileChannel = null

    try {
      sourceChannel = new FileInputStream(source).getChannel()
      destinationChannel = new FileOutputStream(destination).getChannel()

      val destSize = destinationChannel.size()
      sourceChannel.transferTo(offset, size.getOrElse(sourceChannel.size()), destinationChannel)
      Some(sourceChannel.size() -> destSize)
    } finally {
      if (sourceChannel != null) {
        sourceChannel.close()
      }
      if (destinationChannel != null) {
        destinationChannel.close()
      }
    }
  }
}


class MetadataStore {
  private[this] val metadata = new ConcurrentHashMap[String, (Int, Long, Long)]()

  def get(key: String): Option[(Int, Long, Long)] = Try(metadata.get(key)).toOption

  def getKeys() = metadata.keySet().

  def put(key: String, id: Int, size: Long, offset: Long) = metadata.put(key, (id, size, offset))

  def writeJournal(path: String) = {
    val writer = new PrintWriter(path + ".metadata-journal", "UTF-8")
    val it = metadata.keySet().iterator()
    while (it.hasNext) {
      val key: String = it.next()
      val value: (Int, Long, Long) = metadata.get(key)
      if (! (value == null)) {
        writer.println(s"$key,${value._1},${value._2},${value._3}")
      }
    }
    writer.close();
  }

  def readJournal(path: String) = {
    for (line <- Source.fromFile(path + ".metadata-journal").getLines()) {
      val values = line.split("\\,", -1)
      put(values(0), values(1).toInt, values(2).toLong, values(3).toLong)
      values(0)
    }
  }

  def getLastSavedBlockId(): Option[Long] = {
    None
  }

}
