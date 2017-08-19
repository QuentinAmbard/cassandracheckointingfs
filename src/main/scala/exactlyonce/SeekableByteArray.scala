package exactlyonce

import java.io.InputStream

import org.apache.hadoop.fs.{PositionedReadable, Seekable}


class SeekableByteArray(recommendationBytes: Array[Byte]) extends InputStream with Seekable with PositionedReadable {

  import java.io.{ByteArrayInputStream, IOException}

  private val inputStream: ByteArrayInputStream = new ByteArrayInputStream(recommendationBytes)
  private var pos = 0L

  @throws[IOException]
  def seek(pos: Long): Unit = {
    inputStream.skip(pos)
    this.pos = pos
  }

  @throws[IOException]
  def getPos: Long = pos

  @throws[IOException]
  def seekToNewSource(targetPos: Long) = throw new UnsupportedOperationException("seekToNewSource is not supported.")

  @throws[IOException]
  def read: Int = {
    val value = inputStream.read
    if (value > 0) pos += 1
    value
  }

  @throws[IOException]
  def read(position: Long, buffer: Array[Byte], offset: Int, length: Int): Int = {
    val currPos = pos
    inputStream.skip(position)
    val bytesRead = inputStream.read(buffer, offset, length)
    inputStream.skip(currPos)
    bytesRead
  }

  @throws[IOException]
  def readFully(position: Long, buffer: Array[Byte], offset: Int, length: Int): Unit = {
    val currPos = pos
    inputStream.skip(position)
    inputStream.read(buffer, offset, length)
    inputStream.skip(currPos)
  }

  @throws[IOException]
  def readFully(position: Long, buffer: Array[Byte]): Unit = {
    val currPos = pos
    inputStream.skip(position)
    inputStream.read(buffer)
    inputStream.skip(currPos)
  }
}
