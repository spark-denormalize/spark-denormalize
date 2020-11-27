package io.github.sparkdenormalize.core

import java.io.IOException
import java.nio.{ByteBuffer, CharBuffer}
import java.nio.charset.{Charset, CodingErrorAction, StandardCharsets}

import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FileSystem, FSDataInputStream, FSDataOutputStream, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

class IOManager(implicit ss: SparkSession) extends Logging {

  // hadoop FileSystem object, configured from SparkContext
  val fileSystem: FileSystem = {
    val conf = ss.sparkContext.hadoopConfiguration
    FileSystem.get(conf)
  }

  /** Read the content of a file into a string */
  def writeFile(pathString: String, content: String, overwrite: Boolean): Unit = {
    val path = new Path(pathString)
    writeFile(path, content, overwrite, StandardCharsets.UTF_8)
  }

  /** Read the content of a file into a string, with a specific charset */
  def writeFile(pathString: String, content: String, overwrite: Boolean, charset: Charset): Unit = {
    val path = new Path(pathString)
    writeFile(path, content, overwrite, charset)
  }

  /** Write the content of a string to a file
    *
    * @param path the path of the file
    * @param content the string to write
    * @param overwrite overwrite any file at this path, if one already exists
    * @param charset the character encoding to use
    * @throws IOException if overwrite=false and there is already a file at this path, or if another error occurs
    */
  def writeFile(
      path: Path,
      content: String,
      overwrite: Boolean,
      charset: Charset = StandardCharsets.UTF_8
  ): Unit = {
    // encode string to byte array
    val contentBytes: Array[Byte] =
      try {
        val byteBuffer: ByteBuffer = charset
          .newEncoder()
          .onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT)
          .encode(CharBuffer.wrap(content))
        byteBuffer.array().slice(0, byteBuffer.limit())
      } catch {
        case ex: Exception =>
          throw new IOException(
            s"""error encoding string with charset=${charset} for file at path: ${path}
               | -------- STRING START --------
               |${content}
               | --------- STRING END ---------""".stripMargin,
            ex
          )
      }

    // open output stream
    val outputStream: FSDataOutputStream =
      try {
        fileSystem.create(path, overwrite)
      } catch {
        case ex: Exception =>
          throw new IOException(s"error opening FSDataOutputStream at path: ${path}", ex)
      }

    // write bytes to file
    try {
      outputStream.write(contentBytes)
    } catch {
      case ex: Exception =>
        throw new IOException(s"error writing to file at path: ${path}", ex)
    } finally {
      outputStream.close()
    }
  }

  /** Read the content of a file into a string. */
  def readFile(pathString: String): String = {
    val path = new Path(pathString)
    readFile(path, StandardCharsets.UTF_8)
  }

  /** Read the content of a file into a string, with a specific charset. */
  def readFile(pathString: String, charset: Charset): String = {
    val path = new Path(pathString)
    readFile(path, charset)
  }

  /** Read the content of a file into a string
    *
    * @param path the path of the file
    * @param charset the character encoding to use
    * @return the content of the file as a string
    * @throws IOException if an error is encountered while reading the file
    */
  def readFile(path: Path, charset: Charset = StandardCharsets.UTF_8): String = {
    // open input stream
    val inputStream: FSDataInputStream =
      try {
        fileSystem.open(path)
      } catch {
        case ex: Exception =>
          throw new IOException(s"error opening FSDataInputStream at path: ${path}", ex)
      }

    // read byte array from file
    val content: Array[Byte] =
      try {
        IOUtils.toByteArray(inputStream)
      } catch {
        case ex: Exception =>
          throw new IOException(s"error reading from file at path: ${path}", ex)
      } finally {
        inputStream.close()
      }

    // decode byte array to string
    try {
      charset
        .newDecoder()
        .onMalformedInput(CodingErrorAction.REPORT)
        .onUnmappableCharacter(CodingErrorAction.REPORT)
        .decode(ByteBuffer.wrap(content))
        .toString
    } catch {
      case ex: Exception =>
        throw new IOException(
          s"error decoding bytes with charset=${charset}, from file at path: ${path}",
          ex
        )
    }
  }

  // TODO: list files in folder
  // TODO: list folders in folder

}
