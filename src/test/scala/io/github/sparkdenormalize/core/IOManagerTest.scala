package io.github.sparkdenormalize.core

import java.io.IOException
import java.nio.charset.StandardCharsets

import scala.reflect.io.File

import io.github.sparkdenormalize.{SharedSparkSession, SharedWorkingDirectory, UnitSpec}

class IOManagerTest extends UnitSpec with SharedSparkSession with SharedWorkingDirectory {

  // ----------------
  // SHARED VALUES
  // ----------------
  val file: File = workingDir / File("test.txt")
  val fileContent: String =
    """line 1
      |是是是是
      |😀😀😀""".stripMargin

  // ----------------
  // TESTS
  // ----------------
  behavior of "IOManager"

  it should "write a file, and read it back without corruption" in {
    // ensure working directory is clean
    clearWorkingDirectory()

    val io = new IOManager()
    io.writeFile(file.path, fileContent, overwrite = false)

    // assert content has not changed
    assertResult(fileContent)(io.readFile(file.path))
  }

  it should "fail to write file content incompatible with its charset" in {
    val io = new IOManager()
    assertThrows[IOException](
      io.writeFile(file.path, fileContent, overwrite = false, charset = StandardCharsets.US_ASCII)
    )
  }

}
