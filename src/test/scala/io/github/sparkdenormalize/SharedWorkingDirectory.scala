package io.github.sparkdenormalize

import java.io.File

import scala.reflect.io.Directory

import org.scalatest.BeforeAndAfterAll

/** Provides a shared working directory for all tests in the suite */
trait SharedWorkingDirectory extends BeforeAndAfterAll {
  self: UnitSpec =>

  final val workingDir = new Directory(new File("./temp/"))

  final def clearWorkingDirectory(log: Boolean = true): Unit = {
    workingDir.deleteRecursively()
    workingDir.createDirectory()
    info(s"[WorkingDirectory] cleared: ${workingDir}")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    if (workingDir.exists) {
      assert(workingDir.isEmpty, s"${workingDir} must be empty")
    } else {
      workingDir.createDirectory()
    }
  }

  override def afterAll(): Unit = {
    super.afterAll()
    workingDir.deleteRecursively()
  }

}
