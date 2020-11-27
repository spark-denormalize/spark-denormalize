package io.github.sparkdenormalize

import org.rogach.scallop.{ScallopConf, ScallopOption}

/** the parser for command line arguments */
private[sparkdenormalize] class ArgumentParser(args: Seq[String]) extends ScallopConf(args) {
  val config: ScallopOption[List[String]] = opt[List[String]](
    name = "config",
    descr = "a list of file paths containing config YAML",
    required = true
  )
  verify()
}
