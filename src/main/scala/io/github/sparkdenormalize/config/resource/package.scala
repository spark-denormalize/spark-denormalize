package io.github.sparkdenormalize.config

import io.circe.generic.extras.Configuration

package object resource {

  // TODO: get rid of any remaining automatic Decoder definitions then:
  //  - remove `circe-generic` and `circe-generic-extras` from build.sbt
  //  - remove these `package.scala` files

  // enable default values for `deriveConfiguredDecoder` from `circe-generic-extras`
  // NOTE: this is needed in each `package.scala` file under `io.github.sparkdenormalize.config`
  implicit val circeConfiguration: Configuration = Configuration.default.withDefaults

}
