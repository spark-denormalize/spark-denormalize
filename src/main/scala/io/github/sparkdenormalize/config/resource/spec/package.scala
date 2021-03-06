package io.github.sparkdenormalize.config.resource

import io.circe.generic.extras.Configuration

package object spec {

  // enable default values for `deriveConfiguredDecoder` from `circe-generic-extras`
  // NOTE: this is needed in each `package.scala` file under `io.github.sparkdenormalize.config`
  implicit val circeConfiguration: Configuration = Configuration.default.withDefaults

}
