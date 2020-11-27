package io.github.sparkdenormalize.core.metadata

import io.github.sparkdenormalize.config.resource.spec.MetadataApplicatorSpec
import io.github.sparkdenormalize.core.Resource

class MetadataApplicator private (val name: String) extends Resource {}

object MetadataApplicator {

  /** Build a MetadataApplicator from a MetadataApplicatorSpec */
  def apply(name: String, spec: MetadataApplicatorSpec): MetadataApplicator = {
    // TODO: implement this
    new MetadataApplicator(name)
  }
}
