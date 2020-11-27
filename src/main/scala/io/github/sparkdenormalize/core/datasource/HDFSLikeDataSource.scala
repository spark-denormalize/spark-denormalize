package io.github.sparkdenormalize.core.datasource

import io.github.sparkdenormalize.config.resource.spec.datasource.HDFSLikeDataSourceSpec

class HDFSLikeDataSource private (val name: String) extends DataSource {}

object HDFSLikeDataSource {

  /** Build a HDFSLikeDataSource from a HDFSLikeDataSourceSpec */
  def apply(name: String, spec: HDFSLikeDataSourceSpec): HDFSLikeDataSource = {
    // TODO: implement this
    new HDFSLikeDataSource(name)
  }

}
