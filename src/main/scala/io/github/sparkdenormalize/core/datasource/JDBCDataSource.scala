package io.github.sparkdenormalize.core.datasource

import io.github.sparkdenormalize.config.resource.spec.datasource.JDBCDataSourceSpec

class JDBCDataSource private (val name: String) extends DataSource {}

object JDBCDataSource {

  /** Build a JDBCDataSource from a JDBCDataSourceSpec */
  def apply(name: String, spec: JDBCDataSourceSpec): JDBCDataSource = {
    // TODO: implement this
    new JDBCDataSource(name)
  }

}
