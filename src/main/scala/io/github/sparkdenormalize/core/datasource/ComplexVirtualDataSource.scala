package io.github.sparkdenormalize.core.datasource

import io.github.sparkdenormalize.config.resource.spec.datasource.ComplexVirtualDataSourceSpec

class ComplexVirtualDataSource private (val name: String) extends VirtualDataSource {}

object ComplexVirtualDataSource {

  /** Build a ComplexVirtualDataSource from a ComplexVirtualDataSourceSpec */
  def apply(name: String, spec: ComplexVirtualDataSourceSpec): ComplexVirtualDataSource = {
    // TODO: implement this
    new ComplexVirtualDataSource(name)
  }

}
