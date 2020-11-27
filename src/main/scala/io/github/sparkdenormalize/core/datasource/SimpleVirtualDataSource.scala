package io.github.sparkdenormalize.core.datasource

import io.github.sparkdenormalize.config.resource.spec.datasource.SimpleVirtualDataSourceSpec

class SimpleVirtualDataSource private (val name: String) extends VirtualDataSource {

  // TODO: we can probably use spark Expression or our SparkPlanGraph
  //       to ensure users only use a single DataSource as we decode the spec

}

object SimpleVirtualDataSource {

  /** Build a SimpleVirtualDataSource from a SimpleVirtualDataSourceSpec */
  def apply(name: String, spec: SimpleVirtualDataSourceSpec): SimpleVirtualDataSource = {
    // TODO: implement this
    new SimpleVirtualDataSource(name)
  }
}
