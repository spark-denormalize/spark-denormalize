package io.github.sparkdenormalize.config.resource

import io.circe.Decoder
import io.circe.generic.extras.semiauto.deriveEnumerationDecoder

object ResourceKinds {

  // ========================
  // Circe Decoders
  // ========================
  // TODO: allow users to put plural forms like `DataRelationships`
  implicit val decodeResourceKind: Decoder[ResourceKind] =
    deriveEnumerationDecoder[ResourceKind]

  // TODO: allow users to put plural forms like `HDFSLikeDataSources`
  implicit val decodeDataSourceKind: Decoder[DataSourceKind] =
    deriveEnumerationDecoder[DataSourceKind]

  // ========================
  // Enums
  // ========================
  /** The base resource kind */
  sealed trait ResourceKind

  /** The base DataSource resource kind */
  sealed trait DataSourceKind extends ResourceKind

  // DataSource
  case object ComplexVirtualDataSource extends DataSourceKind
  case object HDFSLikeDataSource extends DataSourceKind
  case object JDBCDataSource extends DataSourceKind
  case object SimpleVirtualDataSource extends DataSourceKind

  // DataRelationships
  case object DataRelationship extends ResourceKind

  // MetadataApplicators
  case object MetadataApplicator extends ResourceKind

  // OutputDatasets
  case object OutputDataset extends ResourceKind

}
