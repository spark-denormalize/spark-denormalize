package io.github.sparkdenormalize.config.resource.spec.datasource

import io.circe.{Decoder, DecodingFailure, HCursor}
import io.circe.generic.extras.semiauto.deriveConfiguredDecoder
import io.github.sparkdenormalize.config.resource.decoder.SparkDataType.decodeStructField
import io.github.sparkdenormalize.config.resource.spec.MetadataApplicatorReference
import org.apache.spark.sql.types.StructField

/** A data source which is accessed through JDBC */
case class JDBCDataSourceSpec(
    connection: String,
    driverClass: String,
    credentials: JDBCDataSourceSpec.CredentialsConfig,
    tableReference: String,
    tablePredicates: List[String], // default: List()
    snapshot: SnapshotConfig,
    schemaFields: Option[List[StructField]],
    metadataApplicators: List[MetadataApplicatorReference] // default: List()
) extends DataSourceSpec {
  require(connection.nonEmpty, "`connection` must be non-empty")
  require(driverClass.nonEmpty, "`driverClass` must be non-empty")
  require(tableReference.nonEmpty, "`tableReference` must be non-empty")
}

object JDBCDataSourceSpec {

  // tells IDE's we need these Decoder in scope so they wont remove it as an 'unused' import
  implicitly[Decoder[StructField]]

  // ========================
  // Circe Decoders
  // ========================
  implicit val decodeJDBCDataSourceSpec: Decoder[JDBCDataSourceSpec] =
    new Decoder[JDBCDataSourceSpec] {
      final def apply(c: HCursor): Decoder.Result[JDBCDataSourceSpec] =
        for {
          connection <- c.get[String]("connection")
          driverClass <- c.get[String]("driverClass")
          credentials <- c.get[CredentialsConfig]("credentials")
          tableReference <- c.get[String]("tableReference")
          tablePredicates <- c.getOrElse[List[String]]("tablePredicates")(List())
          snapshot <- c.get[SnapshotConfig]("snapshot")
          schemaFields <- c.get[Option[List[StructField]]]("schemaFields")
          metadataApplicators <-
            c.getOrElse[List[MetadataApplicatorReference]]("metadataApplicators")(List())
        } yield {
          try {
            JDBCDataSourceSpec(
              connection = connection,
              driverClass = driverClass,
              credentials = credentials,
              tableReference = tableReference,
              tablePredicates = tablePredicates,
              snapshot = snapshot,
              schemaFields = schemaFields,
              metadataApplicators = metadataApplicators
            )
          } catch {
            case ex: Exception =>
              return Left(DecodingFailure(ex.getMessage, c.history)) // scalastyle:ignore return
          }
        }
    }

  implicit val decodeCredentialsConfig: Decoder[CredentialsConfig] =
    new Decoder[CredentialsConfig] {
      final def apply(c: HCursor): Decoder.Result[CredentialsConfig] =
        for {
          fromPlaintext <- c.get[Option[CredentialsFromPlaintext]]("fromPlaintext")
          fromEnvironment <- c.get[Option[CredentialsFromEnvironment]]("fromEnvironment")
        } yield {
          try {
            CredentialsConfig(
              fromPlaintext = fromPlaintext,
              fromEnvironment = fromEnvironment
            )
          } catch {
            case ex: Exception =>
              return Left(DecodingFailure(ex.getMessage, c.history)) // scalastyle:ignore return
          }
        }
    }

  implicit val decodeCredentialsFromPlaintext: Decoder[CredentialsFromPlaintext] =
    deriveConfiguredDecoder[CredentialsFromPlaintext]

  implicit val decodeCredentialsFromEnvironment: Decoder[CredentialsFromEnvironment] =
    new Decoder[CredentialsFromEnvironment] {
      final def apply(c: HCursor): Decoder.Result[CredentialsFromEnvironment] =
        for {
          userVariable <- c.get[String]("userVariable")
          passwordVariable <- c.get[String]("passwordVariable")
        } yield {
          try {
            CredentialsFromEnvironment(
              userVariable = userVariable,
              passwordVariable = passwordVariable
            )
          } catch {
            case ex: Exception =>
              return Left(DecodingFailure(ex.getMessage, c.history)) // scalastyle:ignore return
          }
        }
    }

  // ========================
  // Classes
  // ========================
  case class CredentialsConfig(
      fromPlaintext: Option[CredentialsFromPlaintext],
      fromEnvironment: Option[CredentialsFromEnvironment]
  ) {
    // count the number of optional configs which were specified
    private val numSpecified: Long =
      Seq(fromPlaintext, fromEnvironment).foldLeft(0L) { case (total: Long, option: Option[_]) =>
        if (option.isDefined) {
          total + 1
        } else {
          total
        }
      }

    // ensure exactly one config is specified
    require(
      numSpecified == 1,
      s"`credentials` requires that exactly one config be specified, but got: ${numSpecified}"
    )

  }

  case class CredentialsFromPlaintext(
      user: String,
      password: String
  )

  case class CredentialsFromEnvironment(
      userVariable: String,
      passwordVariable: String
  ) {
    require(userVariable.nonEmpty, "`userVariable` must be non-empty")
    require(passwordVariable.nonEmpty, "`passwordVariable` must be non-empty")
  }

}
