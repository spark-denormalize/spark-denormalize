package io.github.sparkdenormalize.common

import org.apache.spark.sql.{AnalysisException, Column, DataFrame}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.DataType

/** Utility methods related to [[org.apache.spark.sql.Column]] */
object SparkColumnUtils {

  /** Resolve attributes of a spark Column against a DataFrame
    *
    * @param column the Column with unresolved attribute references
    * @param dataFrame the DataFrame to resolve attributes against
    * @param attributePrefix the prefix of `dataFrame` with all unresolved attributes of `column` live under
    *                        (e.g. `dataFrame` may contain a "data" Struct field which we want to use as the implied
    *                         root for all unresolved attributes in `column`)
    * @param expectedDataTypes a map storing {attributeReference -> expected DataType}
    *                          if an unresolved attribute reference dose not conform to this expectation,
    *                          or is not found in the map, an exception will be thrown
    *                          (NOTE: if map is empty, no check will be performed)
    * @return a resolved Column
    * @throws UnresolvableAttributeException if an unresolved attribute in `column` is not resolvable against
    *                                        `dataFrame`
    */
  def resolveAttributes(
      column: Column,
      dataFrame: DataFrame,
      attributePrefix: Seq[String],
      expectedDataTypes: Map[String, DataType] = Map()
  ): Column = {
    val unresolved: Expression = column.expr
    val resolved: Expression = unresolved.transformDown(
      resolveAttributeTransformer(dataFrame, attributePrefix, expectedDataTypes)
    )
    new Column(resolved)
  }

  /** A transformer for spark Expression which resolves UnresolvedAttribute TreeNodes against a DataFrame */
  private def resolveAttributeTransformer(
      dataFrame: DataFrame,
      attributePrefix: Seq[String],
      expectedDataTypes: Map[String, DataType]
  ): PartialFunction[Expression, Expression] =
    new PartialFunction[Expression, Expression] {

      def apply(ex: Expression): Expression = {
        ex match {
          case unresolved: UnresolvedAttribute => {
            val unresolvedWithPrefix = UnresolvedAttribute(attributePrefix ++ unresolved.nameParts)
            val resolved: Expression =
              try {
                dataFrame(unresolvedWithPrefix.name).expr
              } catch {
                case ex: AnalysisException =>
                  throw new UnresolvableAttributeException(
                    s"field `${unresolved.name}` could not be resolved",
                    ex
                  )
              }

            // validate this attribute conforms to `expectedDataTypes`
            if (expectedDataTypes.nonEmpty) {
              expectedDataTypes.get(unresolved.name) match {
                case Some(expectedDataType: DataType) =>
                  if (resolved.dataType != expectedDataType) {
                    throw new UnresolvableAttributeException(
                      s"field `${unresolved.name}` has the wrong data type, expected ${expectedDataType}, " +
                        s"but got: ${resolved.dataType}"
                    )
                  }
                case None =>
                  throw new UnresolvableAttributeException(
                    s"field `${unresolved.name}` is not declared in `expectedDataTypes`"
                  )
              }
            }
            resolved
          }
          case _ => ex
        }
      }

      def isDefinedAt(ex: Expression) = {
        ex match {
          case ua: UnresolvedAttribute => true
          case _                       => false
        }
      }
    }

  /** An exception which reports the fact we cannot resolve an attribute for some reason */
  class UnresolvableAttributeException(message: String, cause: Throwable = None.orNull)
      extends Exception(message, cause)

}
