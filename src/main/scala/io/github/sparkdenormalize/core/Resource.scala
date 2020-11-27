package io.github.sparkdenormalize.core

/** A trait for the named resources of the app */
trait Resource {

  /** The kind of this Resource */
  lazy val kind: String = this.getClass.getSimpleName

  /** The name of this Resource */
  val name: String

  /** A human readable reference for this Resource */
  val refString: String = s"${kind}/${name}"

  override def toString: String = refString

}
