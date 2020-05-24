package org.apache.spark.sql.glue.util

import com.amazonaws.services.glue.model.{Database, GetDatabasesResult, GetTablesResult, Table}

import scala.annotation.tailrec
import scala.collection.JavaConverters._

object AWS {
  /**
   * Amazon has forgotten to create iterables for its types, so you have to write the same code to loop over
   * pages returned by the API over and over again.
   * This resolves by exploiting that every type Amazon uses, has an implicit interface. This interface is expressed by Q.
   * Unfortunately, every object has its own method name to get the values you are interested in.
   *
   * Except for S3, because that has a slightly different structure, so it doesn't fit the Q type
   *
   * @tparam V The result type, e.g. if {{{ type S = GetTablesResult }}} then {{{ type V = Table }}}
   */
  private trait ToIterable[V] {
    /**
     * Q is the supertype of S, it defines that S at least should have the following two methods:
     */
    type Q = {
      def getNextToken() : String
      def withNextToken(token: String) : S
    }

    /**
     * Tell the compiler to check that S is a subtype of Q
     * in java language: it means S has the interface defined above (getNextToken...)
     */
    type S <: Q

    /**
     * Use getV to get one page, then ask for a new token and recurse if not null, otherwise return results
     * Call is tail recursive
     *
     * @param q  the thing S containing V
     * @param xs the already collected results (not for the user to call)
     * @return
     */
    @tailrec
    final def apply(q: S, xs: Iterable[V] = Iterable.empty): Iterable[V] = {
      val res = getV(q)
      val newToken = q.getNextToken()
      if (newToken != null) apply(q.withNextToken(newToken), res ++ xs) else res ++ xs
    }

    /**
     * Method the user should implement, it describe how to get one page of items.
     *
     * @param s thing S containing some V's
     * @return one page of iterable V
     */
    protected def getV(s: S): Iterable[V]
  }

  val getTablesIterable = new ToIterable[Table] {
    override type S = GetTablesResult
    override protected def getV(s: GetTablesResult): Iterable[Table] = s.getTableList.asScala
  }
  val getDatabasesIterable = new ToIterable[Database] {
    override type S = GetDatabasesResult
    override protected def getV(s: GetDatabasesResult): Iterable[Database] = s.getDatabaseList.asScala

  }
}
