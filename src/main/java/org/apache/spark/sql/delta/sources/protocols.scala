package org.apache.spark.sql.delta.sources

import org.apache.spark.sql.Row

/**
 * 1/11/2019 WilliamZhu(allwefantasy@gmail.com)
 */
case class BFItem(fileName: String, bf: String, size: Int, sizePretty: String)

case class FullOuterJoinRow(left: Row, right: Row, leftPresent: Boolean, rightPresent: Boolean)
