/*
 *
 * This file is part of the Documentum Elasticus project.
 * Copyright (c) 2010-2011 Limited Liability Company «MEZHGALAKTICHESKIJ TORGOVYJ ALIANS»
 * Author: Alexey Aksenov
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Global License version 3
 * as published by the Free Software Foundation with the addition of the
 * following permission added to Section 15 as permitted in Section 7(a):
 * FOR ANY PART OF THE COVERED WORK IN WHICH THE COPYRIGHT IS OWNED
 * BY Limited Liability Company «MEZHGALAKTICHESKIJ TORGOVYJ ALIANS»,
 * Limited Liability Company «MEZHGALAKTICHESKIJ TORGOVYJ ALIANS» DISCLAIMS
 * THE WARRANTY OF NON INFRINGEMENT OF THIRD PARTY RIGHTS.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Global License for more details.
 * You should have received a copy of the GNU Affero General Global License
 * along with this program; if not, see http://www.gnu.org/licenses or write to
 * the Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor,
 * Boston, MA, 02110-1301 USA, or download the license from the following URL:
 * http://www.gnu.org/licenses/agpl.html
 *
 * The interactive user interfaces in modified source and object code versions
 * of this program must display Appropriate Legal Notices, as required under
 * Section 5 of the GNU Affero General Global License.
 *
 * In accordance with Section 7(b) of the GNU Affero General Global License,
 * you must retain the producer line in every report, form or document
 * that is created or manipulated using Documentum Elasticus.
 *
 * You can be released from the requirements of the license by purchasing
 * a commercial license. Buying such a license is mandatory as soon as you
 * develop commercial activities involving the Documentum Elasticus software without
 * disclosing the source code of your own applications.
 * These activities include: offering paid services to customers,
 * serving files in a web or/and network application,
 * shipping Documentum Elasticus with a closed source product.
 *
 * For more information, please contact Documentum Elasticus Team at this
 * address: ezh@ezh.msk.ru
 *
 */

package org.digimead.documentumelasticus.database

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.SQLException
import java.sql.Types
import org.slf4j.Logger

trait XDBUtils {
  protected val log: Logger
  def addTablePrefix(sBaseName: String): String = {
    //var sReturn = "" //sTablePrefix
//    if (0 < sTablePrefix.length())
//      sReturn += "." + sBaseName
    //sReturn
    sBaseName
  }
  def createTable(connection: Connection, sCreateStatement: String, sTableName: String): Boolean = {
    if (!executeStatement(connection, sCreateStatement)) {
      log.error("could not create the table " + sTableName)
      return false
    }
    log.debug("create table \"" + sTableName + "\"")
    true
  }
  def executeStatement(connection: Connection, sStatement: String): Boolean = {
    val xStatement = connection.createStatement()
    try {
      xStatement.execute(sStatement);
    } catch {
      case e: SQLException => {
          log.error("statement failed: " + sStatement, e)
          xStatement.close()
          return false
      }
    }
    xStatement.close()
    true
  }
  def selectRow(connection: Connection, sStatement: String): Array[Any] = {
    val statement = connection.createStatement()
    val rs = statement.executeQuery(sStatement)
    if (rs.next()) {
      val rsmd = rs.getMetaData
      val result: Array[Any] = Array(rsmd.getColumnCount())
      for(i <- 0 until result.length) {
        rsmd.getColumnType(i+1) match {
          case Types.VARCHAR => result(i) = rs.getString(i+1)
          case Types.ARRAY => result(i) = rs.getArray(i+1)
          case Types.BIGINT => result(i) = rs.getLong(i+1)
          case Types.BOOLEAN => result(i) = rs.getBoolean(i+1)
          case Types.CHAR => result(i) = rs.getString(i+1)
          case Types.DATE => result(i) = rs.getDate(i+1)
          case Types.DECIMAL => result(i) = rs.getInt(i+1)
          case Types.DOUBLE => result(i) = rs.getDouble(i+1)
          case Types.FLOAT => result(i) = rs.getFloat(i+1)
          case Types.INTEGER => result(i) = rs.getInt(i+1)
          case Types.NULL => result(i) = null
          case Types.TIMESTAMP => result(i) = rs.getTimestamp(i+1)
          case a => throw new RuntimeException("Unsupported jdbc SQL type: " + a)
        }
      }
      rs.close()
      statement.close()
      return result
    }
    rs.close()
    statement.close()
    Array()
  }
  def addStatementParameters(statement: PreparedStatement, args: Array[Any]) = {
    for(i <- 0 until args.length) {
      args(i) match {
        case a:String => statement.setString(i+1, a)
        case a:Long => statement.setLong(i+1, a)
        case a:Boolean => statement.setBoolean(i+1, a)
        case a:Double => statement.setDouble(i+1, a)
        case a:Float => statement.setFloat(i+1, a)
        case a:Int => statement.setInt(i+1, a)
        case a => throw new RuntimeException("Unsupported jdbc SQL type: " + a)
      }
    }
  }
}
