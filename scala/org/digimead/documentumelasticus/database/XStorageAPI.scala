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

import com.sun.star.util.DateTime
import java.sql.ResultSet
import java.sql.Types
import java.util.UUID
import org.digimead.documentumelasticus.helper._

trait XStorageAPI extends XDatabase with XDBUtils {
  def storageExists(id: Long): Boolean = {
    val resultset = storageGet("1", "WHERE \"id\" = ?", Array(id))
    if (resultset.next()) {
      resultset.getStatement().close()
      return true
    } else {
      resultset.getStatement().close()
      return false
    }
  }
  def storageExists(name: String): Boolean = {
    val resultset = storageGet("1", "WHERE \"name\" = ?", Array(name))
    if (resultset.next()) {
      resultset.getStatement().close()
      return true
    } else {
      resultset.getStatement().close()
      return false
    }
  }
  def storageIDByName(name: String): Long = {
    val resultset = storageGet("\"id\"", "WHERE \"name\" = ?", Array(name))
    if (!resultset.next()) {
      throw new RuntimeException("Storage with NAME = '" + name + "' not found")
    }
    val result = resultset.getLong(1)
    resultset.getStatement.close()
    result
  }
  def storageCreate(serviceName: String, storageName: String, storageLocation: String, userID: Long): Long = {
    val sTable = addTablePrefix("STORAGE")
    val statement = connection.prepareStatement("INSERT INTO \"" + sTable + "\" (\"uuid\", \n"
                                                    +"\"name\", \"service\", \"URL\", \"owner_id\", \n"
                                                    +"\"group_id\", \"last_user\", \"last_time\", \n"
                                                    +"\"created_at\", \"updated_at\", \n"
                                                    +"\"timestamp\") VALUES ("
                                                    +"?, ?, ?, ?, ?, ?, ?, now, now, now, now)"
    )
    val uuid = UUID.randomUUID()
    statement.setString(1, uuid.toString)
    statement.setString(2, storageName)
    statement.setString(3, serviceName)
    statement.setString(4, storageLocation)
    statement.setLong(5, userID)
    if (userGetGroup(userID) == 0) {
      statement.setNull(6, Types.BIGINT)
    } else {
      statement.setLong(6, userGetGroup(userID))
    }
    statement.setLong(7, userID)
    statement.executeUpdate()
    statement.close()
    val resultset = storageGet("\"id\"", "WHERE \"uuid\" = ?", Array(uuid.toString))
    if (!resultset.next()) {
      throw new RuntimeException("New storage with UUID = " + uuid.toString + " not found")
    }
    val result = resultset.getLong(1)
    resultset.getStatement.close()
    result
  }
  def storageGetUUID(id: Long): String = {
    val resultset = storageGet("\"uuid\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("Storage with ID = " + id + " not found")
    }
    val result = resultset.getString(1)
    resultset.getStatement.close()
    result
  }
  def storageSetUUID(id: Long, arg: String, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default storage record with id 1 is read only")
    val sTable = addTablePrefix("STORAGE")
    val statement = connection.prepareStatement("UPDATE \"" + sTable + "\" "
                                                 +"SET \"uuid\" = ?, \"last_user\" = ?, \"last_time\" = now, \"timestamp\" = now "
                                                 +"WHERE \"id\" = ?")
    statement.setString(1, arg)
    statement.setLong(2, userID)
    statement.setLong(3, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("Storage with ID = " + id + " not found")
    statement.close()
  }
  def storageGetName(id: Long): String = {
    val resultset = storageGet("\"name\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("Storage with ID = " + id + " not found")
    }
    val result = resultset.getString(1)
    resultset.getStatement.close()
    result
  }
  def storageSetName(id: Long, arg: String, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default storage record with id 1 is read only")
    val sTable = addTablePrefix("STORAGE")
    val statement = connection.prepareStatement("UPDATE \"" + sTable + "\" "
                                                 +"SET \"name\" = ?, \"last_user\" = ?, \"last_time\" = now, \"timestamp\" = now "
                                                 +"WHERE \"id\" = ?")
    statement.setString(1, arg)
    statement.setLong(2, userID)
    statement.setLong(3, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("Storage with ID = " + id + " not found")
    statement.close()
  }
  def storageGetService(id: Long): String = {
    val resultset = storageGet("\"service\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("Storage with ID = " + id + " not found")
    }
    val result = resultset.getString(1)
    resultset.getStatement.close()
    result
  }
  def storageSetService(id: Long, arg: String, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default storage record with id 1 is read only")
    val sTable = addTablePrefix("STORAGE")
    val statement = connection.prepareStatement("UPDATE \"" + sTable + "\" "
                                                 +"SET \"service\" = ?, \"last_user\" = ?, \"last_time\" = now, \"timestamp\" = now "
                                                 +"WHERE \"id\" = ?")
    statement.setString(1, arg)
    statement.setLong(2, userID)
    statement.setLong(3, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("Storage with ID = " + id + " not found")
    statement.close()
  }
  def storageGetURL(id: Long): String = {
    val resultset = storageGet("\"URL\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("Storage with ID = " + id + " not found")
    }
    val result = resultset.getString(1)
    resultset.getStatement.close()
    result
  }
  def storageSetURL(id: Long, arg: String, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default storage record with id 1 is read only")
    val sTable = addTablePrefix("STORAGE")
    val statement = connection.prepareStatement("UPDATE \"" + sTable + "\" "
                                                 +"SET \"URL\" = ?, \"last_user\" = ?, \"last_time\" = now, \"timestamp\" = now "
                                                 +"WHERE \"id\" = ?")
    statement.setString(1, arg)
    statement.setLong(2, userID)
    statement.setLong(3, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("Storage with ID = " + id + " not found")
    statement.close()
  }
  def storageGetOwner(id: Long): Long = {
    val resultset = storageGet("\"owner_id\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("Storage with ID = " + id + " not found")
    }
    val result = resultset.getLong(1)
    resultset.getStatement.close()
    result
  }
  def storageSetOwner(id: Long, arg: Long, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default storage record with id 1 is read only")
    val sTable = addTablePrefix("STORAGE")
    val statement = connection.prepareStatement("UPDATE \"" + sTable + "\" "
                                                 +"SET \"owner_id\" = ?, \"last_user\" = ?, \"last_time\" = now, \"timestamp\" = now "
                                                 +"WHERE \"id\" = ?")
    if (arg != 0) {
      statement.setLong(1, arg)
    } else {
      statement.setNull(1, Types.BIGINT)
    }
    statement.setLong(2, userID)
    statement.setLong(3, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("Storage with ID = " + id + " not found")
    statement.close()
  }
  def storageGetGroup(id: Long): Long = {
    val resultset = storageGet("\"group_id\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("Storage with ID = " + id + " not found")
    }
    val result = resultset.getLong(1)
    resultset.getStatement.close()
    result
  }
  def storageSetGroup(id: Long, arg: Long, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default storage record with id 1 is read only")
    val sTable = addTablePrefix("STORAGE")
    val statement = connection.prepareStatement("UPDATE \"" + sTable + "\" "
                                                 +"SET \"group_id\" = ?, \"last_user\" = ?, \"last_time\" = now, \"timestamp\" = now "
                                                 +"WHERE \"id\" = ?")
    if (arg != 0) {
      statement.setLong(1, arg)
    } else {
      statement.setNull(1, Types.BIGINT)
    }
    statement.setLong(2, userID)
    statement.setLong(3, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("Storage with ID = " + id + " not found")
    statement.close()
  }
  def storageGetLastUser(id: Long): Long = {
    val resultset = storageGet("\"last_user\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("Storage with ID = " + id + " not found")
    }
    val result = resultset.getLong(1)
    resultset.getStatement.close()
    result
  }
  def storageSetLastUser(id: Long, arg: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default storage record with id 1 is read only")
    val sTable = addTablePrefix("STORAGE")
    val statement = connection.prepareStatement("UPDATE \"" + sTable + "\" SET \"last_user\" = ?, \"timestamp\" = now WHERE \"id\" = ?")
    if (arg != 0) {
      statement.setLong(1, arg)
    } else {
      statement.setNull(1, Types.BIGINT)
    }
    statement.setLong(2, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("Storage with ID = " + id + " not found")
    statement.close()
  }
  def storageGetLastTime(id: Long): DateTime = {
    val resultset = storageGet("\"last_time\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("Storage with ID = " + id + " not found")
    }
    val result = resultset.getTimestamp(1)
    resultset.getStatement.close()
    DT.toDateTime(result)
  }
  def storageSetLastTime(id: Long, arg: DateTime): Unit = {
    if (id == 1)
      throw new RuntimeException("Default storage record with id 1 is read only")
    val sTable = addTablePrefix("STORAGE")
    val statement = connection.prepareStatement("UPDATE \"" + sTable + "\" SET \"last_time\" = ?, \"timestamp\" = now WHERE \"id\" = ?")
    statement.setTimestamp(1, DT.toTimestamp(arg))
    statement.setLong(2, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("Storage with ID = " + id + " not found")
    statement.close()
  }
  def storageGetDescription(id: Long): String = {
    val resultset = storageGet("\"description\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("Storage with ID = " + id + " not found")
    }
    val result = resultset.getString(1)
    resultset.getStatement.close()
    result
  }
  def storageSetDescription(id: Long, arg: String, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default storage record with id 1 is read only")
    val sTable = addTablePrefix("STORAGE")
    val statement = connection.prepareStatement("UPDATE \"" + sTable + "\" "
                                                 +"SET \"description\" = ?, \"last_user\" = ?, \"last_time\" = now, \"timestamp\" = now "
                                                 +"WHERE \"id\" = ?")
    statement.setString(1, arg)
    statement.setLong(2, userID)
    statement.setLong(3, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("Storage with ID = " + id + " not found")
    statement.close()
  }
  def storageGetCreatedAt(id: Long): DateTime = {
    val resultset = storageGet("\"created_at\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("Storage with ID = " + id + " not found")
    }
    val result = resultset.getTimestamp(1)
    resultset.getStatement.close()
    DT.toDateTime(result)
  }
  def storageSetCreatedAt(id: Long, arg: DateTime, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default storage record with id 1 is read only")
    val sTable = addTablePrefix("STORAGE")
    val statement = connection.prepareStatement("UPDATE \"" + sTable + "\" "
                                                 +"SET \"created_at\" = ?, \"last_user\" = ?, \"last_time\" = now, \"timestamp\" = now "
                                                 +"WHERE \"id\" = ?")
    statement.setTimestamp(1, DT.toTimestamp(arg))
    statement.setLong(2, userID)
    statement.setLong(3, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("Storage with ID = " + id + " not found")
    statement.close()
  }
  def storageGetUpdatedAt(id: Long): DateTime = {
    val resultset = storageGet("\"updated_at\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("Storage with ID = " + id + " not found")
    }
    val result = resultset.getTimestamp(1)
    resultset.getStatement.close()
    DT.toDateTime(result)
  }
  def storageSetUpdatedAt(id: Long, arg: DateTime, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default storage record with id 1 is read only")
    val sTable = addTablePrefix("STORAGE")
    val statement = connection.prepareStatement("UPDATE \"" + sTable + "\" "
                                                 +"SET \"updated_at\" = ?, \"last_user\" = ?, \"last_time\" = now, \"timestamp\" = now "
                                                 +"WHERE \"id\" = ?")
    statement.setTimestamp(1, DT.toTimestamp(arg))
    statement.setLong(2, userID)
    statement.setLong(3, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("Storage with ID = " + id + " not found")
    statement.close()
  }
  def storageGetFilesCounter(id: Long): Long = {
    val resultset = storageGet("\"files_counter\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("Storage with ID = " + id + " not found")
    }
    val result = resultset.getLong(1)
    resultset.getStatement.close()
    result
  }
  def storageSetFilesCounter(id: Long, arg: Long, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default storage record with id 1 is read only")
    val sTable = addTablePrefix("STORAGE")
    val statement = connection.prepareStatement("UPDATE \"" + sTable + "\" "
                                                 +"SET \"files_counter\" = ?, \"last_user\" = ?, \"last_time\" = now, \"timestamp\" = now "
                                                 +"WHERE \"id\" = ?")
    statement.setLong(1, arg)
    statement.setLong(2, userID)
    statement.setLong(3, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("Storage with ID = " + id + " not found")
    statement.close()
  }
  def storageGetFoldersCounter(id: Long): Long = {
    val resultset = storageGet("\"folders_counter\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("Storage with ID = " + id + " not found")
    }
    val result = resultset.getInt(1)
    resultset.getStatement.close()
    result
  }
  def storageSetFoldersCounter(id: Long, arg: Long, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default storage record with id 1 is read only")
    val sTable = addTablePrefix("STORAGE")
    val statement = connection.prepareStatement("UPDATE \"" + sTable + "\" "
                                                 +"SET \"folders_counter\" = ?, \"last_user\" = ?, \"last_time\" = now, \"timestamp\" = now "
                                                 +"WHERE \"id\" = ?")
    statement.setLong(1, arg)
    statement.setLong(2, userID)
    statement.setLong(3, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("Storage with ID = " + id + " not found")
    statement.close()
  }
  def storageGetTimestamp(id: Long): DateTime = {
    val resultset = storageGet("\"timestamp\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("Storage with ID = " + id + " not found")
    }
    val result = resultset.getTimestamp(1)
    resultset.getStatement.close()
    DT.toDateTime(result)
  }
  def storageSetTimestamp(id: Long, arg: DateTime, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default storage record with id 1 is read only")
    val sTable = addTablePrefix("STORAGE")
    val statement = connection.prepareStatement("UPDATE \"" + sTable + "\" "
                                                 +"SET \"timestamp\" = ?, \"last_user\" = ?, \"last_time\" = now "
                                                 +"WHERE \"id\" = ?")
    statement.setTimestamp(1, DT.toTimestamp(arg))
    statement.setLong(2, userID)
    statement.setLong(3, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("Storage with ID = " + id + " not found")
    statement.close()
  }
  def storageGet(sqlFields: String, sqlConstraint: String, args: Array[Any] = Array()): ResultSet = {
    val sTable = addTablePrefix("STORAGE")
    val statement = connection.prepareStatement("SELECT " + sqlFields + " FROM \"" + sTable + "\" " + sqlConstraint)
    addStatementParameters(statement, args)
    statement.executeQuery()
  }
  def storageUpdateAccess(id: Long, arg: Long): Unit = {
    val sTable = addTablePrefix("STORAGE")
    val statement = connection.prepareStatement("UPDATE \"" + sTable + "\" "
                                                 +"SET \"last_user\" = ?, \"last_time\" = now "
                                                 +"WHERE \"id\" = ?")
    statement.setLong(1, arg)
    statement.setLong(2, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("Storage with ID = " + id + " not found")
    statement.close()
  }
}
