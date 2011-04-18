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
import org.digimead.documentumelasticus.helper.DT

trait XGroupAPI extends XDatabase
                  with XDBUtils {
  def groupExists(id: Long): Boolean = {
    val resultset = groupGet("1", "WHERE \"id\" = ?", Array(id))
    if (resultset.next()) {
      resultset.getStatement().close()
      return true
    } else {
      resultset.getStatement().close()
      return false
    }
  }
  def groupExists(name: String): Boolean = {
    val resultset = groupGet("1", "WHERE \"name\" = ?", Array(name))
    if (resultset.next()) {
      resultset.getStatement().close()
      return true
    } else {
      resultset.getStatement().close()
      return false
    }
  }
  def groupIDByName(name: String): Long = {
    val resultset = groupGet("\"id\"", "WHERE \"name\" = ?", Array(name))
    if (!resultset.next()) {
      throw new RuntimeException("Group with NAME = '" + name + "' not found")
    }
    val result = resultset.getLong(1)
    resultset.getStatement.close()
    result
  }
  def groupCreate(name: String): Long = {
    val sTable = addTablePrefix("GROUP")
    val statement = connection.prepareStatement("INSERT INTO \"" + sTable + "\" ("
                                                    +"\"auc\", \"name\", \"uuid\", \"timestamp\") VALUES ("
                                                    +"'localhost', ?, ?, now)"
    )
    val uuid = UUID.randomUUID()
    statement.setString(1, name)
    statement.setString(2, uuid.toString)
    statement.executeUpdate()
    statement.close()
    val resultset = groupGet("\"id\"", "WHERE \"uuid\" = ?", Array(uuid.toString))
    if (!resultset.next()) {
      throw new RuntimeException("New group with UUID = " + uuid.toString + " not found")
    }
    val result = resultset.getLong(1)
    resultset.getStatement.close()
    result
  }
  def groupGetUUID(id: Long): String = {
    val resultset = groupGet("\"uuid\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("Group with ID = " + id + " not found")
    }
    val result = resultset.getString(1)
    resultset.getStatement.close()
    result
  }
  def groupSetUUID(id: Long, arg: String): Unit = {
    if (id == 1)
      throw new RuntimeException("Default group record with id 1 is read only")
    val sTable = addTablePrefix("GROUP")
    val statement = connection.prepareStatement("UPDATE \"" + sTable + "\" SET \"uuid\" = ?, \"timestamp\" = now WHERE \"id\" = ?")
    statement.setString(1, arg)
    statement.setLong(2, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("Group with ID = " + id + " not found")
    statement.close()
  }
  def groupGetAUC(id: Long): String = {
    val resultset = groupGet("\"auc\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("Group with ID = " + id + " not found")
    }
    val result = resultset.getString(1)
    resultset.getStatement.close()
    result
  }
  def groupSetAUC(id: Long, arg: String): Unit = {
    if (id == 1)
      throw new RuntimeException("Default group record with id 1 is read only")
    val sTable = addTablePrefix("GROUP")
    val statement = connection.prepareStatement("UPDATE \"" + sTable + "\" SET \"auc\" = ?, \"timestamp\" = now WHERE \"id\" = ?")
    statement.setString(1, arg)
    statement.setLong(2, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("Group with ID = " + id + " not found")
    statement.close()
  }
  def groupGetParent(id: Long): Long = {
    val resultset = groupGet("\"parent_id\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("Group with ID = " + id + " not found")
    }
    val result = resultset.getLong(1)
    resultset.getStatement.close()
    result
  }
  def groupSetParent(id: Long, arg: Long): Unit = {
    if (id == arg)
      throw new RuntimeException("Error adding group ID " + id + " to itself")
    val sTable = addTablePrefix("GROUP")
    val statement = connection.prepareStatement("UPDATE \"" + sTable + "\" SET \"parent_id\" = ?, \"timestamp\" = now WHERE \"id\" = ?")
    if (arg != 0) {
      statement.setLong(1, arg)
    } else {
      statement.setNull(1, Types.BIGINT)
    }
    statement.setLong(2, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("Group with ID = " + id + " not found")
    statement.close()
  }
  def groupGetName(id: Long): String = {
    val resultset = groupGet("\"name\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("Group with ID = " + id + " not found")
    }
    val result = resultset.getString(1)
    resultset.getStatement.close()
    result
  }
  def groupSetName(id: Long, arg: String): Unit = {
    if (id == 1)
      throw new RuntimeException("Default group record with id 1 is read only")
    val sTable = addTablePrefix("GROUP")
    val statement = connection.prepareStatement("UPDATE \"" + sTable + "\" SET \"name\" = ?, \"timestamp\" = now WHERE \"id\" = ?")
    statement.setString(1, arg)
    statement.setLong(2, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("Group with ID = " + id + " not found")
    statement.close()
  }
  def groupGetDescription(id: Long): String = {
    val resultset = groupGet("\"description\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("Group with ID = " + id + " not found")
    }
    val result = resultset.getString(1)
    resultset.getStatement.close()
    result
  }
  def groupSetDescription(id: Long, arg: String): Unit = {
    if (id == 1)
      throw new RuntimeException("Default group record with id 1 is read only")
    val sTable = addTablePrefix("GROUP")
    val statement = connection.prepareStatement("UPDATE \"" + sTable + "\" SET \"description\" = ?, \"timestamp\" = now WHERE \"id\" = ?")
    statement.setString(1, arg)
    statement.setLong(2, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("Group with ID = " + id + " not found")
    statement.close()
  }
  def groupGetAvatar(id: Long): String = {
    val resultset = groupGet("\"avatar\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("Group with ID = " + id + " not found")
    }
    val result = resultset.getString(1)
    resultset.getStatement.close()
    result
  }
  def groupSetAvatar(id: Long, arg: String): Unit = {
    if (id == 1)
      throw new RuntimeException("Default group record with id 1 is read only")
    val sTable = addTablePrefix("GROUP")
    val statement = connection.prepareStatement("UPDATE \"" + sTable + "\" SET \"avatar\" = ?, \"timestamp\" = now WHERE \"id\" = ?")
    statement.setString(1, arg)
    statement.setLong(2, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("Group with ID = " + id + " not found")
    statement.close()
  }
  def groupGetGlobal(id: Long): Boolean = {
    val resultset = groupGet("\"global\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("Group with ID = " + id + " not found")
    }
    val result = resultset.getBoolean(1)
    resultset.getStatement.close()
    result
  }
  def groupSetGlobal(id: Long, arg: Boolean): Unit = {
    if (id == 1)
      throw new RuntimeException("Default group record with id 1 is read only")
    val sTable = addTablePrefix("GROUP")
    val statement = connection.prepareStatement("UPDATE \"" + sTable + "\" SET \"global\" = ?, \"timestamp\" = now WHERE \"id\" = ?")
    statement.setBoolean(1, arg)
    statement.setLong(2, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("Group with ID = " + id + " not found")
    statement.close()
  }
  def groupGetUsers(id: Long): Array[Long] = {
    var result: Array[Long] = Array()
    val sTable = addTablePrefix("USERGROUP")
    val statement = connection.prepareStatement("SELECT \"user_id\" FROM \"" + sTable + "\" WHERE \"group_id\" = ?")
    statement.setLong(1, id)
    val resultset = statement.executeQuery()
    while (resultset.next()) {
      result = result ++ Array(resultset.getLong(1))
    }
    resultset.close()
    statement.close()
    result
  }
  def groupHasUser(id: Long, arg: Long): Boolean = {
    val sTable = addTablePrefix("USERGROUP")
    val statement = connection.prepareStatement("SELECT 1 FROM \"" + sTable + "\" WHERE \"group_id\" = ? AND \"user_id\" = ?")
    statement.setLong(1, id)
    statement.setLong(2, arg)
    val resultset = statement.executeQuery()
    if (resultset.next()) {
      resultset.getStatement().close()
      return true
    } else {
      resultset.getStatement().close()
      return false
    }
  }
  def groupAddUser(id: Long, arg: Long): Unit = {
    val sTable = addTablePrefix("USERGROUP")
    val statement = connection.prepareStatement("INSERT INTO \"" + sTable + "\" VALUES (?, ?, NULL)")
    statement.setLong(1, arg)
    statement.setLong(2, id)
    val result = statement.executeUpdate()
    statement.close()
  }
  def groupDelUser(id: Long, arg: Long): Unit = {
    val sTable = addTablePrefix("USERGROUP")
    val statement = connection.prepareStatement("DELETE FROM \"" + sTable + "\" WHERE \"user_id\" = ? AND \"group_id\" = ?")
    statement.setLong(1, arg)
    statement.setLong(2, id)
    val result = statement.executeUpdate()
    statement.close()
    if (result == 0)
      throw new RuntimeException("Error deleting user ID " + arg + " from group ID " + id + "; relationship not found")
  }
  def groupClearUsers(id: Long): Unit = {
    val sTable = addTablePrefix("USERGROUP")
    val statement = connection.prepareStatement("DELETE FROM \"" + sTable + "\" WHERE \"group_id\" = ?")
    statement.setLong(1, id)
    statement.executeUpdate()
    statement.close()
  }
  def groupGetGroups(id: Long): Array[Long] = {
    var result: Array[Long] = Array()
    val sTable = addTablePrefix("GROUP")
    val statement = connection.prepareStatement("SELECT \"id\" FROM \"" + sTable + "\" WHERE \"parent_id\" = ?")
    statement.setLong(1, id)
    val resultset = statement.executeQuery()
    while (resultset.next()) {
      result = result ++ Array(resultset.getLong(1))
    }
    resultset.close()
    statement.close()
    result
  }
  def groupHasGroup(id: Long, arg: Long): Boolean = {
    val sTable = addTablePrefix("GROUP")
    val resultset = groupGet("1", "WHERE \"parent_id\" = ? AND \"id\" = ?", Array(id, arg))
    if (resultset.next()) {
      resultset.getStatement().close()
      return true
    } else {
      resultset.getStatement().close()
      return false
    }
  }
  def groupClearGroups(id: Long): Unit = {
    val sTable = addTablePrefix("GROUP")
    val statement = connection.prepareStatement("UPDATE \"" + sTable + "\" SET \"parent_id\" = NULL WHERE \"parent_id\" = ?")
    statement.setLong(1, id)
    val result = statement.executeUpdate()
    statement.close()
  }
  def groupGetTimestamp(id: Long): DateTime = {
    val resultset = groupGet("\"timestamp\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("Group with ID = " + id + " not found")
    }
    val result = resultset.getTimestamp(1)
    resultset.getStatement.close()
    DT.toDateTime(result)
  }
  def groupSetTimestamp(id: Long, arg: DateTime): Unit = {
    if (id == 1)
      throw new RuntimeException("Default group record with id 1 is read only")
    val sTable = addTablePrefix("GROUP")
    val statement = connection.prepareStatement("UPDATE \"" + sTable + "\" SET \"timestamp\" = ? WHERE \"id\" = ?")
    statement.setTimestamp(1, DT.toTimestamp(arg))
    statement.setLong(2, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("Group with ID = " + id + " not found")
    statement.close()
  }
  def groupGet(sqlFields: String, sqlConstraint: String, args: Array[Any] = Array()): ResultSet = {
    val sTable = addTablePrefix("GROUP")
    val statement = connection.prepareStatement("SELECT " + sqlFields + " FROM \"" + sTable + "\" " + sqlConstraint)
    addStatementParameters(statement, args)
    statement.executeQuery()
  }
}
