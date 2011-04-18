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
import org.digimead.documentumelasticus.storage.AvailableState

trait XFileAPI extends XDatabase with XDBUtils {
  val sFileTable = addTablePrefix("FILE")
  val sFileComponentTable = addTablePrefix("FILE_X_COMPONENT")
  def fileExists(id: Long): Boolean = {
    val resultset = fileGet("1", "WHERE \"id\" = ?", Array(id))
    if (resultset.next()) {
      resultset.getStatement().close()
      return true
    } else {
      resultset.getStatement().close()
      return false
    }
  }
  def fileExists(folderID: Long, name: String): Boolean = {
    val resultset = fileGet("1", "WHERE \"folder_id\" = ? AND \"name\" = ?", Array(folderID, name))
    if (resultset.next()) {
      resultset.getStatement().close()
      return true
    } else {
      resultset.getStatement().close()
      return false
    }
  }
  def fileIDByName(folderID: Long, name: String): Long = {
    val resultset = fileGet("\"id\"", "WHERE \"folder_id\" = ? AND \"name\" = ?", Array(folderID, name))
    if (!resultset.next()) {
      throw new RuntimeException("File with folder ID = '" + folderID + "' and NAME = '" + name + "' not found")
    }
    val result = resultset.getLong(1)
    resultset.getStatement.close()
    result
  }
  def fileCreate(serviceName: String, folderID: Long, fileName: String, userID: Long): Long = {
    val statement = connection.prepareStatement("INSERT INTO \"" + sFileTable + "\" (\"uuid\", \n"
                                                    +"\"folder_id\", \"name\", \"service\", \"owner_id\", \n"
                                                    +"\"group_id\", \"last_user\", \"last_time\", \n"
                                                    +"\"created_at\", \"updated_at\", \n"
                                                    +"\"timestamp\") VALUES ("
                                                    +"?, ?, ?, ?, ?, ?, ?, now, now, now, now)"
    )
    if (fileName.contains("/"))
      throw new RuntimeException("Invalid file name. Please, provide name without '/' path delimiter")
    val uuid = UUID.randomUUID()
    statement.setString(1, uuid.toString)
    statement.setLong(2, folderID)
    statement.setString(3, fileName)
    statement.setString(4, serviceName)
    statement.setLong(5, userID)
    if (userGetGroup(userID) == 0) {
      statement.setNull(6, Types.BIGINT)
    } else {
      statement.setLong(6, userGetGroup(userID))
    }
    statement.setLong(7, userID)
    statement.executeUpdate()
    statement.close()
    val resultset = fileGet("\"id\"", "WHERE \"uuid\" = ?", Array(uuid.toString))
    if (!resultset.next()) {
      throw new RuntimeException("New file with UUID = " + uuid.toString + " not found")
    }
    val result = resultset.getLong(1)
    resultset.getStatement.close()
    result
  }
  def fileGetUUID(id: Long): String = {
    val resultset = fileGet("\"uuid\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("File with ID = " + id + " not found")
    }
    val result = resultset.getString(1)
    resultset.getStatement.close()
    result
  }
  def fileSetUUID(id: Long, arg: String, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default file record with id 1 is read only")
    val statement = connection.prepareStatement("UPDATE \"" + sFileTable + "\" "
                                                 +"SET \"uuid\" = ?, \"last_user\" = ?, \"last_time\" = now, \"timestamp\" = now "
                                                 +"WHERE \"id\" = ?")
    statement.setString(1, arg)
    statement.setLong(2, userID)
    statement.setLong(3, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("File with ID = " + id + " not found")
    statement.close()
  }
  def fileGetFolder(id: Long): Long = {
    val resultset = fileGet("\"folder_id\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("File with ID = " + id + " not found")
    }
    val result = resultset.getLong(1)
    resultset.getStatement.close()
    result
  }
  def fileSetFolder(id: Long, arg: Long, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default file record with id 1 is read only")
    if (id == arg)
      throw new RuntimeException("Error adding file ID " + id + " to itself")
    val statement = connection.prepareStatement("UPDATE \"" + sFileTable + "\" "
                                                 +"SET \"folder_id\" = ?, \"last_user\" = ?, \"last_time\" = now, \"timestamp\" = now "
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
      throw new RuntimeException("File with ID = " + id + " not found")
    statement.close()
  }
  def fileGetName(id: Long): String = {
    val resultset = fileGet("\"name\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("File with ID = " + id + " not found")
    }
    val result = resultset.getString(1)
    resultset.getStatement.close()
    result
  }
  def fileSetName(id: Long, arg: String, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default file record with id 1 is read only")
    if (arg.contains("/"))
      throw new RuntimeException("Invalid file name. Please, provide name without '/' path delimiter")
    val statement = connection.prepareStatement("UPDATE \"" + sFileTable + "\" "
                                                 +"SET \"name\" = ?, \"last_user\" = ?, \"last_time\" = now, \"timestamp\" = now "
                                                 +"WHERE \"id\" = ?")
    statement.setString(1, arg)
    statement.setLong(2, userID)
    statement.setLong(3, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("File with ID = " + id + " not found")
    statement.close()
  }
  def fileGetService(id: Long): String = {
    val resultset = fileGet("\"service\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("File with ID = " + id + " not found")
    }
    val result = resultset.getString(1)
    resultset.getStatement.close()
    result
  }
  def fileSetService(id: Long, arg: String, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default file record with id 1 is read only")
    val statement = connection.prepareStatement("UPDATE \"" + sFileTable + "\" "
                                                 +"SET \"service\" = ?, \"last_user\" = ?, \"last_time\" = now, \"timestamp\" = now "
                                                 +"WHERE \"id\" = ?")
    statement.setString(1, arg)
    statement.setLong(2, userID)
    statement.setLong(3, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("File with ID = " + id + " not found")
    statement.close()
  }
  def fileGetOwner(id: Long): Long = {
    val resultset = fileGet("\"owner_id\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("File with ID = " + id + " not found")
    }
    val result = resultset.getLong(1)
    resultset.getStatement.close()
    result
  }
  def fileSetOwner(id: Long, arg: Long, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default file record with id 1 is read only")
    val statement = connection.prepareStatement("UPDATE \"" + sFileTable + "\" "
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
      throw new RuntimeException("File with ID = " + id + " not found")
    statement.close()
  }
  def fileGetGroup(id: Long): Long = {
    val resultset = fileGet("\"group_id\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("File with ID = " + id + " not found")
    }
    val result = resultset.getLong(1)
    resultset.getStatement.close()
    result
  }
  def fileSetGroup(id: Long, arg: Long, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default file record with id 1 is read only")
    val statement = connection.prepareStatement("UPDATE \"" + sFileTable + "\" "
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
      throw new RuntimeException("File with ID = " + id + " not found")
    statement.close()
  }
  def fileGetLastUser(id: Long): Long = {
    val resultset = fileGet("\"last_user\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("File with ID = " + id + " not found")
    }
    val result = resultset.getLong(1)
    resultset.getStatement.close()
    result
  }
  def fileSetLastUser(id: Long, arg: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default file record with id 1 is read only")
    val statement = connection.prepareStatement("UPDATE \"" + sFileTable + "\" SET \"last_user\" = ?, \"timestamp\" = now WHERE \"id\" = ?")
    if (arg != 0) {
      statement.setLong(1, arg)
    } else {
      statement.setNull(1, Types.BIGINT)
    }
    statement.setLong(2, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("File with ID = " + id + " not found")
    statement.close()
  }
  def fileGetLastTime(id: Long): DateTime = {
    val resultset = fileGet("\"last_time\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("File with ID = " + id + " not found")
    }
    val result = resultset.getTimestamp(1)
    resultset.getStatement.close()
    DT.toDateTime(result)
  }
  def fileSetLastTime(id: Long, arg: DateTime): Unit = {
    if (id == 1)
      throw new RuntimeException("Default file record with id 1 is read only")
    val statement = connection.prepareStatement("UPDATE \"" + sFileTable + "\" SET \"last_time\" = ?, \"timestamp\" = now WHERE \"id\" = ?")
    statement.setTimestamp(1, DT.toTimestamp(arg))
    statement.setLong(2, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("File with ID = " + id + " not found")
    statement.close()
  }
  def fileGetDescription(id: Long): String = {
    val resultset = fileGet("\"description\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("File with ID = " + id + " not found")
    }
    val result = resultset.getString(1)
    resultset.getStatement.close()
    result
  }
  def fileSetDescription(id: Long, arg: String, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default file record with id 1 is read only")
    val statement = connection.prepareStatement("UPDATE \"" + sFileTable + "\" "
                                                 +"SET \"description\" = ?, \"last_user\" = ?, \"last_time\" = now, \"timestamp\" = now "
                                                 +"WHERE \"id\" = ?")
    statement.setString(1, arg)
    statement.setLong(2, userID)
    statement.setLong(3, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("File with ID = " + id + " not found")
    statement.close()
  }
  def fileGetNote(id: Long): String = {
    val resultset = fileGet("\"note\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("File with ID = " + id + " not found")
    }
    val result = resultset.getString(1)
    resultset.getStatement.close()
    result
  }
  def fileSetNote(id: Long, arg: String, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default file record with id 1 is read only")
    val statement = connection.prepareStatement("UPDATE \"" + sFileTable + "\" "
                                                 +"SET \"note\" = ?, \"last_user\" = ?, \"last_time\" = now, \"timestamp\" = now "
                                                 +"WHERE \"id\" = ?")
    statement.setString(1, arg)
    statement.setLong(2, userID)
    statement.setLong(3, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("File with ID = " + id + " not found")
    statement.close()
  }
  def fileGetPermission(id: Long): Long = {
    val resultset = fileGet("\"permission\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("File with ID = " + id + " not found")
    }
    val result = resultset.getLong(1)
    resultset.getStatement.close()
    result
  }
  def fileSetPermission(id: Long, arg: Long, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default file record with id 1 is read only")
    val statement = connection.prepareStatement("UPDATE \"" + sFileTable + "\" "
                                                 +"SET \"permission\" = ?, \"last_user\" = ?, \"last_time\" = now, \"timestamp\" = now "
                                                 +"WHERE \"id\" = ?")
    statement.setLong(1, arg)
    statement.setLong(2, userID)
    statement.setLong(3, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("File with ID = " + id + " not found")
    statement.close()
  }
  def fileGetCreatedAt(id: Long): DateTime = {
    val resultset = fileGet("\"created_at\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("File with ID = " + id + " not found")
    }
    val result = resultset.getTimestamp(1)
    resultset.getStatement.close()
    DT.toDateTime(result)
  }
  def fileSetCreatedAt(id: Long, arg: DateTime, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default file record with id 1 is read only")
    val statement = connection.prepareStatement("UPDATE \"" + sFileTable + "\" "
                                                 +"SET \"created_at\" = ?, \"last_user\" = ?, \"last_time\" = now, \"timestamp\" = now "
                                                 +"WHERE \"id\" = ?")
    statement.setTimestamp(1, DT.toTimestamp(arg))
    statement.setLong(2, userID)
    statement.setLong(3, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("File with ID = " + id + " not found")
    statement.close()
  }
  def fileGetUpdatedAt(id: Long): DateTime = {
    val resultset = fileGet("\"updated_at\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("File with ID = " + id + " not found")
    }
    val result = resultset.getTimestamp(1)
    resultset.getStatement.close()
    DT.toDateTime(result)
  }
  def fileSetUpdatedAt(id: Long, arg: DateTime, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default file record with id 1 is read only")
    val statement = connection.prepareStatement("UPDATE \"" + sFileTable + "\" "
                                                 +"SET \"updated_at\" = ?, \"last_user\" = ?, \"last_time\" = now, \"timestamp\" = now "
                                                 +"WHERE \"id\" = ?")
    statement.setTimestamp(1, DT.toTimestamp(arg))
    statement.setLong(2, userID)
    statement.setLong(3, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("File with ID = " + id + " not found")
    statement.close()
  }
  def fileGetSize(id: Long): Long = {
    val resultset = fileGet("\"size\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("File with ID = " + id + " not found")
    }
    val result = resultset.getLong(1)
    resultset.getStatement.close()
    result
  }
  def fileSetSize(id: Long, arg: Long, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default file record with id 1 is read only")
    val statement = connection.prepareStatement("UPDATE \"" + sFileTable + "\" "
                                                 +"SET \"size\" = ?, \"last_user\" = ?, \"last_time\" = now, \"timestamp\" = now "
                                                 +"WHERE \"id\" = ?")
    statement.setLong(1, arg)
    statement.setLong(2, userID)
    statement.setLong(3, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("File with ID = " + id + " not found")
    statement.close()
  }
  def fileGetMimeType(id: Long): String = {
    val resultset = fileGet("\"mimetype\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("File with ID = " + id + " not found")
    }
    val result = resultset.getString(1)
    resultset.getStatement.close()
    result
  }
  def fileSetMimeType(id: Long, arg: String, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default file record with id 1 is read only")
    val statement = connection.prepareStatement("UPDATE \"" + sFileTable + "\" "
                                                 +"SET \"mimetype\" = ?, \"last_user\" = ?, \"last_time\" = now, \"timestamp\" = now "
                                                 +"WHERE \"id\" = ?")
    statement.setString(1, arg)
    statement.setLong(2, userID)
    statement.setLong(3, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("File with ID = " + id + " not found")
    statement.close()
  }
  def fileGetHash(id: Long): String = {
    val resultset = fileGet("\"hash\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("File with ID = " + id + " not found")
    }
    val result = resultset.getString(1)
    resultset.getStatement.close()
    result
  }
  def fileSetHash(id: Long, arg: String, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default file record with id 1 is read only")
    val statement = connection.prepareStatement("UPDATE \"" + sFileTable + "\" "
                                                 +"SET \"hash\" = ?, \"last_user\" = ?, \"last_time\" = now, \"timestamp\" = now "
                                                 +"WHERE \"id\" = ?")
    statement.setString(1, arg)
    statement.setLong(2, userID)
    statement.setLong(3, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("File with ID = " + id + " not found")
    statement.close()
  }
  def fileGetVersion(id: Long): Long = {
    val resultset = fileGet("\"version\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("File with ID = " + id + " not found")
    }
    val result = resultset.getLong(1)
    resultset.getStatement.close()
    result
  }
  def fileSetVersion(id: Long, arg: Long, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default file record with id 1 is read only")
    val statement = connection.prepareStatement("UPDATE \"" + sFileTable + "\" "
                                                 +"SET \"version\" = ?, \"last_user\" = ?, \"last_time\" = now, \"timestamp\" = now "
                                                 +"WHERE \"id\" = ?")
    statement.setLong(1, arg)
    statement.setLong(2, userID)
    statement.setLong(3, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("File with ID = " + id + " not found")
    statement.close()
  }
  def fileGetReadCounter(id: Long): Long = {
    val resultset = fileGet("\"readcounter\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("File with ID = " + id + " not found")
    }
    val result = resultset.getLong(1)
    resultset.getStatement.close()
    result
  }
  def fileSetReadCounter(id: Long, arg: Long, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default file record with id 1 is read only")
    val statement = connection.prepareStatement("UPDATE \"" + sFileTable + "\" "
                                                 +"SET \"readcounter\" = ?, \"last_user\" = ?, \"last_time\" = now, \"timestamp\" = now "
                                                 +"WHERE \"id\" = ?")
    statement.setLong(1, arg)
    statement.setLong(2, userID)
    statement.setLong(3, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("File with ID = " + id + " not found")
    statement.close()
  }
  def fileGetWriteCounter(id: Long): Long = {
    val resultset = fileGet("\"writecounter\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("File with ID = " + id + " not found")
    }
    val result = resultset.getLong(1)
    resultset.getStatement.close()
    result
  }
  def fileSetWriteCounter(id: Long, arg: Long, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default file record with id 1 is read only")
    val statement = connection.prepareStatement("UPDATE \"" + sFileTable + "\" "
                                                 +"SET \"writecounter\" = ?, \"last_user\" = ?, \"last_time\" = now, \"timestamp\" = now "
                                                 +"WHERE \"id\" = ?")
    statement.setLong(1, arg)
    statement.setLong(2, userID)
    statement.setLong(3, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("File with ID = " + id + " not found")
    statement.close()
  }
  def fileGetDisplayOrder(id: Long): Int = {
    val resultset = fileGet("\"display_order\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("File with ID = " + id + " not found")
    }
    val result = resultset.getInt(1)
    resultset.getStatement.close()
    result
  }
  def fileSetDisplayOrder(id: Long, arg: Int, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default file record with id 1 is read only")
    val statement = connection.prepareStatement("UPDATE \"" + sFileTable + "\" "
                                                 +"SET \"display_order\" = ?, \"last_user\" = ?, \"last_time\" = now, \"timestamp\" = now "
                                                 +"WHERE \"id\" = ?")
    statement.setInt(1, arg)
    statement.setLong(2, userID)
    statement.setLong(3, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("File with ID = " + id + " not found")
    statement.close()
  }
  def fileGetUndeletable(id: Long): Boolean = {
    val resultset = fileGet("\"undeletable\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("File with ID = " + id + " not found")
    }
    val result = resultset.getBoolean(1)
    resultset.getStatement.close()
    result
  }
  def fileSetUndeletable(id: Long, arg: Boolean, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default file record with id 1 is read only")
    val statement = connection.prepareStatement("UPDATE \"" + sFileTable + "\" "
                                                 +"SET \"undeletable\" = ?, \"last_user\" = ?, \"last_time\" = now, \"timestamp\" = now "
                                                 +"WHERE \"id\" = ?")
    statement.setBoolean(1, arg)
    statement.setLong(2, userID)
    statement.setLong(3, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("File with ID = " + id + " not found")
    statement.close()
  }
  def fileGetDeleted(id: Long): Boolean = {
    val resultset = fileGet("\"not_exists\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("File with ID = " + id + " not found")
    }
    val result = resultset.getBoolean(1)
    resultset.getStatement.close()
    result
  }
  def fileSetDeleted(id: Long, arg: Boolean, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default file record with id 1 is read only")
    val statement = connection.prepareStatement("UPDATE \"" + sFileTable + "\" "
                                                 +"SET \"not_exists\" = ?, \"last_user\" = ?, \"last_time\" = now, \"timestamp\" = now "
                                                 +"WHERE \"id\" = ?")
    statement.setBoolean(1, arg)
    statement.setLong(2, userID)
    statement.setLong(3, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("File with ID = " + id + " not found")
    statement.close()
  }
  def fileGetAvailable(id: Long, componentName: String): AvailableState = {
    val statement = connection.prepareStatement("SELECT \"available\" FROM \"" + sFileComponentTable + "\" WHERE \"file_id\" = ? AND \"component\" = ?")
    addStatementParameters(statement, Array(id, componentName))
    val resultset = statement.executeQuery()
    var result: AvailableState = AvailableState.NOT_AVAILABLE
    if (resultset.next()) {
      if (resultset.getBoolean(1))
        result = AvailableState.AVAILABLE
      else
        result = AvailableState.BLOCKED
    }
    resultset.close()
    statement.close()
    result
  }
  def fileSetAvailable(id: Long, componentName: String, state: AvailableState, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default file record with id 1 is read only")
    val result = if (state == AvailableState.NOT_AVAILABLE) {
      val statement = connection.prepareStatement("DELETE FROM \"" + sFileComponentTable + "\" "
                                                   +"WHERE \"file_id\" = ? AND \"component\" = ?")
      statement.setLong(1, id)
      statement.setString(2, componentName)
      statement.executeUpdate()
      statement.close()
    } else {
      val available = if (state == AvailableState.AVAILABLE) true else false
      // insert or update from standard SQL2008
      val statement = connection.prepareStatement("MERGE INTO \"" + sFileComponentTable + "\" USING (VALUES(CAST(? AS BIGINT), CAST(? AS VARCHAR_IGNORECASE(1024)), CAST(? AS BOOLEAN))) AS vals (nid, ncomp, nav) "
                                                   +"ON \"file_id\" = nid AND \"component\" = ncomp "
                                                   +"WHEN MATCHED THEN UPDATE SET \"available\" = nav "
                                                   +"WHEN NOT MATCHED THEN INSERT VALUES(nid, ncomp, nav)")
      addStatementParameters(statement, Array(id, componentName, available))
      statement.executeUpdate()
      statement.close()
    }
  }
  def fileGetTimestamp(id: Long): DateTime = {
    val resultset = fileGet("\"timestamp\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("File with ID = " + id + " not found")
    }
    val result = resultset.getTimestamp(1)
    resultset.getStatement.close()
    DT.toDateTime(result)
  }
  def fileSetTimestamp(id: Long, arg: DateTime, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default file record with id 1 is read only")
    val statement = connection.prepareStatement("UPDATE \"" + sFileTable + "\" "
                                                 +"SET \"timestamp\" = ?, \"last_user\" = ?, \"last_time\" = now "
                                                 +"WHERE \"id\" = ?")
    statement.setTimestamp(1, DT.toTimestamp(arg))
    statement.setLong(2, userID)
    statement.setLong(3, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("File with ID = " + id + " not found")
    statement.close()
  }
  def fileGet(sqlFields: String, sqlConstraint: String, args: Array[Any] = Array()): ResultSet = {
    val statement = connection.prepareStatement("SELECT " + sqlFields + " FROM \"" + sFileTable + "\" " + sqlConstraint)
    addStatementParameters(statement, args)
    statement.executeQuery()
  }
  def fileUpdateAccess(id: Long, arg: Long): Unit = {
    val statement = connection.prepareStatement("UPDATE \"" + sFileTable + "\" "
                                                 +"SET \"last_user\" = ?, \"last_time\" = now "
                                                 +"WHERE \"id\" = ?")
    statement.setLong(1, arg)
    statement.setLong(2, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("File with ID = " + id + " not found")
    statement.close()
  }
}
