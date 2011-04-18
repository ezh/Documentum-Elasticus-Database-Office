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

trait XFolderAPI extends XDatabase with XDBUtils {
  val sFolderTable = addTablePrefix("FOLDER")
  def folderExists(id: Long): Boolean = {
    val resultset = folderGet("1", "WHERE \"id\" = ?", Array(id))
    if (resultset.next()) {
      resultset.getStatement().close()
      return true
    } else {
      resultset.getStatement().close()
      return false
    }
  }
  def folderExists(storageID: Long, path: String): Boolean = {
    val resultset = folderGet("1", "WHERE \"storage_id\" = ? AND \"path\" = ?", Array(storageID, path))
    if (resultset.next()) {
      resultset.getStatement().close()
      return true
    } else {
      resultset.getStatement().close()
      return false
    }
  }
  def folderIDByPath(storageID: Long, path: String): Long = {
    val resultset = folderGet("\"id\"", "WHERE \"storage_id\" = ? AND \"path\" = ?", Array(storageID, path))
    if (!resultset.next()) {
      throw new RuntimeException("Folder with storage ID = '" + storageID + "' and PATH = '" + path + "' not found")
    }
    val result = resultset.getLong(1)
    resultset.getStatement.close()
    result
  }
  def folderCreate(serviceName: String, storageID: Long, folderPath: String, userID: Long): Long = {
    val statement = connection.prepareStatement("INSERT INTO \"" + sFolderTable + "\" (\"uuid\", \n"
                                                    +"\"storage_id\", \"path\", \"service\", \"owner_id\", \n"
                                                    +"\"group_id\", \"last_user\", \"last_time\", \n"
                                                    +"\"created_at\", \"updated_at\", \"name\", \n"
                                                    +"\"timestamp\") VALUES ("
                                                    +"?, ?, ?, ?, ?, ?, ?, now, now, now, ?, now)"
    )
    if (!folderPath.startsWith("/"))
      throw new RuntimeException("Invalid folder path. Please, provide path like /aaa/bbb/ccc")
    val name = folderPath.split("/").lastOption match {
      case None => if (folderPath.length == 1) "/" else throw new RuntimeException("Invalid folder path. Please, provide path like /aaa/bbb/ccc")
      case Some(a) => a
    }
    if (name.length == 0)
      throw new RuntimeException("Invalid folder path. Please, provide path like /aaa/bbb/ccc")
    val uuid = UUID.randomUUID()
    statement.setString(1, uuid.toString)
    statement.setLong(2, storageID)
    statement.setString(3, folderPath)
    statement.setString(4, serviceName)
    statement.setLong(5, userID)
    if (userGetGroup(userID) == 0) {
      statement.setNull(6, Types.BIGINT)
    } else {
      statement.setLong(6, userGetGroup(userID))
    }
    statement.setLong(7, userID)
    statement.setString(8, name)
    statement.executeUpdate()
    statement.close()
    val resultset = folderGet("\"id\"", "WHERE \"uuid\" = ?", Array(uuid.toString))
    if (!resultset.next()) {
      throw new RuntimeException("New folder with UUID = " + uuid.toString + " not found")
    }
    val result = resultset.getLong(1)
    resultset.getStatement.close()
    result
  }
  def folderGetUUID(id: Long): String = {
    val resultset = folderGet("\"uuid\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("Folder with ID = " + id + " not found")
    }
    val result = resultset.getString(1)
    resultset.getStatement.close()
    result
  }
  def folderSetUUID(id: Long, arg: String, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default folder record with id 1 is read only")
    val statement = connection.prepareStatement("UPDATE \"" + sFolderTable + "\" "
                                                 +"SET \"uuid\" = ?, \"last_user\" = ?, \"last_time\" = now, \"timestamp\" = now "
                                                 +"WHERE \"id\" = ?")
    statement.setString(1, arg)
    statement.setLong(2, userID)
    statement.setLong(3, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("Folder with ID = " + id + " not found")
    statement.close()
  }
  def folderGetStorage(id: Long): Long = {
    val resultset = folderGet("\"storage_id\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("Folder with ID = " + id + " not found")
    }
    val result = resultset.getLong(1)
    resultset.getStatement.close()
    result
  }
  def folderSetStorage(id: Long, arg: Long, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default folder record with id 1 is read only")
    val statement = connection.prepareStatement("UPDATE \"" + sFolderTable + "\" "
                                                 +"SET \"storage_id\" = ?, \"last_user\" = ?, \"last_time\" = now, \"timestamp\" = now "
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
      throw new RuntimeException("Folder with ID = " + id + " not found")
    statement.close()
  }
  def folderGetPath(id: Long): String = {
    val resultset = folderGet("\"path\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("Folder with ID = " + id + " not found")
    }
    val result = resultset.getString(1)
    resultset.getStatement.close()
    result
  }
  def folderSetPath(id: Long, arg: String, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default folder record with id 1 is read only")
    if (!arg.startsWith("/"))
      throw new RuntimeException("Invalid folder path. Please, provide path like /aaa/bbb/ccc")
    val name = arg.split("/").lastOption match {
      case None => throw new RuntimeException("Invalid folder path. Please, provide path like /aaa/bbb/ccc")
      case Some(a) => a
    }
    if (arg.length == 0)
      throw new RuntimeException("Invalid folder path. Please, provide path like /aaa/bbb/ccc")
    val statement = connection.prepareStatement("UPDATE \"" + sFolderTable + "\" "
                                                 +"SET \"path\" = ?, \"last_user\" = ?, \"last_time\" = now, \"timestamp\" = now "
                                                 +"WHERE \"id\" = ?")
    statement.setString(1, arg)
    statement.setLong(2, userID)
    statement.setLong(3, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("Folder with ID = " + id + " not found")
    statement.close()
    folderSetName(id, name, userID)
  }
  def folderGetService(id: Long): String = {
    val resultset = folderGet("\"service\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("Folder with ID = " + id + " not found")
    }
    val result = resultset.getString(1)
    resultset.getStatement.close()
    result
  }
  def folderSetService(id: Long, arg: String, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default folder record with id 1 is read only")
    val statement = connection.prepareStatement("UPDATE \"" + sFolderTable + "\" "
                                                 +"SET \"service\" = ?, \"last_user\" = ?, \"last_time\" = now, \"timestamp\" = now "
                                                 +"WHERE \"id\" = ?")
    statement.setString(1, arg)
    statement.setLong(2, userID)
    statement.setLong(3, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("Folder with ID = " + id + " not found")
    statement.close()
  }
  def folderGetParent(id: Long): Long = {
    val resultset = folderGet("\"parent_id\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("Folder with ID = " + id + " not found")
    }
    val result = resultset.getLong(1)
    resultset.getStatement.close()
    result
  }
  def folderSetParent(id: Long, arg: Long, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default folder record with id 1 is read only")
    if (id == arg)
      throw new RuntimeException("Error adding folder ID " + id + " to itself")
    val statement = connection.prepareStatement("UPDATE \"" + sFolderTable + "\" "
                                                 +"SET \"parent_id\" = ?, \"last_user\" = ?, \"last_time\" = now, \"timestamp\" = now "
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
      throw new RuntimeException("Folder with ID = " + id + " not found")
    statement.close()
  }
  def folderGetOwner(id: Long): Long = {
    val resultset = folderGet("\"owner_id\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("Folder with ID = " + id + " not found")
    }
    val result = resultset.getLong(1)
    resultset.getStatement.close()
    result
  }
  def folderSetOwner(id: Long, arg: Long, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default folder record with id 1 is read only")
    val statement = connection.prepareStatement("UPDATE \"" + sFolderTable + "\" "
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
      throw new RuntimeException("Folder with ID = " + id + " not found")
    statement.close()
  }
  def folderGetGroup(id: Long): Long = {
    val resultset = folderGet("\"group_id\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("Folder with ID = " + id + " not found")
    }
    val result = resultset.getLong(1)
    resultset.getStatement.close()
    result
  }
  def folderSetGroup(id: Long, arg: Long, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default folder record with id 1 is read only")
    val statement = connection.prepareStatement("UPDATE \"" + sFolderTable + "\" "
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
      throw new RuntimeException("Folder with ID = " + id + " not found")
    statement.close()
  }
  def folderGetLastUser(id: Long): Long = {
    val resultset = folderGet("\"last_user\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("Folder with ID = " + id + " not found")
    }
    val result = resultset.getLong(1)
    resultset.getStatement.close()
    result
  }
  def folderSetLastUser(id: Long, arg: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default folder record with id 1 is read only")
    val statement = connection.prepareStatement("UPDATE \"" + sFolderTable + "\" SET \"last_user\" = ?, \"timestamp\" = now WHERE \"id\" = ?")
    if (arg != 0) {
      statement.setLong(1, arg)
    } else {
      statement.setNull(1, Types.BIGINT)
    }
    statement.setLong(2, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("Folder with ID = " + id + " not found")
    statement.close()
  }
  def folderGetLastTime(id: Long): DateTime = {
    val resultset = folderGet("\"last_time\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("Folder with ID = " + id + " not found")
    }
    val result = resultset.getTimestamp(1)
    resultset.getStatement.close()
    DT.toDateTime(result)
  }
  def folderSetLastTime(id: Long, arg: DateTime): Unit = {
    if (id == 1)
      throw new RuntimeException("Default folder record with id 1 is read only")
    val statement = connection.prepareStatement("UPDATE \"" + sFolderTable + "\" SET \"last_time\" = ?, \"timestamp\" = now WHERE \"id\" = ?")
    statement.setTimestamp(1, DT.toTimestamp(arg))
    statement.setLong(2, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("Folder with ID = " + id + " not found")
    statement.close()
  }
  def folderGetName(id: Long): String = {
    val resultset = folderGet("\"name\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("Folder with ID = " + id + " not found")
    }
    val result = resultset.getString(1)
    resultset.getStatement.close()
    result
  }
  def folderSetName(id: Long, arg: String, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default folder record with id 1 is read only")
    if (arg.contains("/"))
      throw new RuntimeException("Invalid folder name. Please, provide name without '/' path delimiter")
    val statement = connection.prepareStatement("UPDATE \"" + sFolderTable + "\" "
                                                 +"SET \"name\" = ?, \"last_user\" = ?, \"last_time\" = now, \"timestamp\" = now "
                                                 +"WHERE \"id\" = ?")
    statement.setString(1, arg)
    statement.setLong(2, userID)
    statement.setLong(3, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("Folder with ID = " + id + " not found")
    statement.close()
  }
  def folderGetDescription(id: Long): String = {
    val resultset = folderGet("\"description\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("Folder with ID = " + id + " not found")
    }
    val result = resultset.getString(1)
    resultset.getStatement.close()
    result
  }
  def folderSetDescription(id: Long, arg: String, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default folder record with id 1 is read only")
    val statement = connection.prepareStatement("UPDATE \"" + sFolderTable + "\" "
                                                 +"SET \"description\" = ?, \"last_user\" = ?, \"last_time\" = now, \"timestamp\" = now "
                                                 +"WHERE \"id\" = ?")
    statement.setString(1, arg)
    statement.setLong(2, userID)
    statement.setLong(3, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("Folder with ID = " + id + " not found")
    statement.close()
  }
  def folderGetNote(id: Long): String = {
    val resultset = folderGet("\"note\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("Folder with ID = " + id + " not found")
    }
    val result = resultset.getString(1)
    resultset.getStatement.close()
    result
  }
  def folderSetNote(id: Long, arg: String, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default folder record with id 1 is read only")
    val statement = connection.prepareStatement("UPDATE \"" + sFolderTable + "\" "
                                                 +"SET \"note\" = ?, \"last_user\" = ?, \"last_time\" = now, \"timestamp\" = now "
                                                 +"WHERE \"id\" = ?")
    statement.setString(1, arg)
    statement.setLong(2, userID)
    statement.setLong(3, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("Folder with ID = " + id + " not found")
    statement.close()
  }
  def folderGetShared(id: Long): Boolean = {
    val resultset = folderGet("\"shared\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("Folder with ID = " + id + " not found")
    }
    val result = resultset.getBoolean(1)
    resultset.getStatement.close()
    result
  }
  def folderSetShared(id: Long, arg: Boolean, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default folder record with id 1 is read only")
    val statement = connection.prepareStatement("UPDATE \"" + sFolderTable + "\" "
                                                 +"SET \"shared\" = ?, \"last_user\" = ?, \"last_time\" = now, \"timestamp\" = now "
                                                 +"WHERE \"id\" = ?")
    statement.setBoolean(1, arg)
    statement.setLong(2, userID)
    statement.setLong(3, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("Folder with ID = " + id + " not found")
    statement.close()
  }
  def folderGetPrivate(id: Long): Boolean = {
    val resultset = folderGet("\"private\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("Folder with ID = " + id + " not found")
    }
    val result = resultset.getBoolean(1)
    resultset.getStatement.close()
    result
  }
  def folderSetPrivate(id: Long, arg: Boolean, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default folder record with id 1 is read only")
    val statement = connection.prepareStatement("UPDATE \"" + sFolderTable + "\" "
                                                 +"SET \"private\" = ?, \"last_user\" = ?, \"last_time\" = now, \"timestamp\" = now "
                                                 +"WHERE \"id\" = ?")
    statement.setBoolean(1, arg)
    statement.setLong(2, userID)
    statement.setLong(3, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("Folder with ID = " + id + " not found")
    statement.close()
  }
  def folderGetPermission(id: Long): Long = {
    val resultset = folderGet("\"permission\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("Folder with ID = " + id + " not found")
    }
    val result = resultset.getLong(1)
    resultset.getStatement.close()
    result
  }
  def folderSetPermission(id: Long, arg: Long, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default folder record with id 1 is read only")
    val statement = connection.prepareStatement("UPDATE \"" + sFolderTable + "\" "
                                                 +"SET \"permission\" = ?, \"last_user\" = ?, \"last_time\" = now, \"timestamp\" = now "
                                                 +"WHERE \"id\" = ?")
    statement.setLong(1, arg)
    statement.setLong(2, userID)
    statement.setLong(3, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("Folder with ID = " + id + " not found")
    statement.close()
  }
  def folderGetCreatedAt(id: Long): DateTime = {
    val resultset = folderGet("\"created_at\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("Folder with ID = " + id + " not found")
    }
    val result = resultset.getTimestamp(1)
    resultset.getStatement.close()
    DT.toDateTime(result)
  }
  def folderSetCreatedAt(id: Long, arg: DateTime, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default folder record with id 1 is read only")
    val statement = connection.prepareStatement("UPDATE \"" + sFolderTable + "\" "
                                                 +"SET \"created_at\" = ?, \"last_user\" = ?, \"last_time\" = now, \"timestamp\" = now "
                                                 +"WHERE \"id\" = ?")
    statement.setTimestamp(1, DT.toTimestamp(arg))
    statement.setLong(2, userID)
    statement.setLong(3, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("Folder with ID = " + id + " not found")
    statement.close()
  }
  def folderGetUpdatedAt(id: Long): DateTime = {
    val resultset = folderGet("\"updated_at\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("Folder with ID = " + id + " not found")
    }
    val result = resultset.getTimestamp(1)
    resultset.getStatement.close()
    DT.toDateTime(result)
  }
  def folderSetUpdatedAt(id: Long, arg: DateTime, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default folder record with id 1 is read only")
    val statement = connection.prepareStatement("UPDATE \"" + sFolderTable + "\" "
                                                 +"SET \"updated_at\" = ?, \"last_user\" = ?, \"last_time\" = now, \"timestamp\" = now "
                                                 +"WHERE \"id\" = ?")
    statement.setTimestamp(1, DT.toTimestamp(arg))
    statement.setLong(2, userID)
    statement.setLong(3, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("Folder with ID = " + id + " not found")
    statement.close()
  }
  def folderGetFilesCounter(id: Long): Long = {
    val resultset = folderGet("\"files_counter\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("Folder with ID = " + id + " not found")
    }
    val result = resultset.getLong(1)
    resultset.getStatement.close()
    result
  }
  def folderSetFilesCounter(id: Long, arg: Long, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default folder record with id 1 is read only")
    val statement = connection.prepareStatement("UPDATE \"" + sFolderTable + "\" "
                                                 +"SET \"files_counter\" = ?, \"last_user\" = ?, \"last_time\" = now, \"timestamp\" = now "
                                                 +"WHERE \"id\" = ?")
    statement.setLong(1, arg)
    statement.setLong(2, userID)
    statement.setLong(3, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("Folder with ID = " + id + " not found")
    statement.close()
  }
  def folderGetFoldersCounter(id: Long): Long = {
    val resultset = folderGet("\"folders_counter\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("Folder with ID = " + id + " not found")
    }
    val result = resultset.getLong(1)
    resultset.getStatement.close()
    result
  }
  def folderSetFoldersCounter(id: Long, arg: Long, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default folder record with id 1 is read only")
    val statement = connection.prepareStatement("UPDATE \"" + sFolderTable + "\" "
                                                 +"SET \"folders_counter\" = ?, \"last_user\" = ?, \"last_time\" = now, \"timestamp\" = now "
                                                 +"WHERE \"id\" = ?")
    statement.setLong(1, arg)
    statement.setLong(2, userID)
    statement.setLong(3, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("Folder with ID = " + id + " not found")
    statement.close()
  }
  def folderGetFilterMask(id: Long): String = {
    val resultset = folderGet("\"filter_mask\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("Folder with ID = " + id + " not found")
    }
    val result = resultset.getString(1)
    resultset.getStatement.close()
    result
  }
  def folderSetFilterMask(id: Long, arg: String, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default folder record with id 1 is read only")
    val statement = connection.prepareStatement("UPDATE \"" + sFolderTable + "\" "
                                                 +"SET \"filter_mask\" = ?, \"last_user\" = ?, \"last_time\" = now, \"timestamp\" = now "
                                                 +"WHERE \"id\" = ?")
    statement.setString(1, arg)
    statement.setLong(2, userID)
    statement.setLong(3, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("Folder with ID = " + id + " not found")
    statement.close()
  }
  def folderGetSortType(id: Long): Int = {
    val resultset = folderGet("\"sort_type\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("Folder with ID = " + id + " not found")
    }
    val result = resultset.getInt(1)
    resultset.getStatement.close()
    result
  }
  def folderSetSortType(id: Long, arg: Int, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default folder record with id 1 is read only")
    val statement = connection.prepareStatement("UPDATE \"" + sFolderTable + "\" "
                                                 +"SET \"sort_type\" = ?, \"last_user\" = ?, \"last_time\" = now, \"timestamp\" = now "
                                                 +"WHERE \"id\" = ?")
    statement.setInt(1, arg)
    statement.setLong(2, userID)
    statement.setLong(3, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("Folder with ID = " + id + " not found")
    statement.close()
  }
  def folderGetSortDirection(id: Long): Boolean = {
    val resultset = folderGet("\"sort_direction\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("Folder with ID = " + id + " not found")
    }
    val result = resultset.getBoolean(1)
    resultset.getStatement.close()
    result
  }
  def folderSetSortDirection(id: Long, arg: Boolean, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default folder record with id 1 is read only")
    val statement = connection.prepareStatement("UPDATE \"" + sFolderTable + "\" "
                                                 +"SET \"sort_direction\" = ?, \"last_user\" = ?, \"last_time\" = now, \"timestamp\" = now "
                                                 +"WHERE \"id\" = ?")
    statement.setBoolean(1, arg)
    statement.setLong(2, userID)
    statement.setLong(3, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("Folder with ID = " + id + " not found")
    statement.close()
  }
  def folderGetDisplayOrder(id: Long): Int = {
    val resultset = folderGet("\"display_order\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("Folder with ID = " + id + " not found")
    }
    val result = resultset.getInt(1)
    resultset.getStatement.close()
    result
  }
  def folderSetDisplayOrder(id: Long, arg: Int, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default folder record with id 1 is read only")
    val statement = connection.prepareStatement("UPDATE \"" + sFolderTable + "\" "
                                                 +"SET \"display_order\" = ?, \"last_user\" = ?, \"last_time\" = now, \"timestamp\" = now "
                                                 +"WHERE \"id\" = ?")
    statement.setInt(1, arg)
    statement.setLong(2, userID)
    statement.setLong(3, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("Folder with ID = " + id + " not found")
    statement.close()
  }
  def folderGetUndeletable(id: Long): Boolean = {
    val resultset = folderGet("\"undeletable\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("Folder with ID = " + id + " not found")
    }
    val result = resultset.getBoolean(1)
    resultset.getStatement.close()
    result
  }
  def folderSetUndeletable(id: Long, arg: Boolean, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default folder record with id 1 is read only")
    val statement = connection.prepareStatement("UPDATE \"" + sFolderTable + "\" "
                                                 +"SET \"undeletable\" = ?, \"last_user\" = ?, \"last_time\" = now, \"timestamp\" = now "
                                                 +"WHERE \"id\" = ?")
    statement.setBoolean(1, arg)
    statement.setLong(2, userID)
    statement.setLong(3, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("Folder with ID = " + id + " not found")
    statement.close()
  }
  def folderGetDeleted(id: Long): Boolean = {
    val resultset = folderGet("\"not_exists\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("Folder with ID = " + id + " not found")
    }
    val result = resultset.getBoolean(1)
    resultset.getStatement.close()
    result
  }
  def folderSetDeleted(id: Long, arg: Boolean, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default folder record with id 1 is read only")
    val statement = connection.prepareStatement("UPDATE \"" + sFolderTable + "\" "
                                                 +"SET \"not_exists\" = ?, \"last_user\" = ?, \"last_time\" = now, \"timestamp\" = now "
                                                 +"WHERE \"id\" = ?")
    statement.setBoolean(1, arg)
    statement.setLong(2, userID)
    statement.setLong(3, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("Folder with ID = " + id + " not found")
    statement.close()
  }
  def folderGetTimestamp(id: Long): DateTime = {
    val resultset = folderGet("\"timestamp\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("Folder with ID = " + id + " not found")
    }
    val result = resultset.getTimestamp(1)
    resultset.getStatement.close()
    DT.toDateTime(result)
  }
  def folderSetTimestamp(id: Long, arg: DateTime, userID: Long): Unit = {
    if (id == 1)
      throw new RuntimeException("Default folder record with id 1 is read only")
    val statement = connection.prepareStatement("UPDATE \"" + sFolderTable + "\" "
                                                 +"SET \"timestamp\" = ?, \"last_user\" = ?, \"last_time\" = now "
                                                 +"WHERE \"id\" = ?")
    statement.setTimestamp(1, DT.toTimestamp(arg))
    statement.setLong(2, userID)
    statement.setLong(3, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("Folder with ID = " + id + " not found")
    statement.close()
  }
  def folderGet(sqlFields: String, sqlConstraint: String, args: Array[Any] = Array()): ResultSet = {
    val statement = connection.prepareStatement("SELECT " + sqlFields + " FROM \"" + sFolderTable + "\" " + sqlConstraint)
    addStatementParameters(statement, args)
    statement.executeQuery()
  }
  def folderUpdateAccess(id: Long, arg: Long): Unit = {
    val statement = connection.prepareStatement("UPDATE \"" + sFolderTable + "\" "
                                                 +"SET \"last_user\" = ?, \"last_time\" = now "
                                                 +"WHERE \"id\" = ?")
    statement.setLong(1, arg)
    statement.setLong(2, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("Folder with ID = " + id + " not found")
    statement.close()
  }
  def folderGetFolders(id: Long): Array[Long] = {
    var result: Array[Long] = Array()
    val resultset = folderGet("\"id\"", "WHERE \"parent_id\" = ?", Array(id))
    while (resultset.next())
      result ++= Array(resultset.getLong(1))
    resultset.getStatement.close()
    result
  }
  def folderGetFiles(id: Long): Array[Long] = {
    var result: Array[Long] = Array()
    val resultset = fileGet("\"id\"", "WHERE \"folder_id\" = ?", Array(id))
    while (resultset.next())
      result ++= Array(resultset.getLong(1))
    resultset.getStatement.close()
    result
  }
}
