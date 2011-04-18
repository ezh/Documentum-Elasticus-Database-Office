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
import org.digimead.documentumelasticus.helper._
import java.sql.ResultSet
import java.sql.Types
import java.util.UUID

trait XUserAPI extends XDatabase
                  with XDBUtils {
  def userExists(id: Long): Boolean = {
    val resultset = userGet("1", "WHERE \"id\" = ?", Array(id))
    if (resultset.next()) {
      resultset.getStatement().close()
      return true
    } else {
      resultset.getStatement().close()
      return false
    }
  }
  def userExists(login: String): Boolean = {
    val resultset = userGet("1", "WHERE \"login\" = ?", Array(login))
    if (resultset.next()) {
      resultset.getStatement().close()
      return true
    } else {
      resultset.getStatement().close()
      return false
    }
  }
  def userIDByLogin(login: String): Long = {
    val resultset = userGet("\"id\"", "WHERE \"login\" = ?", Array(login))
    if (!resultset.next()) {
      throw new RuntimeException("User with LOGIN = '" + login + "' not found")
    }
    val result = resultset.getLong(1)
    resultset.getStatement.close()
    result
  }
  def userCreate(login: String): Long = {
    val sTable = addTablePrefix("USER")
    val statement = connection.prepareStatement("INSERT INTO \"" + sTable + "\" ("
                                                    +"\"login\", \"uuid\", \"timestamp\") VALUES ("
                                                    +"?, ?, now)"
    )
    val uuid = UUID.randomUUID()
    statement.setString(1, login)
    statement.setString(2, uuid.toString)
    statement.executeUpdate()
    statement.close()
    val resultset = userGet("\"id\"", "WHERE \"uuid\" = ?", Array(uuid.toString))
    if (!resultset.next()) {
      throw new RuntimeException("New user with UUID = " + uuid.toString + " not found")
    }
    val result = resultset.getLong(1)
    resultset.getStatement.close()
    result
  }
  def userGetUUID(id: Long): String = {
    val resultset = userGet("\"uuid\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("User with ID = " + id + " not found")
    }
    val result = resultset.getString(1)
    resultset.getStatement.close()
    result
  }
  def userSetUUID(id: Long, arg: String): Unit = {
    if (id == 1)
      throw new RuntimeException("anonymous user record with id 1 is read only")
    val sTable = addTablePrefix("USER")
    val statement = connection.prepareStatement("UPDATE \"" + sTable + "\" SET \"uuid\" = ?, \"timestamp\" = now WHERE \"id\" = ?")
    statement.setString(1, arg)
    statement.setLong(2, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("User with ID = " + id + " not found")
    statement.close()
  }
  def userGetLogin(id: Long): String = {
    val resultset = userGet("\"login\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("User with ID = " + id + " not found")
    }
    val result = resultset.getString(1)
    resultset.getStatement.close()
    result
  }
  def userSetLogin(id: Long, arg: String): Unit = {
    if (id == 1)
      throw new RuntimeException("anonymous user record with id 1 is read only")
    val sTable = addTablePrefix("USER")
    val statement = connection.prepareStatement("UPDATE \"" + sTable + "\" SET \"login\" = ?, \"timestamp\" = now WHERE \"id\" = ?")
    statement.setString(1, arg)
    statement.setLong(2, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("User with ID = " + id + " not found")
    statement.close()
  }
  def userGetGroup(id: Long): Long = {
    val resultset = userGet("\"group_id\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("User with ID = " + id + " not found")
    }
    val result = resultset.getLong(1)
    resultset.getStatement.close()
    result
  }
  def userSetGroup(id: Long, arg: Long): Unit = {
    val sTable = addTablePrefix("USER")
    val statement = connection.prepareStatement("UPDATE \"" + sTable + "\" SET \"group_id\" = ?, \"timestamp\" = now WHERE \"id\" = ?")
    if (arg != 0) {
      statement.setLong(1, arg)
    } else {
      statement.setNull(1, Types.BIGINT)
    }
    statement.setLong(2, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("User with ID = " + id + " not found")
    statement.close()
  }
  def userGetOrganization(id: Long): String = {
    val resultset = userGet("\"o\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("User with ID = " + id + " not found")
    }
    val result = resultset.getString(1)
    resultset.getStatement.close()
    result
  }
  def userSetOrganization(id: Long, arg: String): Unit = {
    if (id == 1)
      throw new RuntimeException("anonymous user record with id 1 is read only")
    val sTable = addTablePrefix("USER")
    val statement = connection.prepareStatement("UPDATE \"" + sTable + "\" SET \"o\" = ?, \"timestamp\" = now WHERE \"id\" = ?")
    statement.setString(1, arg)
    statement.setLong(2, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("User with ID = " + id + " not found")
    statement.close()
  }
  def userGetGivenname(id: Long): String = {
    val resultset = userGet("\"givenname\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("User with ID = " + id + " not found")
    }
    val result = resultset.getString(1)
    resultset.getStatement.close()
    result
  }
  def userSetGivenname(id: Long, arg: String): Unit = {
    if (id == 1)
      throw new RuntimeException("anonymous user record with id 1 is read only")
    val sTable = addTablePrefix("USER")
    val statement = connection.prepareStatement("UPDATE \"" + sTable + "\" SET \"givenname\" = ?, \"timestamp\" = now WHERE \"id\" = ?")
    statement.setString(1, arg)
    statement.setLong(2, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("User with ID = " + id + " not found")
    statement.close()
}
  def userGetFathersname(id: Long): String = {
    val resultset = userGet("\"fathersname\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("User with ID = " + id + " not found")
    }
    val result = resultset.getString(1)
    resultset.getStatement.close()
    result
  }
  def userSetFathersname(id: Long, arg: String): Unit = {
    if (id == 1)
      throw new RuntimeException("anonymous user record with id 1 is read only")
    val sTable = addTablePrefix("USER")
    val statement = connection.prepareStatement("UPDATE \"" + sTable + "\" SET \"fathersname\" = ?, \"timestamp\" = now WHERE \"id\" = ?")
    statement.setString(1, arg)
    statement.setLong(2, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("User with ID = " + id + " not found")
    statement.close()
  }
  def userGetSurname(id: Long): String = {
    val resultset = userGet("\"sn\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("User with ID = " + id + " not found")
    }
    val result = resultset.getString(1)
    resultset.getStatement.close()
    result
  }
  def userSetSurname(id: Long, arg: String): Unit = {
    if (id == 1)
      throw new RuntimeException("anonymous user record with id 1 is read only")
    val sTable = addTablePrefix("USER")
    val statement = connection.prepareStatement("UPDATE \"" + sTable + "\" SET \"sn\" = ?, \"timestamp\" = now WHERE \"id\" = ?")
    statement.setString(1, arg)
    statement.setLong(2, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("User with ID = " + id + " not found")
    statement.close()
  }
  def userGetInitials(id: Long): String = {
    val resultset = userGet("\"initials\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("User with ID = " + id + " not found")
    }
    val result = resultset.getString(1)
    resultset.getStatement.close()
    result
  }
  def userSetInitials(id: Long, arg: String): Unit = {
    if (id == 1)
      throw new RuntimeException("anonymous user record with id 1 is read only")
    val sTable = addTablePrefix("USER")
    val statement = connection.prepareStatement("UPDATE \"" + sTable + "\" SET \"initials\" = ?, \"timestamp\" = now WHERE \"id\" = ?")
    statement.setString(1, arg)
    statement.setLong(2, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("User with ID = " + id + " not found")
    statement.close()
  }
  def userGetApartment(id: Long): String = {
    val resultset = userGet("\"apartment\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("User with ID = " + id + " not found")
    }
    val result = resultset.getString(1)
    resultset.getStatement.close()
    result
  }
  def userSetApartment(id: Long, arg: String): Unit = {
    if (id == 1)
      throw new RuntimeException("anonymous user record with id 1 is read only")
    val sTable = addTablePrefix("USER")
    val statement = connection.prepareStatement("UPDATE \"" + sTable + "\" SET \"apartment\" = ?, \"timestamp\" = now WHERE \"id\" = ?")
    statement.setString(1, arg)
    statement.setLong(2, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("User with ID = " + id + " not found")
    statement.close()
  }
  def userGetStreet(id: Long): String = {
    val resultset = userGet("\"street\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("User with ID = " + id + " not found")
    }
    val result = resultset.getString(1)
    resultset.getStatement.close()
    result
  }
  def userSetStreet(id: Long, arg: String): Unit = {
    if (id == 1)
      throw new RuntimeException("anonymous user record with id 1 is read only")
    val sTable = addTablePrefix("USER")
    val statement = connection.prepareStatement("UPDATE \"" + sTable + "\" SET \"street\" = ?, \"timestamp\" = now WHERE \"id\" = ?")
    statement.setString(1, arg)
    statement.setLong(2, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("User with ID = " + id + " not found")
    statement.close()
  }
  def userGetCity(id: Long): String = {
    val resultset = userGet("\"l\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("User with ID = " + id + " not found")
    }
    val result = resultset.getString(1)
    resultset.getStatement.close()
    result
  }
  def userSetCity(id: Long, arg: String): Unit = {
    if (id == 1)
      throw new RuntimeException("anonymous user record with id 1 is read only")
    val sTable = addTablePrefix("USER")
    val statement = connection.prepareStatement("UPDATE \"" + sTable + "\" SET \"l\" = ?, \"timestamp\" = now WHERE \"id\" = ?")
    statement.setString(1, arg)
    statement.setLong(2, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("User with ID = " + id + " not found")
    statement.close()
  }
  def userGetState(id: Long): String = {
    val resultset = userGet("\"st\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("User with ID = " + id + " not found")
    }
    val result = resultset.getString(1)
    resultset.getStatement.close()
    result
  }
  def userSetState(id: Long, arg: String): Unit = {
    if (id == 1)
      throw new RuntimeException("anonymous user record with id 1 is read only")
    val sTable = addTablePrefix("USER")
    val statement = connection.prepareStatement("UPDATE \"" + sTable + "\" SET \"st\" = ?, \"timestamp\" = now WHERE \"id\" = ?")
    statement.setString(1, arg)
    statement.setLong(2, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("User with ID = " + id + " not found")
    statement.close()
  }
  def userGetPostalCode(id: Long): String = {
    val resultset = userGet("\"postalcode\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("User with ID = " + id + " not found")
    }
    val result = resultset.getString(1)
    resultset.getStatement.close()
    result
  }
  def userSetPostalCode(id: Long, arg: String): Unit = {
    if (id == 1)
      throw new RuntimeException("anonymous user record with id 1 is read only")
    val sTable = addTablePrefix("USER")
    val statement = connection.prepareStatement("UPDATE \"" + sTable + "\" SET \"postalcode\" = ?, \"timestamp\" = now WHERE \"id\" = ?")
    statement.setString(1, arg)
    statement.setLong(2, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("User with ID = " + id + " not found")
    statement.close()
  }
  def userGetCoutry(id: Long): String = {
    val resultset = userGet("\"c\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("User with ID = " + id + " not found")
    }
    val result = resultset.getString(1)
    resultset.getStatement.close()
    result
  }
  def userSetCoutry(id: Long, arg: String): Unit = {
    if (id == 1)
      throw new RuntimeException("anonymous user record with id 1 is read only")
    val sTable = addTablePrefix("USER")
    val statement = connection.prepareStatement("UPDATE \"" + sTable + "\" SET \"c\" = ?, \"timestamp\" = now WHERE \"id\" = ?")
    statement.setString(1, arg)
    statement.setLong(2, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("User with ID = " + id + " not found")
    statement.close()
  }
  def userGetTitle(id: Long): String = {
    val resultset = userGet("\"title\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("User with ID = " + id + " not found")
    }
    val result = resultset.getString(1)
    resultset.getStatement.close()
    result
  }
  def userSetTitle(id: Long, arg: String): Unit = {
    if (id == 1)
      throw new RuntimeException("anonymous user record with id 1 is read only")
    val sTable = addTablePrefix("USER")
    val statement = connection.prepareStatement("UPDATE \"" + sTable + "\" SET \"title\" = ?, \"timestamp\" = now WHERE \"id\" = ?")
    statement.setString(1, arg)
    statement.setLong(2, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("User with ID = " + id + " not found")
    statement.close()
  }
  def userGetPosition(id: Long): String = {
    val resultset = userGet("\"position\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("User with ID = " + id + " not found")
    }
    val result = resultset.getString(1)
    resultset.getStatement.close()
    result
  }
  def userSetPosition(id: Long, arg: String): Unit = {
    if (id == 1)
      throw new RuntimeException("anonymous user record with id 1 is read only")
    val sTable = addTablePrefix("USER")
    val statement = connection.prepareStatement("UPDATE \"" + sTable + "\" SET \"position\" = ?, \"timestamp\" = now WHERE \"id\" = ?")
    statement.setString(1, arg)
    statement.setLong(2, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("User with ID = " + id + " not found")
    statement.close()
  }
  def userGetHomePhone(id: Long): String = {
    val resultset = userGet("\"homephone\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("User with ID = " + id + " not found")
    }
    val result = resultset.getString(1)
    resultset.getStatement.close()
    result
  }
  def userSetHomePhone(id: Long, arg: String): Unit = {
    if (id == 1)
      throw new RuntimeException("anonymous user record with id 1 is read only")
    val sTable = addTablePrefix("USER")
    val statement = connection.prepareStatement("UPDATE \"" + sTable + "\" SET \"homephone\" = ?, \"timestamp\" = now WHERE \"id\" = ?")
    statement.setString(1, arg)
    statement.setLong(2, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("User with ID = " + id + " not found")
    statement.close()
  }
  def userGetMainPhone(id: Long): String = {
    val resultset = userGet("\"telephonenumber\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("User with ID = " + id + " not found")
    }
    val result = resultset.getString(1)
    resultset.getStatement.close()
    result
  }
  def userSetMainPhone(id: Long, arg: String): Unit = {
    if (id == 1)
      throw new RuntimeException("anonymous user record with id 1 is read only")
    val sTable = addTablePrefix("USER")
    val statement = connection.prepareStatement("UPDATE \"" + sTable + "\" SET \"telephonenumber\" = ?, \"timestamp\" = now WHERE \"id\" = ?")
    statement.setString(1, arg)
    statement.setLong(2, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("User with ID = " + id + " not found")
    statement.close()
  }
  def userGetFaxNumber(id: Long): String = {
    val resultset = userGet("\"facsimiletelephonenumber\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("User with ID = " + id + " not found")
    }
    val result = resultset.getString(1)
    resultset.getStatement.close()
    result
  }
  def userSetFaxNumber(id: Long, arg: String): Unit = {
    if (id == 1)
      throw new RuntimeException("anonymous user record with id 1 is read only")
    val sTable = addTablePrefix("USER")
    val statement = connection.prepareStatement("UPDATE \"" + sTable + "\" SET \"facsimiletelephonenumber\" = ?, \"timestamp\" = now WHERE \"id\" = ?")
    statement.setString(1, arg)
    statement.setLong(2, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("User with ID = " + id + " not found")
    statement.close()
  }
  def userGetEMail(id: Long): String = {
    val resultset = userGet("\"mail\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("User with ID = " + id + " not found")
    }
    val result = resultset.getString(1)
    resultset.getStatement.close()
    result
  }
  def userSetEMail(id: Long, arg: String): Unit = {
    if (id == 1)
      throw new RuntimeException("anonymous user record with id 1 is read only")
    val sTable = addTablePrefix("USER")
    val statement = connection.prepareStatement("UPDATE \"" + sTable + "\" SET \"mail\" = ?, \"timestamp\" = now WHERE \"id\" = ?")
    statement.setString(1, arg)
    statement.setLong(2, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("User with ID = " + id + " not found")
    statement.close()
  }
  def userGetAvatar(id: Long): String = {
    val resultset = userGet("\"avatar\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("User with ID = " + id + " not found")
    }
    val result = resultset.getString(1)
    resultset.getStatement.close()
    result
  }
  def userSetAvatar(id: Long, arg: String): Unit = {
    if (id == 1)
      throw new RuntimeException("anonymous user record with id 1 is read only")
    val sTable = addTablePrefix("USER")
    val statement = connection.prepareStatement("UPDATE \"" + sTable + "\" SET \"avatar\" = ?, \"timestamp\" = now WHERE \"id\" = ?")
    statement.setString(1, arg)
    statement.setLong(2, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("User with ID = " + id + " not found")
    statement.close()
  }
  def userGetTimestamp(id: Long): DateTime = {
    val resultset = userGet("\"timestamp\"", "WHERE \"id\" = ?", Array(id))
    if (!resultset.next()) {
      throw new RuntimeException("User with ID = " + id + " not found")
    }
    val result = resultset.getTimestamp(1)
    resultset.getStatement.close()
    DT.toDateTime(result)
  }
  def userSetTimestamp(id: Long, arg: DateTime): Unit = {
    if (id == 1)
      throw new RuntimeException("anonymous user record with id 1 is read only")
    val sTable = addTablePrefix("USER")
    val statement = connection.prepareStatement("UPDATE \"" + sTable + "\" SET \"timestamp\" = ? WHERE \"id\" = ?")
    statement.setTimestamp(1, DT.toTimestamp(arg))
    statement.setLong(2, id)
    val result = statement.executeUpdate()
    if (result == 0)
      throw new RuntimeException("User with ID = " + id + " not found")
    statement.close()
  }
  def userGet(sqlFields: String, sqlConstraint: String, args: Array[Any] = Array()): ResultSet = {
    val sTable = addTablePrefix("USER")
    val statement = connection.prepareStatement("SELECT " + sqlFields + " FROM \"" + sTable + "\" " + sqlConstraint)
    addStatementParameters(statement, args)
    statement.executeQuery()
  }
  // -------------------
  // - private methods -
  // -------------------
}
