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

import java.io.File
import java.io.IOException
import java.util.Properties
import org.hsqldb.{Server => HSQLServer}
import org.slf4j.{Logger, LoggerFactory}

class Server(val databases: Properties,
             val trace: Boolean = false,
             val silent: Boolean = true,
             val port: Int = 9002) extends Runnable {
  private val log = LoggerFactory.getLogger(this.getClass)
  val hsqlServer = new HSQLServer()
  init()
  def init() = {
    if (this.databases == null || this.databases.size() == 0) {
      log.error("HSQLDB Server is configured, but no databases are found")
      log.error("HSQLDB Server not started.")
      throw new RuntimeException("HSQLDB Server is configured, but no databases are found")
    }
    this.hsqlServer.setSilent(this.silent)
    this.hsqlServer.setTrace(this.trace)
    this.hsqlServer.setPort(this.port)
    log.debug("configure HSQLDB with port: " + hsqlServer.getPort() +
                 ", silent: " + hsqlServer.isSilent() +
                 ", trace: " + hsqlServer.isTrace())
    val i = this.databases.entrySet().iterator()
    var index = 0
    while (i.hasNext()) {
      val current = i.next()
      val name = current.getKey().toString()
      val dbCfgPath = current.getValue().toString()
      log.debug("configuring database " + name + " with path " + dbCfgPath)
      var dbPath = dbCfgPath
      if (dbPath.startsWith("file:")) {
        dbPath = dbPath.substring(5)
      }
      // we test if the path points to a directory in the file system
      val directory = new File(dbPath)
      if (!directory.exists()) {
        throw new RuntimeException("path for hsqldb database does not exist: " + dbPath)
      }
      if (!directory.isDirectory()) {
        throw new RuntimeException("path for hsqldb database does not point to a directory: " + dbPath)
      }
      try {
        hsqlServer.setDatabasePath(index, new File(dbPath).getCanonicalPath() + File.separator + name)
        hsqlServer.setDatabaseName(index, name);
      } catch {
        case e: IOException => throw new RuntimeException("could not get database directory \"" + dbPath + "\"", e)
      }
      log.debug("database path for " + name + " is \"" + hsqlServer.getDatabasePath(index, true) + "\", index " + index)
      index += 1
    }
  }
  def halt(): Unit = {
    log.debug("Shutting down HSQLDB")
    // A newer version of hsqldb or SAP NetWeaver may not need the next line
    // DatabaseManager.closeDatabases(Database.CLOSEMODE_COMPACT)
    this.hsqlServer.stop()
    log.debug("Shutting down HSQLDB: Done");
  }
  def run(): Unit = {
    log.debug("Starting " + hsqlServer.getProductName() + " " + hsqlServer.getProductVersion() + " with parameters???")
    this.hsqlServer.start()
  }
}

object Server {
  private var thread: Thread = null
  private var server: Server = null
  def start(sprop: Properties): Unit = {
    if (server != null)
      throw new RuntimeException("HSQLDB server already started")
    server = new org.digimead.documentumelasticus.database.Server(sprop)
    thread = new Thread(server)
    thread.setDaemon(true)
    thread.start()
    // wait SERVER_STATE_ONLINE
    while (server.hsqlServer.getState() == 1) {
      try {
        Thread.sleep(100)
      } catch {
        case e =>
      }
    }
    // wait for port
    /*var fWaitForPort = true
    while (fWaitForPort) {
      Thread.sleep(100)
      try {
        val ss = new Socket("127.0.0.1", server.port)
        if (ss != null) {
          fWaitForPort = false
        }
        ss.close()
      } catch {
        case e =>
      }
    }*/
    // sleep a little
    Thread.sleep(1000)
  }
  def stop(): Unit = {
    if (server == null)
      return
    server.halt()
    thread.join(0)
    server = null
    thread = null
  }
}
