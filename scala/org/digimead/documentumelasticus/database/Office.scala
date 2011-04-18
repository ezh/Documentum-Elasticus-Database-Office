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

    //val oMri = O.SI[XIntrospection](oSM, "mytools.Mri", ctx)
    //oMri.inspect(oDoc)

package org.digimead.documentumelasticus.database

import com.sun.star.beans.PropertyState
import com.sun.star.beans.PropertyValue
import com.sun.star.beans.XPropertySet
import com.sun.star.container.XNameAccess
import com.sun.star.frame.XComponentLoader
import com.sun.star.frame.XModel
import com.sun.star.frame.XStorable
import com.sun.star.lang.XComponent
import com.sun.star.lang.DisposedException
import com.sun.star.lib.uno.helper.ComponentBase
import com.sun.star.sdb.XDocumentDataSource
import com.sun.star.sdb.XOfficeDatabaseDocument
import com.sun.star.sdbc.XDataSource
import com.sun.star.ucb.XSimpleFileAccess
import com.sun.star.uno.XComponentContext
import com.sun.star.uno.XNamingService
import com.sun.star.util.CloseVetoException
import com.sun.star.util.XCloseable
import com.sun.star.util.XModifiable
import java.io.File
import java.net.URL
import java.util.Properties
import org.slf4j.LoggerFactory
import org.digimead.documentumelasticus.component.XBase
import org.digimead.documentumelasticus.component.XBaseInfo
import org.digimead.documentumelasticus.helper._
import java.sql.DriverManager

class Office(val ctx: XComponentContext) extends ComponentBase
                                            with XDatabase
                                            with XStorageAPI
                                            with XFolderAPI
                                            with XFileAPI
                                            with XUserAPI
                                            with XGroupAPI
                                            with XDBFix
                                            with XDBUtils {
  protected val logger = LoggerFactory.getLogger(this.getClass)
  var componentSingleton = Office.componentSingleton
  val componentTitle = Office.componentTitle
  val componentDescription = Office.componentDescription
  val componentURL = Office.componentURL
  val componentName = Office.componentName
  val componentServices = Office.componentServices
  val componentDisabled = Office.componentDisabled
  private var aLocalPath: File = null // path to local folder
  initialize(Array()) // initialized by default
  logger.info(componentName + " active")
  def show(): Unit = {
  }
  def hide(): Unit = {
  }
  def refresh(): Unit = {
  }
  def open(sourceName: String, odbLocation: String, fCreate: Option[Boolean]): Boolean = {
    val locationURL = new URL(odbLocation)
    var (oDataFile, oDataSource, dbPath) = openDataBase(sourceName, locationURL)
    if (oDataSource == null) {
      if (fCreate == Some(true)) {
        val (file, source, path) = createDataBase(locationURL)
        oDataFile = file
        oDataSource = source
        dbPath = path
      } else if (fCreate == None) {
        val result = UI.showMessageBox("warningbox", com.sun.star.awt.MessageBoxButtons.BUTTONS_OK_CANCEL |
                                       com.sun.star.awt.MessageBoxButtons.DEFAULT_BUTTON_OK, "Database not found",
                                       "Do you want to create database at \"" +
                                       locationURL.toString().replaceFirst("^file:/(?=[^/])","file:///") + "\"?", ctx)
        if (result == 1) { // RESULT_OK
          val (file, source, path) = createDataBase(locationURL)
          oDataFile = file
          oDataSource = source
          dbPath = path
        }
      }
    }
    // assign values and initialize database
    if (oDataFile != null && oDataSource != null) {
      aName = sourceName
      aURL = locationURL
      document = oDataFile
      aLocalPath = dbPath
      // connection
      val oDataSource = O.I[XOfficeDatabaseDocument](document).getDataSource()
      val oDataSourceProps = O.I[XPropertySet](oDataSource)
      val dbURL = oDataSourceProps.getPropertyValue("URL").asInstanceOf[String]
      if (aLocalPath != null) {
        val dbName = aLocalPath.getName().substring(1)
        val sprop: Properties = new Properties() {put(dbName, aLocalPath.getAbsolutePath())}
        Server.start(sprop)
      }
      Class.forName("org.hsqldb.jdbcDriver")
      logger.info("connect to database " + dbURL)
      connection = DriverManager.getConnection(dbURL, "sa", "")
      fixDataSource(connection)
    } else {
      logger.error("could not obtain datasource with URL " + locationURL.toString().replaceFirst("^file:/(?=[^/])","file:///"))
      false
    }
  }
  def save(): Boolean = {
    logger.trace("save database")
    val oDocStorable = O.I[XStorable](document)
    oDocStorable.store()
    true
  }
  def close(): Unit = {
    logger.trace("close database \"" + aName + "\"")
    // close connection
    if (connection != null && connection.isValid(5)) {
      if (connection.getAutoCommit()) {
        logger.trace("database autocommit is ON")
      } else {
        logger.trace("database autocommit is OFF; commit")
        connection.commit()
      }
      if (aLocalPath != null) {
        connection.createStatement().executeUpdate("SHUTDOWN COMPACT")
      }
      connection.close()
      connection = null
    }
    // stop server; only if started
    Server.stop()
    // close office ODB document
    val xModifiable = O.I[XModifiable](document)
    val xStorable = O.I[XStorable](document)
    try {
      if (xModifiable != null && xModifiable.isModified()){
        logger.trace("database document was modified")
        if (xStorable.hasLocation() && xStorable.isReadonly()) {
          xStorable.store()
          logger.trace("database document has been stored")
        }else{
          xModifiable.setModified(false)
        }
      } else {
        logger.trace("database document was NOT modified")
      }
      // Check supported functionality of the document (model or controller).
      val xModel: XModel = O.I[XModel](document).asInstanceOf[XModel]
      if (xModel != null) {
        // It is a full featured office document.
        // Try to use close mechanism instead of a hard dispose().
        // But maybe such service is not available on this model.
        logger.trace("detect full featured office document")
        val xCloseable: XCloseable = O.I[XCloseable](xModel)
        if (xCloseable != null) {
          logger.trace("detect XCloseable interface in document, close")
          try {
            // use close(boolean DeliverOwnership)
            // The boolean parameter DeliverOwnership tells objects vetoing the close process that they may
            // assume ownership if they object the closure by throwing a CloseVetoException
            // Here we give up ownership. To be on the safe side, catch possible veto exception anyway.
            xCloseable.close(true)
          } catch {
            case e: CloseVetoException => logger.warn("CloseVetoException")
          }
        } else {
          // If close is not supported by this model - try to dispose it.
          // But if the model disagree with a reset request for the modify state
          // we shouldn't do so. Otherwhise some strange things can happen.
          logger.trace("XCloseable interface is absent in document, dispose")
          val xDisposeable: XComponent = O.I[XComponent](xModel)
          xDisposeable.dispose()
        }
      }
    } catch {
      case e: DisposedException => logger.warn(e.getMessage(), e)
    }
    document = null
  }
  def delete(): Unit = {
    if (aURL != null) {
      delete(aName, aURL.toString().replaceFirst("^file:/(?=[^/])","file:///"))
    } else {
      if (aName != null && aName.length > 0) {
        delete(aName, "")
      } else {
        logger.trace("try to delete uninitialized database; action skip")
      }
    }
  }
  def delete(sourceName: String, odbLocation: String): Unit = {
    logger.trace("delete database with source name \"" + sourceName + "\" and location " + odbLocation)
    val locationURL = new URL(odbLocation)
    if (((sourceName.length > 0) && (sourceName == aName)) ||
        (locationURL.equals(aURL))) {
      if (connection != null && document != null)
        close()
    }
    // rm source name if any
    if (sourceName.length > 0) {
      val xNamingService = O.SI[XNamingService](mcf, "com.sun.star.sdb.DatabaseContext", ctx)
      if (O.I[XNameAccess](xNamingService).hasByName(sourceName)) {
        logger.warn("revoke data source " + sourceName)
        xNamingService.revokeObject(sourceName)
      }
    }
    // rm odb if any
    val oSimpleFileAccess = O.SI[XSimpleFileAccess](mcf, "com.sun.star.ucb.SimpleFileAccess", ctx)
    if (oSimpleFileAccess.exists(locationURL.toString().replaceFirst("^file:/(?=[^/])","file:///"))) {
      logger.warn("remove ODB file " + locationURL.toString().replaceFirst("^file:/(?=[^/])","file:///"))
      oSimpleFileAccess.kill(locationURL.toString().replaceFirst("^file:/(?=[^/])","file:///"))
    }
    // rm database directory if any
    if (locationURL.getProtocol() == "file") {
      val dbFile = new File(locationURL.toURI())
      val dbName = dbFile.getName().substring(0, dbFile.getName().length - 4)
      val dbPath = new File(dbFile.getParent() + File.separator + "." + dbName)
      if (dbPath.exists()) {
      logger.warn("remove database directory" + dbPath.toString)
      def deleteFolder(folder: File): Unit = {
          val files = folder.listFiles
          if (files != null) {
            for (f <- files) {
              if (f.isDirectory) {
                deleteFolder(f)
              } else {
                f.delete
              }
            }
          }
          folder.delete
        }
        deleteFolder(dbPath)
      }
    }
  }
  def getVersion(): String = {
    val sVersionTable = addTablePrefix("METADATA")
    val md = connection.getMetaData()
    if (md.getTables(null, null, sVersionTable, Array("TABLE")).next())
      return selectRow(connection, "SELECT VALUE FROM METADATA WHERE KEY='VERSION'")(0).asInstanceOf[String]
    return ""
  }
  def isInitialized() = Office.initialized
  // -----------------------------------
  // - implement trait XInitialization -
  // -----------------------------------
  def initialize(args: Array[AnyRef]) = synchronized {
    logger.info("initialize " + componentName)
    if (isInitialized())
      throw new RuntimeException("Initialization of " + componentName + " already done")
    Office.initialized = true
  }
  // ------------------------------
  // - implement trait XComponent -
  // ------------------------------
  override def dispose(): Unit = synchronized {
    logger.info("dispose " + componentName)
    if (!isInitialized()) {
      logger.warn("dispose of " + componentName + " already done")
      return
    }
    close()
    super.dispose()
    Office.initialized = false
  }
  // -------------------
  // - restricted methods -
  // -------------------
  protected def createDataBase(locationURL: java.net.URL): (XComponent, XDataSource, File) = {
    logger.trace("create database with url \"" + locationURL.toString.replaceFirst("^file:/(?=[^/])","file:///") + "\"")
    if (locationURL.getProtocol() != "file")
      throw new RuntimeException("Incorrect database location " + locationURL.toString + ". Provide file:///... for automatic database creation")
    if (!locationURL.toString.toUpperCase.endsWith(".ODB"))
      throw new RuntimeException("Incorrect database location " + locationURL.toString + ". Provide full path to ODB file")
    val dbFile = new File(locationURL.toURI())
    val dbName = dbFile.getName().substring(0, dbFile.getName().length - 4)
    val dbPath = new File(dbFile.getParent() + File.separator + "." + dbName)
    if (dbFile.exists())
      throw new RuntimeException("ODB file \"" + dbFile.toString + "\" already exists")
    if (dbPath.exists())
      throw new RuntimeException("Database path \"" + dbPath.toString + "\" already exists")
    val oDesk = O.SI[XComponentLoader](mcf, "com.sun.star.frame.Desktop", ctx)
    // create a new blank (empty) office database file ... does NOT yet contain any connection info
    logger.debug("create private:factory/sdatabase")
    val oDataFile = oDesk.loadComponentFromURL("private:factory/sdatabase", "_default", 0, Array[PropertyValue](new PropertyValue("Hidden", 0, true, PropertyState.DIRECT_VALUE)))
    logger.debug("store private:factory/sdatabase")
    val oDocStorable = O.I[XStorable](oDataFile)
    oDocStorable.storeAsURL(locationURL.toString.replaceFirst("^file:/(?=[^/])","file:///"), Array[PropertyValue]())
    logger.debug("retrieve datasource")
    Thread.sleep(1000) // BUG in office? hang, if too fast ;-)
    val oDataBase = O.I[XOfficeDatabaseDocument](oDataFile)
    val oDataSource = oDataBase.getDataSource()
    val oDataSourceProps = O.I[XPropertySet](oDataSource)
    // create HSQL database
    logger.debug("create HSQL database")
    if (!dbPath.mkdirs())
      throw new RuntimeException("mkdir failed \"" + dbPath.toString + "\"")
    // = = =  below is an example of a JDBC database engine = = =
    // oDoc.Datasource.URL = "jdbc:hsqldb:file:c://Program Files/HSQL-20/data/mydb;default_schema=true;shutdown=true;hsqldb.default_table_type=cached;get_column_name=false"
    // oDoc.Datasource.User = "SA"
    // oDoc.Datasource.Settings.JavaDriverClass = "org.hsqldb.jdbcDriver"
    // = = = = = = = = = = = = = = = = = = = = = = = = = = =
    // define use of HSQL database as type of this one
    oDataSourceProps.setPropertyValue("URL", "jdbc:hsqldb:hsql://localhost:9002/" + dbName)
    oDataSourceProps.setPropertyValue("User", "sa")
    oDataSourceProps.setPropertyValue("Info", Array(new PropertyValue("JavaDriverClass", 0, "org.hsqldb.jdbcDriver", PropertyState.DIRECT_VALUE)))
    oDocStorable.store()
    (oDataFile, oDataSource, dbPath)
  }
  protected def openDataBase(name: String, locationURL: java.net.URL): (XComponent, XDataSource, File)  = {
    val xNamingService = O.SI[XNamingService](mcf, "com.sun.star.sdb.DatabaseContext", ctx)
    var oDataFile: XComponent = null
    var oDataSource: XDataSource = null
    var dbPath: File = null
    // try to obtain datasource from xNamingService
    try {
      logger.trace("try to open registered datasource \"" + name + "\"")
      val oDataBase = O.I[XDocumentDataSource](xNamingService.getRegisteredObject(name))
      oDataFile = O.I[XComponent](oDataBase.getDatabaseDocument())
      oDataSource = O.I[XDataSource](oDataBase)
      logger.info("datasource \"" + name + "\" found")
    } catch {
      case e => logger.info("datasource \"" + name + "\" not found")
    }
    if (oDataSource == null) {
      // try to obtain datasource from ODB file
      logger.trace("try to open unregistered datasource with url \"" + locationURL.toString.replaceFirst("^file:/(?=[^/])","file:///") + "\"")
      val oSimpleFileAccess = O.SI[XSimpleFileAccess](mcf, "com.sun.star.ucb.SimpleFileAccess", ctx)
      if (oSimpleFileAccess.exists(locationURL.toString().replaceFirst("^file:/(?=[^/])","file:///"))) {
        val oDesk = O.SI[XComponentLoader](mcf, "com.sun.star.frame.Desktop", ctx)
        oDataFile = oDesk.loadComponentFromURL(locationURL.toString().replaceFirst("^file:/(?=[^/])","file:///"), "_blank", 0, Array[PropertyValue]())
        val oDataBase = O.I[XOfficeDatabaseDocument](oDataFile)
        oDataSource = oDataBase.getDataSource()
      }
    }
    if (oDataFile != null) {
      // search dbLocalPath
      val oDocStorable = O.I[XStorable](oDataFile)
      if (oDocStorable.hasLocation()) {
        val url = new URL(oDocStorable.getLocation())
        if (url.getProtocol() == "file") {
          val dbFile = new File(url.toURI)
          val dbName = dbFile.getName().substring(0, dbFile.getName().length - 4)
          val tmpPath = new File(dbFile.getParent() + File.separator + "." + dbName)
          if (tmpPath.exists) {
            dbPath = tmpPath
            logger.debug("find folder \"" + dbPath.toString() + "\" for local database; mask database as local")
          }
        }
      }
    }
    (oDataFile, oDataSource, dbPath)
  }
}

object Office extends XBaseInfo {
  private val logger = LoggerFactory.getLogger(this.getClass.getName)
  var componentSingleton: Option[XBase] = None
  val componentTitle = "Documentum Elasticus Storage"
  val componentDescription = "local file storage component"
  val componentURL = "http://www."
  val componentName = classOf[Office].getName
  val componentServices: Array[String] = Array(componentName)
  val componentDisabled = false
  var initialized: Boolean = false
  logger.info(componentName + " active")
  /**
   * @param args the command line arguments
   */
  def main(args: Array[String]): Unit = {
    println("Hello, world!")
  }
}



//TODO http://www.google.com/codesearch/p?hl=ru#wsF1keyOAzI/trunk/odk/examples/DevelopersGuide/Forms/DataAwareness.java&q=lang:java XTablesSupplier UnoRuntime exists XTablesSupplier&sa=N&cd=4&ct=rc
/*
 *   def updateStorage(tStorage: XStorageUNO): Boolean = {
    val storage = tStorage.asInstanceOf[XStorage]
/*    if (storage.aID == -1) {
      // read from database
      val sTable = addTablePrefix("STORAGES")
      val statement = connection.prepareStatement("SELECT * FROM " + sTable + " WHERE NAME = ?")
      val params = O.I[XParameters](statement)
      params.setString(1, storage.getName())
      var rs = statement.executeQuery()
      if (!rs.next()) {
        createStorage(storage)
        rs = statement.executeQuery() // reexec
        rs.next()
      } else {
        logger.debug("update exists storage \"" + storage.getName()+ "\"")
      }
      val row = O.I[XRow](rs)
      storage.aID = row.getInt(1)
      // skip name
      //val owner_id(row.getInt(3) // TODO
      //val group_id(row.getInt(4) // TODO
      //val lastUser_id(row.getInt(5) // TODO
      storage.aLastTime = row.getTimestamp(6)
      storage.aDescription = row.getString(7)
      storage.aCreatedAt = row.getTimestamp(8)
      storage.aUpdatedAt = row.getTimestamp(9)
      storage.aDocumentsCount = row.getInt(10)
      storage.aFoldersCount = row.getInt(11)
      storage.aTimestamp = row.getTimestamp(12)
      true
    } else {
      // save to database
      //throw new RuntimeException("Storage \"" + storage.getName() + "\" already updated")
      true
    }*/
   true
  }
  def getStorageFoldersTree(storage: XStorageUNO): HashMap[Int, Array[XFolderUNO]] = {
    val folders: HashMap[Int, Array[XFolderUNO]] = HashMap()
    val sTable = addTablePrefix("FOLDERS")
    val statement = connection.prepareStatement("SELECT ID, Parent_ID FROM " + sTable + " WHERE STORAGE_ID = ?")
    val params = O.I[XParameters](statement)
    params.setLong(1, storage.getID())
    var rs = statement.executeQuery()
    if (rs.next()) {
      val row = O.I[XRow](rs)
      do {
        val folder = O.SI[XFolderUNO](mcf, "org.digimead.documentumelasticus.FolderUNO", ctx)
//        folder.setID(row.getInt(1))
        updateFolder(folder)
        val parentID = row.getInt(2)
        if (!folders.contains(parentID))
          folders(parentID) = Array()
        folders(parentID) = folders(parentID) ++ Array(folder)
      } while (rs.next())
    }
    folders
  }
  def updateFolder(folder: XFolderUNO): Boolean = {
    if (folder.getID() == -1) {
      val statement = connection.prepareStatement("SELECT * FROM FOLDERS WHERE PATH = ?")
      val params = O.I[XParameters](statement)
      params.setString(1, folder.getPath())
      var rs = statement.executeQuery()
      if (!rs.next()) {
        createFolder(folder)
        rs = statement.executeQuery() // reexec
        rs.next()
        val row = O.I[XRow](rs)
  //      folder.setID(row.getInt(1))
        return true
      } else {
        logger.debug("update exists folder \"" + folder.getPath()+ "\"")
      }
      // merge data from select
      val row = O.I[XRow](rs)
      updateFolderObjectFromRecord(row, folder)
      return true
    } else {
      // update exists if ts > ts
      val sTable = addTablePrefix("FOLDERS")
      val statement = connection.prepareStatement("SELECT * FROM " + sTable + " WHERE ID = ?")
      val params = O.I[XParameters](statement)
      params.setLong(1, folder.getID())
      var rs = statement.executeQuery()
      if (rs.next()) {
        val row = O.I[XRow](rs)
        if (DT.toCalendar(row.getTimestamp(24)).after(DT.toCalendar(folder.getTimestamp()))) {
            updateFolderObjectFromRecord(row, folder)
        } else {
            updateFolderRecordFromObject(folder)
        }
        return true
      }
    }
    def updateFolderObjectFromRecord(row: XRow, folder: XFolderUNO) = {
    //  folder.setID(row.getInt(1))
      //folder.setStorage(row.getInt(2))
      //val parent_id(row.getInt(3) // TODO
      //val owner_id(row.getInt(4) // TODO
      //val group_id(row.getInt(5) // TODO
      //val lastUser_id(row.getInt(6) // TODO
      folder.setLastTime(row.getTimestamp(7))
      folder.setName(row.getString(8))
      folder.setPath(row.getString(9))
      folder.setDescription(row.getString(10))
      folder.setNote(row.getString(11))
      folder.setShared(row.getBoolean(12))
      folder.setPrivate(row.getBoolean(13))
      folder.setPermission(row.getInt(14))
      folder.setCreatedAt(row.getTimestamp(15))
      folder.setUpdatedAt(row.getTimestamp(16))
      folder.setDocumentsCount(row.getInt(17))
      folder.setFoldersCount(row.getInt(18))
      folder.setFilterMask(row.getString(19))
      folder.setSortType(row.getShort(20))
      folder.setSortDirection(row.getBoolean(21))
      folder.setDisplayOrder(row.getInt(22))
      folder.setUndeletable(row.getBoolean(23))
      folder.setTimestamp(row.getTimestamp(24))
    }
    def updateFolderRecordFromObject(folder: XFolderUNO) = {

    }
    false
  }
  def updateFile(file: XFileUNO): Boolean = {
    if (file.getID() == -1) {
      val statement = connection.prepareStatement("SELECT * FROM FILES WHERE PATH = ?")
      val params = O.I[XParameters](statement)
      params.setString(1, file.getPath())
      var rs = statement.executeQuery()
      if (!rs.next()) {
        createFile(file)
        rs = statement.executeQuery() // reexec
        rs.next()
        val row = O.I[XRow](rs)
      //  file.setID(row.getInt(1))
        return true
      } else {
        logger.debug("update exists file \"" + file.getPath()+ "\"")
      }
      // merge data from select
      val row = O.I[XRow](rs)
      updateFileObjectFromRecord(row, file)
      return true
    } else {
      // update exists if ts > ts
      false
    }
    false
  }
  def updateFileObjectFromRecord(row: XRow, file: XFileUNO) = {
    //file.setID(row.getInt(1))
    //file.setStorage(row.getInt(2))
    //val parent_id(row.getInt(3) // TODO
    //val owner_id(row.getInt(4) // TODO
    //val group_id(row.getInt(5) // TODO
    //val lastUser_id(row.getInt(6) // TODO
    file.setLastTime(row.getTimestamp(7))
    file.setName(row.getString(8))
    file.setPath(row.getString(9))
    file.setDescription(row.getString(10))
    file.setNote(row.getString(11))
    file.setPermission(row.getInt(12))
    file.setCreatedAt(row.getTimestamp(13))
    file.setUpdatedAt(row.getTimestamp(14))
    file.setSize(row.getInt(15))
    file.setMimeType(row.getString(16))
    file.setHash(row.getString(17))
    file.setVersion(row.getInt(18))
    file.setReadCount(row.getInt(19))
    file.setWriteCount(row.getInt(20))
    file.setDisplayOrder(row.getInt(21))
    file.setUndeletable(row.getBoolean(22))
    file.setTimestamp(row.getTimestamp(23))
  }
  // ---------------------------------------------------------------------------
  private def createDataBase(name: String, locationURL: java.net.URL, interactive: Boolean): (XComponent, XDataSource) = {
    logger.trace("create datasource for \"" + name + "\" with url \"" + locationURL.toString.replaceFirst("^file:/(?=[^/])","file:///") + "\"")
    val oDesk = O.SI[XComponentLoader](mcf, "com.sun.star.frame.Desktop", ctx)
    // create a new blank (empty) office database file ... does NOT yet contain any connection info
    val oDataFile = oDesk.loadComponentFromURL("private:factory/sdatabase", "_blank", 0, Array[PropertyValue]())
    val oDataBase = O.I[XOfficeDatabaseDocument](oDataFile)
    val oDataSource = oDataBase.getDataSource();
    val oDataSourceProps = O.I[XPropertySet](oDataSource)
    // TODO
    // = = =  below is an example of a JDBC database engine = = =
    // oDoc.Datasource.URL = "jdbc:hsqldb:file:c://Program Files/HSQL-20/data/mydb;default_schema=true;shutdown=true;hsqldb.default_table_type=cached;get_column_name=false"
    // oDoc.Datasource.User = "SA"
    // oDoc.Datasource.Settings.JavaDriverClass = "org.hsqldb.jdbcDriver"
    // = = = = = = = = = = = = = = = = = = = = = = = = = = =
    // define use of embedded HSQL database as type of this one
    oDataSourceProps.setPropertyValue("URL", "sdbc:embedded:hsqldb");
    val oDocStorable = O.I[XStorable](oDataFile);
    oDocStorable.storeAsURL(locationURL.toString.replaceFirst("^file:/(?=[^/])","file:///"), Array[PropertyValue]())

/*
// retrieve the DatabaseContext and get its com.sun.star.container.XNameAccess interface
    val xNamingService = O.SI[XNamingService](mcf, "com.sun.star.sdb.DatabaseContext", ctx);
    // register the new db name
    logger.trace("register datasource for \"" + name + "\" with url \"" + locationURL.toString.replaceFirst("^file:/(?=[^/])","file:///") + "\"")
    // revoke the data source, if it previously existed
    if (O.I[XNameAccess](xNamingService).hasByName(name)) {
      logger.warn("revoke old data source " + name)
      xNamingService.revokeObject(name)
    }
    xNamingService.registerObject(name, oDataSource)
    val oDocModel = O.I[XModel](oDataFile);
    val oDocFrame = oDocModel.getCurrentController().getFrame()
*/
    (oDataFile, oDataSource)
  }
  private def openDataBase(name: String, locationURL: java.net.URL): (XComponent, XDataSource)  = {
    val oSM = ctx.getServiceManager()
    val xNamingService = O.SI[XNamingService](oSM, "com.sun.star.sdb.DatabaseContext", ctx)
    var oDataFile: XComponent = null
    var oDataSource: XDataSource = null
    // try to obtain datasource from xNamingService
    try {
      logger.trace("try to open registered datasource \"" + name + "\"")
      val oDataBase = O.I[XDocumentDataSource](xNamingService.getRegisteredObject(name))
      oDataFile = O.I[XComponent](oDataBase.getDatabaseDocument())
      oDataSource = O.I[XDataSource](oDataBase)
      logger.info("datasource \"" + name + "\" found")
    } catch {
      case e => logger.info("datasource \"" + name + "\" not found")
    }
    if (oDataSource == null) {
      // try to obtain datasource from ODB file
      logger.trace("try to open unregistered datasource with url \"" + locationURL.toString.replaceFirst("^file:/(?=[^/])","file:///") + "\"")
      val oSimpleFileAccess = O.SI[XSimpleFileAccess](oSM, "com.sun.star.ucb.SimpleFileAccess", ctx)
      if (oSimpleFileAccess.exists(locationURL.toString().replaceFirst("^file:/(?=[^/])","file:///"))) {
        val oDesk = O.SI[XComponentLoader](oSM, "com.sun.star.frame.Desktop", ctx)
        oDataFile = oDesk.loadComponentFromURL(locationURL.toString().replaceFirst("^file:/(?=[^/])","file:///"), "_blank", 0, Array[PropertyValue]())
        val oDataBase = O.I[XOfficeDatabaseDocument](oDataFile)
        oDataSource = oDataBase.getDataSource()
      }
    }
    (oDataFile, oDataSource)
  }
  private def closeDataSource(): Boolean = {
            //TODO!!
        //val oDocModel = O.I[XModel](oDoc);
        //val oDocFrame = oDocModel.getCurrentController().getFrame()
        //O.I[XCloseable](oDocFrame).close(true)
        //oDoc.dispose()
        //    //val oMri = O.SI[XIntrospection](oSM, "mytools.Mri", ctx)
    //oMri.inspect(oDocFrame)
    //O.I[XCloseable](oDocFrame).close(true)
    //oDoc.dispose()
    true
  }
  private def createStorage(storage: XStorageUNO) {
    logger.debug("create new storage \"" + storage.getName()+ "\"")
    val calendar = Calendar.getInstance()
/*    if (storage.getUpdatedAt() == null)
      storage.setUpdatedAt(DT.toDateTime(calendar))
    if (storage.getCreatedAt() == null)
      storage.setCreatedAt(storage.getUpdatedAt())
    if (storage.getTimestamp() == null)
      storage.setTimestamp(DT.toDateTime(calendar))*/
    /*val sTable = addTablePrefix("STORAGES")
    val statementIns = connection.prepareStatement("INSERT INTO " + sTable + " ("
                                                    +"\"NAME\", \"OWNER_ID\", \"GROUP_ID\","
                                                    +"\"LAST_USER\", \"LAST_TIME\","
                                                    +"\"DESCRIPTION\", \"CREATED_AT\","
                                                    +"\"UPDATED_AT\", \"DOCUMENTS_COUNT\","
                                                    +"\"FOLDERS_COUNT\", \"TIMESTAMP\") VALUES ("
                                                    +"?,?,?,?,?,?,?,?,?,?,?)"
    )
    val params = O.I[XParameters](statementIns)
    params.setString(1, storage.getName())
    if (storage.getOwner() == null) {
      params.setNull(2, DataType.BIGINT)
    } else {
      params.setInt(2, storage.getOwner().getID())
    }
    if (storage.getGroup() == null) {
      params.setNull(3, DataType.BIGINT)
    } else {
      params.setInt(3, storage.getGroup().getID())
    }
    if (storage.getLastUser() == null) {
      params.setNull(4, DataType.BIGINT)
    } else {
      params.setInt(4, storage.getLastUser().getID())
    }
    if (storage.getLastTime() == null) {
      params.setNull(4, DataType.TIMESTAMP)
    } else {
      params.setTimestamp(5, storage.getLastTime())
    }
    params.setString(6, storage.getDescription())
    params.setTimestamp(7, storage.getCreatedAt())
    params.setTimestamp(8, storage.getUpdatedAt())
    params.setInt(9, storage.getDocumentsCount())
    params.setInt(10, storage.getFoldersCount())
    params.setTimestamp(11, storage.getTimestamp())
    try {
      statementIns.executeUpdate()
    } catch {
      case e => logger.error("insert new storage failed: ", e)
    }*/
  }
  private def createFolder(folder: XFolderUNO) {
    logger.debug("create new folder \"" + folder.getPath()+ "\"")
    val calendar = Calendar.getInstance()
    if (folder.getUpdatedAt() == null)
      folder.setUpdatedAt(DT.toDateTime(calendar))
    if (folder.getCreatedAt() == null)
      folder.setCreatedAt(folder.getUpdatedAt())
    if (folder.getTimestamp() == null)
      folder.setTimestamp(DT.toDateTime(calendar))
    val sTable = addTablePrefix("FOLDERS")
    val statementIns = connection.prepareStatement("INSERT INTO " + sTable + " ("
                                                    +"\"STORAGE_ID\", \"PARENT_ID\", \"OWNER_ID\", \"GROUP_ID\","
                                                    +"\"LAST_USER\", \"LAST_TIME\","
                                                    +"\"NAME\", \"PATH\", \"DESCRIPTION\","
                                                    +"\"NOTE\", \"SHARED\", \"PRIVATE\","
                                                    +"\"PERMISSION\", \"CREATED_AT\","
                                                    +"\"UPDATED_AT\", \"DOCUMENTS_COUNT\","
                                                    +"\"FOLDERS_COUNT\", \"FILTER_MASK\","
                                                    +"\"SORT_TYPE\", \"SORT_DIRECTION\","
                                                    +"\"DISPLAY_ORDER\", \"UNDELETABLE\","
                                                    +"\"TIMESTAMP\") VALUES ("
                                                    +"?,?,?,?,?,?,?,?,?,?,?,?,?,?,"
                                                    +"?,?,?,?,?,?,?,?,?)"
    )
    val params = O.I[XParameters](statementIns)
    params.setLong(1, folder.getStorage().getID())
/*    if (folder.getParent() == null) {
      params.setNull(2, DataType.BIGINT)
    } else {
      params.setInt(2, folder.getParent().getID())
    }
    if (folder.getOwner() == null) {
      params.setNull(3, DataType.BIGINT)
    } else {
      params.setInt(3, folder.getOwner().getID())
    }
    if (folder.getGroup() == null) {
      params.setNull(4, DataType.BIGINT)
    } else {
      params.setInt(4, folder.getGroup().getID())
    }
    if (folder.getLastUser() == null) {
      params.setNull(5, DataType.BIGINT)
    } else {
      params.setInt(5, folder.getLastUser().getID())
    }
    if (folder.getLastTime() == null) {
      params.setNull(6, DataType.TIMESTAMP)
    } else {
      params.setTimestamp(6, folder.getLastTime())
    }
    params.setString(7, folder.getName())
    params.setString(8, folder.getPath())
    params.setString(9, folder.getDescription())
    params.setString(10, folder.getNote())
    params.setBoolean(11, folder.getShared())
    params.setBoolean(12, folder.getPrivate())
    params.setLong(13, folder.getPermission())
    params.setTimestamp(14, folder.getCreatedAt())
    params.setTimestamp(15, folder.getUpdatedAt())
    params.setInt(16, folder.getDocumentsCount())
    params.setInt(17, folder.getFoldersCount())
    params.setString(18, folder.getFilterMask())
    params.setInt(19, folder.getSortType())
    params.setBoolean(20, folder.getSortDirection())
    params.setInt(21, folder.getDisplayOrder())
    params.setBoolean(22, folder.getUndeletable())
    params.setTimestamp(23, folder.getTimestamp())*/
    try {
      statementIns.executeUpdate()
    } catch {
      case e => logger.error("insert new folder failed: ", e)
    }
  }
  private def createFile(file: XFileUNO) {
    logger.debug("create new file \"" + file.getPath()+ "\"")
    val calendar = Calendar.getInstance()
    if (file.getUpdatedAt() == null)
      file.setUpdatedAt(DT.toDateTime(calendar))
    if (file.getCreatedAt() == null)
      file.setCreatedAt(file.getUpdatedAt())
    if (file.getTimestamp() == null)
      file.setTimestamp(DT.toDateTime(calendar))
    val sTable = addTablePrefix("FILES")
    val statementIns = connection.prepareStatement("INSERT INTO " + sTable + " ("
                                                    +"\"STORAGE_ID\", \"PARENT_ID\", \"OWNER_ID\", \"GROUP_ID\","
                                                    +"\"LAST_USER\", \"LAST_TIME\","
                                                    +"\"NAME\", \"PATH\", \"DESCRIPTION\","
                                                    +"\"NOTE\", \"PERMISSION\", \"CREATED_AT\","
                                                    +"\"UPDATED_AT\", \"SIZE\","
                                                    +"\"MIMETYPE\", \"HASH\", \"VERSION\","
                                                    +"\"READCOUNT\",\"WRITECOUNT\","
                                                    +"\"DISPLAY_ORDER\", \"UNDELETABLE\","
                                                    +"\"TIMESTAMP\") VALUES ("
                                                    +"?,?,?,?,?,?,?,?,?,?,?,?,"
                                                    +"?,?,?,?,?,?,?,?,?,?)"
    )
    val params = O.I[XParameters](statementIns)
    params.setLong(1, file.getStorage().getID())
 /*   if (file.getParent() == null) {
      params.setNull(2, DataType.BIGINT)
    } else {
      params.setInt(2, file.getParent().getID())
    }
    if (file.getOwner() == null) {
      params.setNull(3, DataType.BIGINT)
    } else {
      params.setInt(3, file.getOwner().getID())
    }
    if (file.getGroup() == null) {
      params.setNull(4, DataType.BIGINT)
    } else {
      params.setInt(4, file.getGroup().getID())
    }
    if (file.getLastUser() == null) {
      params.setNull(5, DataType.BIGINT)
    } else {
      params.setInt(5, file.getLastUser().getID())
    }
    if (file.getLastTime() == null) {
      params.setNull(6, DataType.TIMESTAMP)
    } else {
      params.setTimestamp(6, file.getLastTime())
    }
    params.setString(7, file.getName())
    params.setString(8, file.getPath())
    params.setString(9, file.getDescription())
    params.setString(10, file.getNote())
    params.setLong(11, file.getPermission())*/
    params.setTimestamp(12, file.getCreatedAt())
    params.setTimestamp(13, file.getUpdatedAt())
    params.setLong(14, file.getSize())
    params.setString(15, file.getMimeType())
    params.setString(16, file.getHash())
    params.setLong(17, file.getVersion())
    params.setLong(18, file.getReadCount())
    params.setLong(19, file.getWriteCount())
    params.setLong(20, file.getDisplayOrder())
    params.setBoolean(21, file.getUndeletable())
    params.setTimestamp(22, file.getTimestamp())
    try {
      statementIns.executeUpdate()
    } catch {
      case e => logger.error("insert new file failed: ", e)
    }
  }
 */