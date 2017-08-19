package exactlyonce

import java.io.ByteArrayOutputStream
import java.net.URI
import java.nio.ByteBuffer

import com.datastax.driver.core.{BatchStatement, Cluster, PreparedStatement, Session}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable

import scala.collection.JavaConversions._

case class CFile(path: String, name: String, isDir: Boolean, length: Long, value: ByteBuffer)


class CassandraSimpleFileSystem extends org.apache.hadoop.fs.FileSystem {

  var session: Option[Session] = None
  var deleteStatement: Option[PreparedStatement] = None
  var deleteDirStatement: Option[PreparedStatement] = None
  var insertStatement: Option[PreparedStatement] = None
  var insertDirStatement: Option[PreparedStatement] = None
  var selectStatement: Option[PreparedStatement] = None
  var listStatement: Option[PreparedStatement] = None

  val replication = 3
  private var uri: Option[URI] = None

  private var workingDirectory = new Path("/")

  private def getPathAndName(path: Path) = {
    val parent = if (path.isRoot) "/" else path.getParent.toString
    (parent, path.getName)
  }

  private def getFile(path: Path): Option[CFile] = {
    val pathAndName = getPathAndName(path)
    getFile(pathAndName._1, pathAndName._2)
  }

  private def getFile(path: String, name: String): Option[CFile] = {
    val row = session.get.execute(selectStatement.get.bind(path, name)).one()
    if (row == null) {
      None
    } else {
      Some(CFile(row.getString("path"), row.getString("name"), row.getBool("is_dir"), row.getLong("length"), row.getBytes("value")))
    }
  }

  override def initialize(name: URI, conf: Configuration): Unit = {
    setConf(conf)
    uri = Some(name)
    val contactPoints = Option(name.getHost).getOrElse(conf.get("cassandra.host", "localhost"))
    val keyspace = conf.get("cassandra.checkpointfs.keyspace", "checkpointfs")
    val table = conf.get("cassandra.checkpointfs.table", "file")

    val port = if (name.getPort >= 0) name.getPort else 9042
    val cluster = Cluster.builder.addContactPoints(contactPoints).withPort(port).build
    if (cluster.getMetadata.getKeyspace(keyspace) == null) {
      throw new IllegalStateException(s"Keyspace $keyspace is missing make sure it's created")
    } else if (cluster.getMetadata.getKeyspace(keyspace).getTable(table) == null) {
      throw new IllegalStateException(s"Table $keyspace.$table is missing, please add it: create table $keyspace.$table (path text, name text, is_dir boolean, length bigint, value blob, primary key ((path), name));")
    }
    session = Some(cluster.connect)

    deleteStatement = Some(session.get.prepare(s"delete from ${keyspace}.${table} where path=? and name=?"))
    deleteDirStatement = Some(session.get.prepare(s"delete from ${keyspace}.${table} where path=?"))
    insertStatement = Some(session.get.prepare(s"insert into ${keyspace}.${table} (path, name, is_dir, length, value) values (?,?,?,?,?)"))
    insertDirStatement = Some(session.get.prepare(s"insert into ${keyspace}.${table} (path, name, is_dir) values (?,?,?)"))
    selectStatement = Some(session.get.prepare(s"select * from ${keyspace}.${table} where path=? and name=?"))
    listStatement = Some(session.get.prepare(s"select * from ${keyspace}.${table} where path=? "))
  }

  override def getFileStatus(p: Path): FileStatus = {
    val f = new Path(p.toUri.getPath)
    getFile(f) match {
      case None => null
      case Some(file) => new FileStatus(file.length, file.isDir, replication, 1024, 1024, f)
    }
  }

  override def mkdirs(p: Path, permission: FsPermission) = {
    val f = new Path(p.toUri.getPath)
    val batch = new BatchStatement()
    if (p.isRoot) {
      session.get.execute(insertDirStatement.get.bind(Seq("/", "", true).asInstanceOf[Seq[Object]]: _*))
    } else {
      createParent(f.getParent, f.getName, batch)
    }
    batch.size() match {
      case 0 => false
      case _ => session.get.execute(batch); true
    }
  }

  override def rename(srcP: Path, dstP: Path): Boolean = {
    val src = new Path(srcP.toUri.getPath)
    val dst = new Path(dstP.toUri.getPath)
    getFile(src) match {
      case None => false
      case Some(srcFile) => {
        val delete = deleteStatement.get.bind(srcFile.path, srcFile.name)
        mkdirs(dst.getParent, null)
        val insert = insertStatement.get.bind(Seq(dst.getParent.toString, dst.getName, srcFile.isDir, srcFile.length, srcFile.value).asInstanceOf[Seq[Object]]: _*)
        val batch = new BatchStatement()
        batch.add(delete).add(insert)
        session.get.execute(batch)
        return true
      }
    }
  }


  override def open(p: Path, bufferSize: Int): FSDataInputStream = {
    val f = new Path(p.toUri.getPath)
    val file = getFile(f)
    val bis = new SeekableByteArray(file.get.value.array())
    new FSDataInputStream(bis)
  }

  override def listStatus(p: Path): Array[FileStatus] = {
    val f = new Path(p.toUri.getPath)
    session.get.execute(listStatement.get.bind(f.toString)).all().filter(r => !r.getString("name").isEmpty).map(r => {
      new FileStatus(r.getLong("length"), r.getBool("is_dir"), replication, 0, 0, new Path(r.getString("path"), r.getString("name")))
    }).toArray
  }

  private def createParent(f: Path, child: String, batch: BatchStatement): Unit = {
    if (f != null && getFile(f.toString, child).isEmpty) {
      batch.add(insertDirStatement.get.bind(Seq(f.toString, child, true).asInstanceOf[Seq[Object]]: _*))
      if (!f.isRoot) {
        createParent(f.getParent, f.getName, batch)
      } else if (getFile("/", "").isEmpty) {
        //root folder
        batch.add(insertDirStatement.get.bind(Seq("/", "", true).asInstanceOf[Seq[Object]]: _*))
      }
    }
  }

  /**
    * In memory buffer, write to C* on close.
    */
  class COutputStream(path: Path) extends ByteArrayOutputStream {
    private var closed = false

    override def close(): Unit = {
      if (!closed) {
        closed = true
        val bytes = toByteArray
        val pathAndName = getPathAndName(path)
        val boundedSt = insertStatement.get.bind(Seq(pathAndName._1, pathAndName._2, false, bytes.length.toLong, ByteBuffer.wrap(bytes)).asInstanceOf[Seq[Object]]: _*)
        val batch = new BatchStatement()
        val parent = path.getParent
        //Need to make sure parents exists
        createParent(parent.getParent, parent.getName, batch)
        if (batch.size() == 0) {
          session.get.execute(boundedSt)
        } else {
          batch.add(boundedSt)
          session.get.execute(batch)
        }
        super.close()
      }
    }
  }

  override def create(p: Path, permission: FsPermission, overwrite: Boolean, bufferSize: Int, replication: Short, blockSize: Long, progress: Progressable): FSDataOutputStream = {
    val f = new Path(p.toUri.getPath)
    new FSDataOutputStream(new COutputStream(f), null)
  }

  override def getWorkingDirectory: Path = workingDirectory


  override def setWorkingDirectory(new_dir: Path): Unit = workingDirectory = new_dir

  override def getUri = uri.getOrElse(throw new IllegalStateException("Not initialized"))

  private def recursiveDelete(f: Path, batch: BatchStatement): Unit = {
    session.get.execute(listStatement.get.bind(f.toString)).all().foreach(r => {
      if (r.getBool("is_dir") && !r.getString("name").isEmpty) {
        recursiveDelete(new Path(r.getString("path"), r.getString("name")), batch)
      }
    })
    batch.add(deleteDirStatement.get.bind(f.toString))
  }

  override def delete(p: Path, recursive: Boolean): Boolean = {
    val f = new Path(p.toUri.getPath)
    getFile(f) match {
      case None => false
      case Some(file) => {
        if (file.isDir) {
          val batch = new BatchStatement()
          recursiveDelete(f, batch)
          if (!f.isRoot) {
            batch.add(deleteStatement.get.bind(f.getParent.toString, f.getName))
          }
          session.get.execute(batch)
        } else {
          //Single file just delete it
          session.get.execute(deleteStatement.get.bind(f.getParent.toString, f.getName))
        }
        true
      }
    }
  }

  override def append(p: Path, bufferSize: Int, progress: Progressable): FSDataOutputStream = {
    val f = new Path(p.toUri.getPath)
    getFile(f) match {
      case None => null
      case Some(file) => {
        val cOutputStream = new COutputStream(new Path(file.path, file.name))
        cOutputStream.write(file.value.array())
        new FSDataOutputStream(cOutputStream, null)
      }
    }
  }
}

