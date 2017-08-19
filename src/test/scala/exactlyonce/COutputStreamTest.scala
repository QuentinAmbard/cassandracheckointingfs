package exactlyonce

import java.io._
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.scalatest.Matchers._
import org.scalatest.{FlatSpec, _}

import scala.collection.JavaConversions._
import scala.util.Random


class CassandraSimpleFileSystemTest extends FlatSpec {

  val csfs = new CassandraSimpleFileSystem
  csfs.initialize(new URI("checkpointingfs://127.0.0.1/"), new Configuration())

  "CassandraSimpleFileSystem" should "write and read file" in {
    val tmpFile = "/tmp/test.txt"
    val txt = createRandomFile(tmpFile)
    csfs.copyFromLocalFile(new Path(tmpFile), new Path(tmpFile))
    csfs.copyToLocalFile(new Path(tmpFile), new Path(s"$tmpFile.check"))
    scala.io.Source.fromFile(s"$tmpFile.check").mkString eq txt
  }


  "CassandraSimpleFileSystem" should "write and read root file" in {
    val tmpFile = "/tmp/test.txt"
    val txt = createRandomFile(tmpFile)
    csfs.copyFromLocalFile(new Path(tmpFile), new Path("/root.txt"))
    csfs.copyToLocalFile(new Path("/root.txt"), new Path(s"$tmpFile.check"))
    scala.io.Source.fromFile(s"$tmpFile.check").mkString eq txt
  }

  "CassandraSimpleFileSystem" should "write and read long file" in {
    val tmpFile = "/tmp/test.txt"
    val txt = createRandomFile(tmpFile)
    csfs.copyFromLocalFile(new Path(tmpFile), new Path("/tmp/toto/tata/tutu/tutu/root.txt"))
    csfs.copyToLocalFile(new Path("/tmp/toto/tata/tutu/tutu/root.txt"), new Path(s"$tmpFile.check"))
    scala.io.Source.fromFile(s"$tmpFile.check").mkString eq txt
  }

  "CassandraSimpleFileSystem" should "should delete everything" in {
    val tmpFile = "/tmp/test.txt"
    val txt = createRandomFile(tmpFile)
    csfs.copyFromLocalFile(new Path(tmpFile), new Path("/tmp/a/b/c/d1/root1.txt"))
    csfs.copyFromLocalFile(new Path(tmpFile), new Path("/tmp/a/b/c/d1/root2.txt"))
    csfs.copyFromLocalFile(new Path(tmpFile), new Path("/tmp/a/b/c/d2/root.txt"))
    csfs.copyFromLocalFile(new Path(tmpFile), new Path("/tmp/a/b/c/d3/root.txt"))
    csfs.delete(new Path("/"), true)
    csfs.session.get.execute(s"select * from checkpointfs.file").all().isEmpty() shouldBe true
  }


  "CassandraSimpleFileSystem" should "should delete a single file" in {
    csfs.session.get.execute(s"truncate table checkpointfs.file")
    val tmpFile = "/tmp/test.txt"
    val txt = createRandomFile(tmpFile)
    csfs.copyFromLocalFile(new Path(tmpFile), new Path("/tmp/a/root1.txt"))
    csfs.copyFromLocalFile(new Path(tmpFile), new Path("/tmp/a/root2.txt"))
    csfs.delete(new Path("/tmp/a/root1.txt"), true)
    checkRowExist(Seq("/", "/tmp", "/tmp/a", "/tmp/a/root2.txt"))
  }

  "CassandraSimpleFileSystem" should "should delete a single root file" in {
    csfs.session.get.execute(s"truncate table checkpointfs.file")
    val tmpFile = "/tmp/test.txt"
    val txt = createRandomFile(tmpFile)
    csfs.copyFromLocalFile(new Path(tmpFile), new Path("/root1.txt"))
    csfs.copyFromLocalFile(new Path(tmpFile), new Path("/root2.txt"))
    csfs.copyFromLocalFile(new Path(tmpFile), new Path("/a/text"))
    csfs.delete(new Path("/root1.txt"), true)
    checkRowExist(Seq("/", "/root2.txt", "/a", "/a/text"))
  }


  "CassandraSimpleFileSystem" should "should make dir" in {
    csfs.session.get.execute(s"truncate table checkpointfs.file")
    val tmpFile = "/tmp/test.txt"
    val txt = createRandomFile(tmpFile)
    csfs.mkdirs(new Path("/tmp"))
    checkRowExist(Seq("/", "/tmp"))
    csfs.session.get.execute(s"truncate table checkpointfs.file")
    csfs.mkdirs(new Path("/"))
    checkRowExist(Seq("/"))
    csfs.session.get.execute(s"truncate table checkpointfs.file")
    csfs.mkdirs(new Path("/aa/bb/cc/dd/e"), null)
    checkRowExist(Seq("/", "/aa", "/aa/bb", "/aa/bb/cc", "/aa/bb/cc/dd", "/aa/bb/cc/dd/e"))
  }


  "CassandraSimpleFileSystem" should "should rename dir" in {
    csfs.session.get.execute(s"truncate table checkpointfs.file")
    val tmpFile = "/tmp/test.txt"
    val txt = createRandomFile(tmpFile)
    csfs.copyFromLocalFile(new Path(tmpFile), new Path("/aa/b/root1.txt"))
    csfs.rename(new Path("/aa/b/root1.txt"), new Path("/aa/b/2"))
    checkRowExist(Seq("/", "/aa", "/aa/b", "/aa/b/2"))
    csfs.copyToLocalFile(new Path("/aa/b/2"), new Path(s"$tmpFile.check"))
    scala.io.Source.fromFile(s"$tmpFile.check").mkString eq txt
    csfs.rename(new Path("/aa/b/2"), new Path("/cc/3.txt"))
    checkRowExist(Seq("/", "/aa", "/aa/b", "/cc", "/cc/3.txt"))
    csfs.copyToLocalFile(new Path("/cc/3.txt"), new Path(s"$tmpFile.check"))
    scala.io.Source.fromFile(s"$tmpFile.check").mkString eq txt
  }


  "CassandraSimpleFileSystem" should "should list all folder" in {
    csfs.session.get.execute(s"truncate table checkpointfs.file")
    val tmpFile = "/tmp/test.txt"
    val txt = createRandomFile(tmpFile)
    csfs.copyFromLocalFile(new Path(tmpFile), new Path("/tmp/d1/root1.txt"))
    csfs.copyFromLocalFile(new Path(tmpFile), new Path("/tmp/d1/root2.txt"))
    csfs.copyFromLocalFile(new Path(tmpFile), new Path("/tmp/d1/ee/root2.txt"))
    csfs.copyFromLocalFile(new Path(tmpFile), new Path("/tmp/d2/root.txt"))
    csfs.listStatus(new Path("/tmp/d1")).toSeq shouldBe(Seq("/tmp/d1/ee", "/tmp/d1/root1.txt", "/tmp/d1/root2.txt").map( p =>new FileStatus(1,true,1,1,1,new Path(p))))
    csfs.listStatus(new Path("/")).toSeq shouldBe(Seq("/tmp").map( p =>new FileStatus(1,true,1,1,1,new Path(p))))
  }

  "CassandraSimpleFileSystem" should "should delete all folder" in {
    csfs.session.get.execute(s"truncate table checkpointfs.file")
    val tmpFile = "/tmp/test.txt"
    val txt = createRandomFile(tmpFile)
    csfs.copyFromLocalFile(new Path(tmpFile), new Path("/tmp/a/b/c/d1/root1.txt"))
    csfs.copyFromLocalFile(new Path(tmpFile), new Path("/tmp/a/b/c/d1/root2.txt"))
    csfs.copyFromLocalFile(new Path(tmpFile), new Path("/tmp/a/b/c/d2/root.txt"))
    csfs.copyFromLocalFile(new Path(tmpFile), new Path("/tmp/a/b/c/d3/root.txt"))
    csfs.delete(new Path("/tmp/a/b/c/"), true)
    checkRowExist(Seq("/", "/tmp", "/tmp/a", "/tmp/a/b"))
  }

  private def checkRowExist(p: Seq[String]) = {
    val paths = p.map(new Path(_))
    val rows = csfs.session.get.execute(s"select * from checkpointfs.file").all()
    rows.size() shouldBe paths.size
    rows.foreach(r => {
      val path = if (r.getString("name").isEmpty) new Path(r.getString("path")) else new Path(r.getString("path"), r.getString("name"))
      paths.contains(path) shouldBe true
    })
  }

  private def createRandomFile(tmpFile: String) = {
    val file = new File(tmpFile)
    file.deleteOnExit()
    val bw = new BufferedWriter(new FileWriter(file))
    val txt = Random.alphanumeric take 100 mkString ("")
    bw.write(txt)
    bw.close()
    txt
  }
}
