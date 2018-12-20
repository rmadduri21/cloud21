
package io.speednscale.snsdp.infrastructure

import org.junit.runner.RunWith
import org.mockito.Mockito.when
import org.scalatest.BeforeAndAfter
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest._
import org.scalatest.Matchers._
import org.scalatest.mock.MockitoSugar

import com.google.common.base.Optional
import scala.language.reflectiveCalls

import java.util.Collection;

import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HConnection;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;


@RunWith(classOf[JUnitRunner])
class HBaseConnectionTest extends FunSuite with MockitoSugar with Matchers with BeforeAndAfter {

  // common setup logic can be handled here; trait BeforeAndAfter
  // needs to be mixed-in to use. this is synonymous to @Before
  // in the java world
  before {

  }

  // common teardown/cleanup logic can be handled here; trait BeforeAndAfter
  // needs to be mixed-in to use. this is synonymous to @After
  // in the java world
  after {

  }

  /** Common setup logic can be defined here. this approach is desired
    * above using before/after if the setup logic doesn't do anything statically,
    * or defines a state that shoud be shared amongst tests (such as starting up a mini-cluster).
    * Each test will use its own fixture. This will easily allow the tests to be ran
    * in parallel.
    *
    * Once the fixture is defined, it can be instantiated and used in the test
    * methods just like any other object.
    *
    * {{{
    * def fixture = new {
    * val sample = 4
    * }
    *
    * test("an example") {
    * val fix = fixture
    * math.sqrt(fix.sample) should equal(2)
    * }
    * }}}
    */
  def fixture(ipAddress: String, port: String) = new {
    //Connect to HBase 
    val hBaseUtil = new HBaseUtil(ipAddress, port)
    val hBaseConnection = hBaseUtil.getConnection()
    val hBaseAdmin = hBaseUtil.getHBaseAdmin(hBaseConnection)
  }

  test("should return true") {
    val fix = fixture("zookeeper-01.c.platform-production.internal,zookeeper-02.c.platform-production.internal,zookeeper-03.c.platform-production.internal", "2181")
    //val fix = fixture("zookeeper-01.us-central1-a.platform-production,zookeeper-02.us-central1-a.platform-production,zookeeper-03.us-central1-a.platform-production", "2181")
    fix.hBaseUtil should not equal(null)
    fix.hBaseConnection should not equal(null)
    fix.hBaseAdmin should not equal(null)
        
    println("HBase coonection test passed...\n\n")
    println("Tables exists tests begin...")
    
    // Verify that telemetry tables exist
    //var exists  = fix.hBaseAdmin.tableExists("telemetry_v1:game_telemetry")
    fix.hBaseAdmin.tableExists("telemetry1:game_telemetry") shouldBe true
    println("Table: telemetry1:game_telemetry exists - " + true)
    
    fix.hBaseAdmin.tableExists("telemetry1:adjust_postbacks") shouldBe true
    println("Table: telemetry1:adjust_postbacks exists - " + true)
    
    fix.hBaseUtil.closeConnection(fix.hBaseConnection)
  }
}