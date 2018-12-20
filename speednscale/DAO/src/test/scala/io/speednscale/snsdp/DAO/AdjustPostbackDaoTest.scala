package io.speednscale.snsdp.DAO

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

import org.apache.hadoop.hbase.client.HConnectionManager
import org.apache.hadoop.hbase.client.HConnection

import com.google.common.base.Optional
import com.google.inject.Inject
import com.google.inject.assistedinject.Assisted

import io.speednscale.snsdp.infrastructure.HBaseUtil
import io.speednscale.snsdp.snsdp_record_models.transformed.AdjustPostback
import io.speednscale.snsdp.transformations.AdjustPostbackTransformer

@RunWith(classOf[JUnitRunner])
class AdjustPostbackDaoTest extends FunSuite with MockitoSugar with Matchers with BeforeAndAfter {

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
		val adjustPostbackDao = new AdjustPostbackDao(hBaseConnection)
	}

	test("should return true") {
		//val fix = fixture("zookeeper-01.c.platform-production.internal,zookeeper-02.c.platform-production.internal,zookeeper-03.c.platform-production.internal", "2181")
		val fix = fixture("zookeeper-01.c.platform-production.internal,zookeeper-02.c.platform-production.internal,zookeeper-03.c.platform-production.internal", "2181")
				fix.hBaseUtil should not equal(null)
				fix.hBaseConnection should not equal(null)
				fix.adjustPostbackDao should not equal(null)		
		
		//println("adjustPostbackDao created")
		
		val rawAdjustPostback = DaoTestUtil.buildRawAdjustPostback("currency_ledger", "popdash")
		rawAdjustPostback should not equal(null)
		//println("rawAdjustedPostbackObject created")
		
						
    //Print the retrieved object for debugging
		//for (i <- 0 until 63) {
    //    if (i != 60)
    //        println("Raw AdjustPostback attribute: " + i + " is " + rawAdjustPostback.get(i))
    //}

		val stdAdjustPostback = AdjustPostbackTransformer.doTransformation(AdjustPostbackDao.AP_DATA_FIELD_COUNT, 
    				AdjustPostbackDao.AP_MP_SERVER_METADATA_FIELD_COUNT, rawAdjustPostback)
		stdAdjustPostback should not equal(null)
		//println("Obtained standard adjustPostbackStd created from rawAdjustedPostbackObject")
								
    //Print the retrieved object for debugging
		//for (i <- 0 until 63) {
    //    if (i != 60)
    //        println("Standard/Transformed AdjustPostback attribute: " + i + " is " + stdAdjustPostback.get(i))
    //}

		fix.adjustPostbackDao.addAdjustPostback(stdAdjustPostback)
		//println("Added standard adjustPostbackStd object to HBase")

		val retrievedAdjustPostback = fix.adjustPostbackDao.getAdjustPostback(fix.adjustPostbackDao.getKey(stdAdjustPostback))

		fix.adjustPostbackDao.compareAdjustPostbacks(stdAdjustPostback, retrievedAdjustPostback) should be (true)
		fix.adjustPostbackDao.deleteAdjustPostback(fix.adjustPostbackDao.getKey(stdAdjustPostback))
		
		fix.adjustPostbackDao.getAdjustPostback(fix.adjustPostbackDao.getKey(stdAdjustPostback)) shouldBe null
		fix.hBaseUtil.closeConnection(fix.hBaseConnection)
	}
}