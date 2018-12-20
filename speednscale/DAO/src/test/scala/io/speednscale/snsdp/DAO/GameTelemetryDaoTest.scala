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
import io.speednscale.snsdp.snsdp_record_models.transformed.GameTelemetry
import io.speednscale.snsdp.transformations.GameTelemetryTransformer

@RunWith(classOf[JUnitRunner])
class GameTelemetryDaoTest extends FunSuite with MockitoSugar with Matchers with BeforeAndAfter {

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
	}

	test("should return true") {
		val fix = fixture("zookeeper-01.c.platform-production.internal,zookeeper-02.c.platform-production.internal,zookeeper-03.c.platform-production.internal", "2181")
				fix.hBaseUtil should not equal(null)
				fix.hBaseConnection should not equal(null)
		
		//println("Obtained hBaseConnection")

		val gameTelDao = new GameTelemetryDao(fix.hBaseConnection)
		//println("gameTelDao created")
		
		val rawGameTelObject = DaoTestUtil.buildRawGameTelemetry("currency_ledger", "Popdash")
		rawGameTelObject should not equal(null)
		//println("rawGameTelObject created")

		val gameTelStd = GameTelemetryTransformer.doTransformation(GameTelemetryDao.DATA_FIELD_COUNT,
    	    		GameTelemetryDao.MP_SERVER_METADATA_FIELD_COUNT, rawGameTelObject)
		gameTelStd should not equal(null)
		//println("Obtained standard gameTelStd created from rawGameTelObject")

		gameTelDao.addGameTelemetry(gameTelStd)
		//println("Added standard gameTelStd object to HBase")
		
		val gameTelKey = gameTelDao.getKey(gameTelStd)

		val retrievedGameTel = gameTelDao.getGameTelemetry(gameTelKey)

		//gameTelDao.compareGameTels(retrievedGameTel, gameTelStd) should be (true)
		
		gameTelDao.deleteGameTelemetry(gameTelKey)
		
		val gameTelAfterDelete = gameTelDao.getGameTelemetry(gameTelDao.getKey(gameTelStd))
		
		gameTelAfterDelete should be (null)
		
		gameTelDao.existsGameTelemetry(gameTelKey) shouldBe false
		
		fix.hBaseUtil.closeConnection(fix.hBaseConnection)
	}
}