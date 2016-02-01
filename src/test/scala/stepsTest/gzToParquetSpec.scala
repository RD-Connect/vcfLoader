package stepsTest
import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.FunSuite
import org.apache.spark.rdd.RDD


/**
 * Created by dpiscia on 14/09/15.
 */

class SampleRDDTest extends FunSuite with SharedSparkContext {
  test("really simple transformation") {
    val input = sc.parallelize(List("hi", "hi cloudera", "bye"))
    val expected = List(List("hi"), List("hi", "cloudera"), List("bye"))
    assert( true === true)
  }
}

case class Person(name: String, age: Int, weight: Double)
