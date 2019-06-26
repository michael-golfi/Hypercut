package dbpart.bucket

import dbpart.Testing
import org.scalatest.Matchers
import org.scalatest.FunSuite

class PathMergingBucketTest extends FunSuite with Matchers {
  import Testing._
  
  test("basic") {
    //Construct PathMergingBucket
    //Check that in- and out-openings are correct
    val k = 5

    val ss = Array("ACTGGG", "TTGTTA")
    var post = Array("CCGAT", "GTTAA")
    var prior = Array[String]()
    var bucket = PathMergingBucket(ss, k).withPost(post).
      withPrior(prior)
    bucket.outOpenings should equal (Seq("GTTA"))
    bucket.inOpenings should equal (Seq())
    
    prior = Array("CCGAG", "AACTGAA")
    bucket = PathMergingBucket(ss, k).withPost(post).
      withPrior(prior)
    bucket.inOpenings should equal (Seq("ACTG"))
    
    
//    val post = Array("CCGAT", "GTTAA")
  }
  
  test("containedUnitigs") {
    // Check that unitigs entirely contained in buckets (not touching boundary)
    // are identified
  }
  
  
  test("seizeUnitigs") {
    //Check that unitigs contained entirely in the bucket
    //can be obtained, and that they do not remain in the bucket
    //afterward
  }  
}