package dbpart

import org.apache.spark.graphx._
import dbpart.graph.PathNode
import dbpart.bucket._

package object spark {
  type BucketGraph = Graph[SimpleCountingBucket, Int]
  type PathGraph = Graph[PathNode, Int]

  type PathMsg = List[(VertexId, Int)]
}