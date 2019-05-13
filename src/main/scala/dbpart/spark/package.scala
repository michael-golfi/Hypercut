package dbpart

import org.apache.spark.graphx.Graph
import dbpart.graph.PathNode
import dbpart.bucket._

package object spark {
  type BucketGraph = Graph[SimpleCountingBucket, Int]
  type PathGraph = Graph[PathNode, Int]
}