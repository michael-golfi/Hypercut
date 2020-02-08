package hypercut

import com.google.common.hash.BloomFilter

package object bucket {
  type BloomBucket = BloomFilter[CharSequence]
}
