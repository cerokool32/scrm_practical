package scrm.practical.ex1

import org.scalatest.flatspec._
import org.scalatest.matchers._

class FindPairsTest extends AnyFlatSpec with should.Matchers {

  "findPairs" should "retrieve 2 pair of values from a non-empty list" in {
    // Given
    val elements = Seq(1,2,4,5)
    val targetValue = 7

    // When
    val result = FindPairs.findPairs(elements, targetValue)

    // Then
    result.length should be (1)
    result.contains(5->2) should be (true)
  }

  it should "retrieve 2 pairs of values from a non-empty list" in {
    // Given
    val elements = Seq(1,2,3,4,5)
    val targetValue = 7

    // When
    val result = FindPairs.findPairs(elements, targetValue)

    // Then
    result.length should be (2)
    result.contains(5->2) should be (true)
    result.contains(4->3) should be (true)
  }

  it should "retrieve no pairs from a non-empty list" in {
    // Given
    val elements = Seq(1,2,3,4,5)
    val targetValue = 100

    // When
    val result = FindPairs.findPairs(elements, targetValue)

    // Then
    result.length should be (0)
  }

  it should "retrieve no pairs from an empty list" in {
    // Given
    val elements = Seq().empty
    val targetValue = 100

    // When
    val result = FindPairs.findPairs(elements, targetValue)

    // Then
    result.length should be (0)
  }

  "findPairsOnLargeFile" should "retrieve 1 pair of values from a non-empty file" in {
    // Given
    val file = "pairs-1.txt"
    val targetValue = 7

    // When
    val result = FindPairs.findPairsOnLargeFile(file, targetValue)

    // Then
    result.length should be (1)
    result.contains(5->2) should be (true)
  }

  it should "retrieve 2 pairs of values from a non-empty file" in {
    // Given
    val file = "pairs-2.txt"
    val targetValue = 7

    // When
    val result = FindPairs.findPairsOnLargeFile(file, targetValue)

    // Then
    result.length should be (2)
    result.contains(5->2) should be (true)
    result.contains(4->3) should be (true)
  }

  it should "retrieve no pairs of values from a non-empty file" in {
    // Given
    val file = "pairs-2.txt"
    val targetValue = 100

    // when
    val result = FindPairs.findPairsOnLargeFile(file, targetValue)

    // Then
    result.length should be (0)
  }

  it should "retrieve no pairs from an empty file" in {
    // Given
    val file = "pairs-3.txt"
    val targetValue = 100

    // When
    val result = FindPairs.findPairsOnLargeFile(file, targetValue)

    // Then
    result.length should be (0)
  }
}
