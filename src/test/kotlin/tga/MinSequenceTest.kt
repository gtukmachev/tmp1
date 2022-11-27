package tga

import MinIterator
import kotlin.test.Test

class MinSequenceTest {

    @Test
    fun t1() {

        val iterators = arrayOf(
            arrayOf(1,3,5).iterator(),
            arrayOf(2,8,10,15,16).iterator(),
            emptyArray<Int>().iterator(),
            arrayOf(7,8,12,13,15,17,18,19,110).iterator(),
            arrayOf(0).iterator()
        )

        val minIterator = MinIterator(iterators.size, {i, j -> i-j}, { iterators[it] })

        val list = minIterator
            .asSequence()
            .take(10)
            .toList()

        println("[${list.joinToString()}]")
    }

}