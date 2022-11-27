import java.util.Comparator
import java.util.PriorityQueue


class MinIterator<T>(
    numberOfSubIterators: Int,
    private val comparator: Comparator<T>,
    subIteratorCreator: (Int) -> Iterator<T>
) : Iterator<T> {

    private val subIterators = Array(numberOfSubIterators, subIteratorCreator)
    private val minHeap = PriorityQueue<Pair<Int, T>>(numberOfSubIterators){
            o1, o2 -> comparator.compare(o1.second, o2.second)
    }

    init {
        repeat(numberOfSubIterators) { i -> consumeNextElementFromSubIterator(i) }
    }

    private fun consumeNextElementFromSubIterator(subIteratorIndex: Int) {
        val subIterator = subIterators[subIteratorIndex]
        if (subIterator.hasNext()) minHeap.add(subIteratorIndex to subIterator.next())
    }

    override fun hasNext(): Boolean = minHeap.isNotEmpty()

    override fun next(): T {
        val (subIteratorIndex, minElement) = minHeap.remove()
        consumeNextElementFromSubIterator(subIteratorIndex)
        return minElement
    }

}


