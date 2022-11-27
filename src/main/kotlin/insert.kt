import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.document.*
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import java.lang.Thread.sleep
import kotlin.random.Random.Default.nextInt



fun main() {
    val client = AmazonDynamoDBClientBuilder.defaultClient()
    val dynamoDb = DynamoDB(client)
    val table: Table = dynamoDb.getTable("grigory-test")

    //table.initData(9651 .. 20000)
    table.queryData(2)
}

fun Table.queryData(shard: Int){
    var prevKey: Map<String, AttributeValue>? = null
    var j = 0
    val nRead = 400

    do {

        val ka = KeyAttribute("PK", "$shard#data")
        val rkc = RangeKeyCondition("SK")
            .beginsWith("sk#166")

        val qs = QuerySpec()
            .withHashKey(ka)
            .withRangeKeyCondition(rkc)
            .withMaxPageSize(78)
            .withMaxResultSize(nRead)
            .withExclusiveStartKey(key(prevKey))

        val qr = query(qs)

        var pk: String = ""
        var sk: String = ""
        var count: Int = 0

        qr.asSequence()
            .take(500)
            .forEachIndexed {i, item ->
                count = i
                j++
                pk = item.getString("PK")
                sk = item.getString("SK")
                val data = item.getInt("data")
                println("$j: {pk:'$pk', sk:'$sk', data:$data}")
            }

        println("---------------- ---------------- ---------------- ---------------- ---------------- ----------------")
        prevKey = if (count == (nRead-1)) {
            mapOf("PK" to AttributeValue().withS(pk), "SK" to AttributeValue().withS(sk))
        } else {
            null
        }

    } while (prevKey != null)
}

fun Table.initData(r: IntRange) {
    r.forEach { i ->
        val pk = "${nextInt(20)}#data"
        val sk = "sk#${System.currentTimeMillis()}$i"
        this.insertRec(pk, sk, i)
        //if (i % 100 == 0) sleep(250L)
    }
}

fun Table.insertRec(pk: String, sk: String, data: Int): PutItemOutcome {
    val item = Item()
        .withPrimaryKey( key(pk, sk) )
        .withInt("data", data)
    println("{pk:'$pk', sk:'$sk', data:$data}")
    return this.putItem(item)
}

fun key(partitionKey: String, sortKey: String): PrimaryKey {
    return PrimaryKey().apply {
        addComponent("PK", partitionKey)
        addComponent("SK", sortKey)
    }

}

fun key(pk: Map<String, AttributeValue>?): PrimaryKey? {
    if (pk == null) return null
    return key(
        partitionKey = pk["PK"]!!.getS(),
        sortKey      = pk["SK"]!!.getS()
    )
}
//data class Foo (val pk: String, val sk: String, val data: Int)