import org.jooq.*
import org.jooq.impl.DSL.*
import org.jooq.impl.DefaultDSLContext
import java.math.BigInteger.ZERO
import java.time.LocalDate
import java.time.temporal.ChronoUnit
import java.util.stream.Collectors
import java.util.stream.Stream

fun main(args: Array<String>) {
    save(
            DefaultDSLContext(SQLDialect.DEFAULT),
            LocalDate.now().minusDays(10),
            LocalDate.now(),
            "id"
    )
}

fun save(dsl: DSLContext, startDate: LocalDate, endDate:LocalDate, Id: String) {
    val selectCommonPart = coalesce(sum(field(name("amount"), Long::class.java)), ZERO)
            .`as`(field(name("totalAmount")))
    var whereCommonPart: Condition = trueCondition().and(field(name("Id")).eq(Id)) // Id comes as a parameter

    var query = dsl.selectQuery()
    query.addSelect(selectCommonPart)
    query.addFrom(table("${tableName(startDate)}")) // `startDate` is a `LocalDate`, `tableName()` generates the table name as String
    query.addConditions(whereCommonPart)

    // `endDate` is also a `LocalDate`, can be either equal to `startDate` or after
    if (endDate.isAfter(startDate)) {
        for (date in Stream.iterate(startDate.plusDays(1), { d: LocalDate -> d.plusDays(1) })
                .limit(ChronoUnit.DAYS.between(startDate, endDate))
                .collect(Collectors.toList())) {

            // This gives an inference error
            query.union(buildSelect(dsl, selectCommonPart, whereCommonPart, date))
        }
    }
}

fun tableName(date: LocalDate): String = date.toString()

private fun buildSelect(dsl: DSLContext, selectCommonPart: Field<*>, whereCommonPart: Condition, date: LocalDate):
        Select<*> {
    var query = dsl.selectQuery()
    query.addSelect(selectCommonPart)
    query.addFrom(table("public.${tableName(date)}"))
    query.addConditions(whereCommonPart)
    return query
}