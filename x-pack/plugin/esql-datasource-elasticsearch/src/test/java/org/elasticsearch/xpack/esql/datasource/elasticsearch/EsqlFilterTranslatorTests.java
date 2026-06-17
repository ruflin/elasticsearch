/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.elasticsearch;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.spi.FilterPushdownSupport;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;

public class EsqlFilterTranslatorTests extends ESTestCase {

    private static FieldAttribute field(String name, DataType type) {
        return new FieldAttribute(Source.EMPTY, name, new EsField(name, type, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
    }

    private static Literal intLit(int value) {
        return new Literal(Source.EMPTY, value, DataType.INTEGER);
    }

    private static Literal longLit(long value) {
        return new Literal(Source.EMPTY, value, DataType.LONG);
    }

    private static Literal kw(String value) {
        return Literal.keyword(Source.EMPTY, value);
    }

    public void testEqualsOnKeyword() {
        Expression expr = new Equals(Source.EMPTY, field("status", DataType.KEYWORD), kw("active"), null);
        assertEquals(Optional.of("`status` == \"active\""), EsqlFilterTranslator.toWhereClause(List.of(expr)));
    }

    public void testEqualsOnReferenceAttribute() {
        Expression expr = new Equals(Source.EMPTY, referenceAttribute("status", DataType.KEYWORD), kw("active"), null);
        assertEquals(Optional.of("`status` == \"active\""), EsqlFilterTranslator.toWhereClause(List.of(expr)));
    }

    public void testEqualsOnLong() {
        Expression expr = new Equals(Source.EMPTY, field("age", DataType.LONG), longLit(42), null);
        assertEquals(Optional.of("`age` == 42"), EsqlFilterTranslator.toWhereClause(List.of(expr)));
    }

    public void testGreaterThanOnInteger() {
        Expression expr = new GreaterThan(Source.EMPTY, field("count", DataType.INTEGER), intLit(10), null);
        assertEquals(Optional.of("`count` > 10"), EsqlFilterTranslator.toWhereClause(List.of(expr)));
    }

    public void testNotEquals() {
        Expression expr = new NotEquals(Source.EMPTY, field("status", DataType.KEYWORD), kw("done"), null);
        assertEquals(Optional.of("`status` != \"done\""), EsqlFilterTranslator.toWhereClause(List.of(expr)));
    }

    public void testLiteralOnLeftIsFlipped() {
        // 10 < count ==> count > 10
        Expression expr = new org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan(
            Source.EMPTY,
            intLit(10),
            field("count", DataType.INTEGER),
            null
        );
        assertEquals(Optional.of("`count` > 10"), EsqlFilterTranslator.toWhereClause(List.of(expr)));
    }

    public void testAnd() {
        Expression left = new GreaterThan(Source.EMPTY, field("count", DataType.INTEGER), intLit(10), null);
        Expression right = new Equals(Source.EMPTY, field("status", DataType.KEYWORD), kw("active"), null);
        Expression and = new And(Source.EMPTY, left, right);
        assertEquals(Optional.of("(`count` > 10 AND `status` == \"active\")"), EsqlFilterTranslator.toWhereClause(List.of(and)));
    }

    public void testOr() {
        Expression left = new Equals(Source.EMPTY, field("status", DataType.KEYWORD), kw("a"), null);
        Expression right = new Equals(Source.EMPTY, field("status", DataType.KEYWORD), kw("b"), null);
        Expression or = new Or(Source.EMPTY, left, right);
        assertEquals(Optional.of("(`status` == \"a\" OR `status` == \"b\")"), EsqlFilterTranslator.toWhereClause(List.of(or)));
    }

    public void testNot() {
        Expression inner = new Equals(Source.EMPTY, field("status", DataType.KEYWORD), kw("a"), null);
        Expression not = new Not(Source.EMPTY, inner);
        assertEquals(Optional.of("NOT (`status` == \"a\")"), EsqlFilterTranslator.toWhereClause(List.of(not)));
    }

    public void testMultipleFiltersJoinedWithAnd() {
        Expression a = new GreaterThan(Source.EMPTY, field("count", DataType.INTEGER), intLit(10), null);
        Expression b = new LessThanOrEqual(Source.EMPTY, field("count", DataType.INTEGER), intLit(20), null);
        assertEquals(Optional.of("`count` > 10 AND `count` <= 20"), EsqlFilterTranslator.toWhereClause(List.of(a, b)));
    }

    public void testStringWithQuotesIsEscaped() {
        Expression expr = new Equals(Source.EMPTY, field("name", DataType.KEYWORD), kw("a\"b"), null);
        assertEquals(Optional.of("`name` == \"a\\\"b\""), EsqlFilterTranslator.toWhereClause(List.of(expr)));
    }

    public void testStringWithControlCharacterIsNotPushed() {
        Expression expr = new Equals(Source.EMPTY, field("name", DataType.KEYWORD), kw("a\nb"), null);
        assertEquals(Optional.empty(), EsqlFilterTranslator.toWhereClause(List.of(expr)));
        assertEquals(FilterPushdownSupport.Pushability.NO, EsqlFilterTranslator.INSTANCE.canPush(expr));
    }

    public void testFieldNameWithDotsIsQuoted() {
        Expression expr = new Equals(Source.EMPTY, field("user.name", DataType.KEYWORD), kw("bob"), null);
        assertEquals(Optional.of("`user.name` == \"bob\""), EsqlFilterTranslator.toWhereClause(List.of(expr)));
    }

    public void testUnsupportedTypeNotPushed() {
        // IP literals are not rendered, so nothing is pushed.
        Expression expr = new Equals(Source.EMPTY, field("addr", DataType.IP), new Literal(Source.EMPTY, null, DataType.IP), null);
        assertEquals(Optional.empty(), EsqlFilterTranslator.toWhereClause(List.of(expr)));
    }

    public void testCanPushReportsYesForSupported() {
        Expression expr = new Equals(Source.EMPTY, field("status", DataType.KEYWORD), kw("active"), null);
        assertEquals(FilterPushdownSupport.Pushability.YES, EsqlFilterTranslator.INSTANCE.canPush(expr));
    }

    public void testCanPushReportsNoForUnsupported() {
        Expression expr = new Equals(Source.EMPTY, field("a", DataType.INTEGER), field("b", DataType.INTEGER), null);
        assertEquals(FilterPushdownSupport.Pushability.NO, EsqlFilterTranslator.INSTANCE.canPush(expr));
    }

    public void testPushFiltersSplitsPushableAndRemainder() {
        Expression pushable = new GreaterThan(Source.EMPTY, field("count", DataType.INTEGER), intLit(10), null);
        // field == field cannot be rendered, so it stays as remainder
        Expression remainder = new Equals(Source.EMPTY, field("a", DataType.INTEGER), field("b", DataType.INTEGER), null);

        FilterPushdownSupport.PushdownResult result = EsqlFilterTranslator.INSTANCE.pushFilters(List.of(pushable, remainder));
        assertTrue(result.hasPushedFilter());
        assertEquals(List.of(pushable), result.pushedExpressions());
        assertEquals(List.of(remainder), result.remainder());
    }

    public void testPushFiltersNoneWhenNothingPushable() {
        Expression remainder = new Equals(Source.EMPTY, field("a", DataType.INTEGER), field("b", DataType.INTEGER), null);
        FilterPushdownSupport.PushdownResult result = EsqlFilterTranslator.INSTANCE.pushFilters(List.of(remainder));
        assertFalse(result.hasPushedFilter());
        assertEquals(List.of(remainder), result.remainder());
    }

    // ---------------------------------------------------------------------------------------------------------
    // Gaps relevant to the Kibana KI / SigEvents query shapes (Kibana streams plugin, sig_events). These assert
    // current (v1) behaviour so the gaps are documented; closing a gap means flipping the corresponding assertion.
    // ---------------------------------------------------------------------------------------------------------

    /**
     * A foldable time bound ({@code @timestamp >= TO_DATETIME("...")}) is pushed (see {@code testDatetimeBoundIsPushed}),
     * but a bound against a <em>non-foldable</em> value side (another field / a runtime expression) still cannot be
     * rendered as a constant and stays in the local FilterExec. The remote returns its full (implicitly capped) page
     * and the predicate is applied locally — a correctness/cost gap only for non-constant value sides.
     */
    public void testTimeRangeAgainstNonFoldableValueIsNotPushed() {
        // The value side is a non-foldable field reference, which the translator cannot render as a constant.
        Expression rhs = field("computed_ts", DataType.DATETIME);
        Expression expr = new GreaterThan(Source.EMPTY, field("@timestamp", DataType.DATETIME), rhs, null);
        assertEquals(Optional.empty(), EsqlFilterTranslator.toWhereClause(List.of(expr)));
        assertEquals(FilterPushdownSupport.Pushability.NO, EsqlFilterTranslator.INSTANCE.canPush(expr));
    }

    /**
     * A datetime bound is pushed by re-rendering the folded epoch-millis value as {@code TO_DATETIME(<millis>)} so
     * the remote compares a datetime to a datetime. {@code TO_DATETIME("...")} folds to a DATETIME literal before
     * pushdown, so a DATETIME literal here faithfully represents the SigEvents/KI time bound after constant folding.
     */
    public void testDatetimeBoundIsPushed() {
        Literal datetimeLit = new Literal(Source.EMPTY, 1_700_000_000_000L, DataType.DATETIME);
        Expression expr = new LessThanOrEqual(Source.EMPTY, field("@timestamp", DataType.DATETIME), datetimeLit, null);
        assertEquals(Optional.of("`@timestamp` <= TO_DATETIME(1700000000000)"), EsqlFilterTranslator.toWhereClause(List.of(expr)));
        assertEquals(FilterPushdownSupport.Pushability.YES, EsqlFilterTranslator.INSTANCE.canPush(expr));
    }

    /**
     * A datetime bound with the value on the left ({@code <datetime literal> >= @timestamp}) is normalised to
     * {@code @timestamp <= TO_DATETIME(...)}, exercising both the foldable-value-side and operator-flip paths.
     */
    public void testDatetimeBoundFlippedIsPushed() {
        Literal datetimeLit = new Literal(Source.EMPTY, 1_700_000_000_000L, DataType.DATETIME);
        Expression expr = new GreaterThan(Source.EMPTY, datetimeLit, field("@timestamp", DataType.DATETIME), null);
        assertEquals(Optional.of("`@timestamp` < TO_DATETIME(1700000000000)"), EsqlFilterTranslator.toWhereClause(List.of(expr)));
    }

    /**
     * The KI dedup / lookup path filters with {@code <field> IN (...)} (Kibana {@code latest_source_query.ts#inFilter},
     * e.g. {@code _id IN (...)}). The list of foldable values is rendered into a remote {@code IN} clause.
     */
    public void testInListIsPushed() {
        Expression in = new org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In(
            Source.EMPTY,
            field("status", DataType.KEYWORD),
            List.of(kw("active"), kw("error"))
        );
        assertEquals(Optional.of("`status` IN (\"active\", \"error\")"), EsqlFilterTranslator.toWhereClause(List.of(in)));
        assertEquals(FilterPushdownSupport.Pushability.YES, EsqlFilterTranslator.INSTANCE.canPush(in));
    }

    /** A numeric IN list is rendered with bare numbers. */
    public void testNumericInListIsPushed() {
        Expression in = new org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In(
            Source.EMPTY,
            field("status_code", DataType.INTEGER),
            List.of(intLit(404), intLit(500))
        );
        assertEquals(Optional.of("`status_code` IN (404, 500)"), EsqlFilterTranslator.toWhereClause(List.of(in)));
    }

    /** An IN list with a non-foldable / unrenderable element is not pushed; it stays in the local FilterExec. */
    public void testInListWithFieldElementIsNotPushed() {
        Expression in = new org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In(
            Source.EMPTY,
            field("status", DataType.KEYWORD),
            List.of(kw("active"), field("other", DataType.KEYWORD))
        );
        assertEquals(Optional.empty(), EsqlFilterTranslator.toWhereClause(List.of(in)));
        assertEquals(FilterPushdownSupport.Pushability.NO, EsqlFilterTranslator.INSTANCE.canPush(in));
    }
}
