/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.elasticsearch;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.FilterPushdownSupport;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Translates ESQL filter {@link Expression}s into a remote ESQL {@code WHERE} clause.
 * <p>
 * Because the remote source is itself an Elasticsearch cluster that speaks ESQL, pushdown is a
 * direct re-rendering of the expression rather than a translation into a foreign query DSL. Only a
 * conservative subset is pushed; anything that cannot be rendered with guaranteed-identical
 * semantics is left in the local {@code FilterExec} (reported as {@link Pushability#NO}), so results
 * stay correct even when nothing is pushed.
 * <p>
 * Supported: comparisons ({@code == != < <= > >=}) between a field and a foldable string / number /
 * boolean literal (either operand order), combined with {@code AND}, {@code OR} and {@code NOT}.
 */
final class EsqlFilterTranslator implements FilterPushdownSupport {

    static final EsqlFilterTranslator INSTANCE = new EsqlFilterTranslator();

    private EsqlFilterTranslator() {}

    @Override
    public PushdownResult pushFilters(List<Expression> filters) {
        List<Expression> pushable = new ArrayList<>();
        List<Expression> remainder = new ArrayList<>();
        for (Expression filter : filters) {
            if (render(filter).isPresent()) {
                pushable.add(filter);
            } else {
                remainder.add(filter);
            }
        }
        if (pushable.isEmpty()) {
            return PushdownResult.none(filters);
        }
        // The pushed filter object is opaque to core; the connector consumes the pushedExpressions instead.
        // A non-null marker is required so PushFiltersToSource recognises that a filter was pushed.
        return new PushdownResult("elasticsearch-where", List.copyOf(pushable), List.copyOf(remainder));
    }

    @Override
    public Pushability canPush(Expression expr) {
        return render(expr).isPresent() ? Pushability.YES : Pushability.NO;
    }

    /**
     * Renders the AND of all pushed filters into a single remote {@code WHERE} clause body, or empty
     * when none of them can be rendered. Used by the connector when building the remote query.
     */
    static Optional<String> toWhereClause(List<Expression> filters) {
        if (filters == null || filters.isEmpty()) {
            return Optional.empty();
        }
        List<String> rendered = new ArrayList<>(filters.size());
        for (Expression filter : filters) {
            Optional<String> r = render(filter);
            if (r.isEmpty()) {
                return Optional.empty();
            }
            rendered.add(r.get());
        }
        return Optional.of(String.join(" AND ", rendered));
    }

    private static Optional<String> render(Expression expr) {
        if (expr instanceof And and) {
            return renderBinaryLogical(and.left(), and.right(), "AND");
        }
        if (expr instanceof Or or) {
            return renderBinaryLogical(or.left(), or.right(), "OR");
        }
        if (expr instanceof Not not) {
            return render(not.field()).map(inner -> "NOT (" + inner + ")");
        }
        if (expr instanceof EsqlBinaryComparison cmp) {
            return renderComparison(cmp);
        }
        return Optional.empty();
    }

    private static Optional<String> renderBinaryLogical(Expression left, Expression right, String op) {
        Optional<String> l = render(left);
        Optional<String> r = render(right);
        if (l.isEmpty() || r.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of("(" + l.get() + " " + op + " " + r.get() + ")");
    }

    private static Optional<String> renderComparison(EsqlBinaryComparison cmp) {
        String symbol = comparisonSymbol(cmp);
        if (symbol == null) {
            return Optional.empty();
        }
        // Accept field <op> literal or literal <op> field, normalising to field <op> literal.
        Attribute field;
        Literal literal;
        boolean flipped;
        Attribute left = Expressions.attribute(cmp.left());
        Attribute right = Expressions.attribute(cmp.right());
        if (left != null && cmp.right() instanceof Literal lit) {
            field = left;
            literal = lit;
            flipped = false;
        } else if (cmp.left() instanceof Literal lit && right != null) {
            field = right;
            literal = lit;
            flipped = true;
        } else {
            return Optional.empty();
        }

        Optional<String> value = renderLiteral(literal);
        if (value.isEmpty()) {
            return Optional.empty();
        }
        String effectiveSymbol = flipped ? flip(symbol) : symbol;
        return Optional.of(EsqlIdentifiers.quote(field.name()) + " " + effectiveSymbol + " " + value.get());
    }

    private static String comparisonSymbol(EsqlBinaryComparison cmp) {
        if (cmp instanceof Equals) {
            return "==";
        }
        if (cmp instanceof NotEquals) {
            return "!=";
        }
        if (cmp instanceof GreaterThan) {
            return ">";
        }
        if (cmp instanceof GreaterThanOrEqual) {
            return ">=";
        }
        if (cmp instanceof LessThan) {
            return "<";
        }
        if (cmp instanceof LessThanOrEqual) {
            return "<=";
        }
        return null;
    }

    /** Mirror a comparison operator so {@code literal < field} becomes {@code field > literal}. */
    private static String flip(String symbol) {
        return switch (symbol) {
            case ">" -> "<";
            case ">=" -> "<=";
            case "<" -> ">";
            case "<=" -> ">=";
            case "==", "!=" -> symbol;
            default -> throw new IllegalStateException("Unexpected comparison symbol: " + symbol);
        };
    }

    private static Optional<String> renderLiteral(Literal literal) {
        DataType type = literal.dataType();
        Object value = literal.fold(FoldContext.small());
        if (value == null) {
            return Optional.empty();
        }
        return switch (type) {
            case KEYWORD, TEXT -> quoteString(bytesRefToString(value));
            case BOOLEAN -> Optional.of(Boolean.TRUE.equals(value) ? "true" : "false");
            case INTEGER, LONG, DOUBLE -> Optional.of(value.toString());
            default -> Optional.empty();
        };
    }

    private static String bytesRefToString(Object value) {
        return value instanceof BytesRef br ? br.utf8ToString() : value.toString();
    }

    /**
     * Renders a string literal as a quoted remote ES|QL value, or empty if it contains an ISO control
     * character (U+0000–U+001F, U+007F–U+009F). Returning empty only declines to <em>push down</em> the
     * comparison; the unpushed predicate stays in the local {@code FilterExec} and is still applied, so the
     * result is identical — only more rows cross the wire. Control characters (including {@code \t}) are not
     * pushed because their handling in a re-rendered remote query string is not guaranteed identical; this is
     * a deliberate v1 correctness-over-coverage trade-off.
     */
    private static Optional<String> quoteString(String value) {
        for (int i = 0; i < value.length(); i++) {
            if (Character.isISOControl(value.charAt(i))) {
                return Optional.empty();
            }
        }
        return Optional.of("\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"");
    }
}
