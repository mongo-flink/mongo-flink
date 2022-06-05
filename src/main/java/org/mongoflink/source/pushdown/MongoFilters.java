package org.mongoflink.source.pushdown;

import com.google.common.collect.ImmutableMap;
import com.mongodb.client.model.Filters;
import org.apache.flink.table.expressions.*;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.util.CollectionUtil;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

public class MongoFilters {

    private static final Logger LOG = LoggerFactory.getLogger(MongoFilters.class);

    private static final ImmutableMap<FunctionDefinition, Function<CallExpression, BsonDocument>>
            FILTERS =
            new ImmutableMap.Builder<
                    FunctionDefinition, Function<CallExpression, BsonDocument>>()
                    .put(BuiltInFunctionDefinitions.IS_NULL, MongoFilters::convertIsNull)
                    .put(
                            BuiltInFunctionDefinitions.IS_NOT_NULL,
                            MongoFilters::convertIsNotNull)
                    .put(BuiltInFunctionDefinitions.NOT, MongoFilters::convertNot)
                    .put(BuiltInFunctionDefinitions.OR, MongoFilters::convertOr)
                    .put(
                            BuiltInFunctionDefinitions.EQUALS,
                            call ->
                                    convertBinary(
                                            call,
                                            MongoFilters::convertEquals,
                                            MongoFilters::convertEquals))
                    .put(
                            BuiltInFunctionDefinitions.NOT_EQUALS,
                            call ->
                                    convertBinary(
                                            call,
                                            MongoFilters::convertNotEquals,
                                            MongoFilters::convertNotEquals))
                    .put(
                            BuiltInFunctionDefinitions.GREATER_THAN,
                            call ->
                                    convertBinary(
                                            call,
                                            MongoFilters::convertGreaterThan,
                                            MongoFilters::convertLessThanEquals))
                    .put(
                            BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL,
                            call ->
                                    convertBinary(
                                            call,
                                            MongoFilters::convertGreaterThanEquals,
                                            MongoFilters::convertLessThan))
                    .put(
                            BuiltInFunctionDefinitions.LESS_THAN,
                            call ->
                                    convertBinary(
                                            call,
                                            MongoFilters::convertLessThan,
                                            MongoFilters::convertGreaterThanEquals))
                    .put(
                            BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL,
                            call ->
                                    convertBinary(
                                            call,
                                            MongoFilters::convertLessThanEquals,
                                            MongoFilters::convertGreaterThan))
                    .build();

    private static BsonDocument convertLessThan(
            String colName, Serializable literal) {
        return Filters.lt(colName, literal).toBsonDocument();
    }

    private static BsonDocument convertLessThanEquals(
            String colName, Serializable literal) {
        return Filters.lte(colName, literal).toBsonDocument();
    }

    private static BsonDocument convertGreaterThanEquals(
            String colName, Serializable literal) {
        return Filters.gte(colName, literal).toBsonDocument();
    }

    private static BsonDocument convertGreaterThan(
            String colName, Serializable literal) {
        return Filters.gt(colName, literal).toBsonDocument();
    }

    private static BsonDocument convertNotEquals(
            String colName, Serializable literal) {
        return Filters.ne(colName, literal).toBsonDocument();
    }

    private static BsonDocument convertEquals(
            String colName, Serializable literal) {
        return Filters.eq(colName, literal).toBsonDocument();
    }

    private static BsonDocument convertOr(CallExpression callExp) {
        if (callExp.getChildren().size() < 2) {
            return null;
        }
        Expression left = callExp.getChildren().get(0);
        Expression right = callExp.getChildren().get(1);

        Bson c1 = toMongoPredicate(left);
        Bson c2 = toMongoPredicate(right);

        if (c1 == null || c2 == null) {
            return null;
        } else {
            return Filters.or(c1, c2).toBsonDocument();
        }
    }

    private static BsonDocument convertNot(CallExpression callExp) {
        if (callExp.getChildren().size() != 1) {
            // not a valid predicate
            LOG.debug(
                    "Unsupported predicate [{}] cannot be pushed into Mongo.",
                    callExp);
            return null;
        }
        Bson bson = toMongoPredicate(callExp.getChildren().get(0));
        if (bson == null) {
            Expression expression = callExp.getChildren().get(0);
            if (expression instanceof FieldReferenceExpression) {
                FieldReferenceExpression fieldReferenceExpression = (FieldReferenceExpression) expression;
                return Filters.eq(fieldReferenceExpression.getName(), false).toBsonDocument();
            }
            return null;
        } else {
            return Filters.not(bson).toBsonDocument();
        }
    }

    private static BsonDocument convertIsNotNull(CallExpression callExp) {
        if (!isUnaryValid(callExp)) {
            // not a valid predicate
            LOG.debug(
                    "Unsupported predicate [{}] cannot be pushed into Mongo.",
                    callExp);
            return null;
        }
        String colName = getColumnName(callExp);
        return Filters.ne(colName, null).toBsonDocument();
    }

    private static BsonDocument convertIsNull(CallExpression callExp) {
        if (!isUnaryValid(callExp)) {
            // not a valid predicate
            LOG.debug(
                    "Unsupported predicate [{}] cannot be pushed into Mongo.",
                    callExp);
            return null;
        }
        String colName = getColumnName(callExp);
        return Filters.eq(colName, null).toBsonDocument();
    }

    public static BsonDocument convertBinary(
            CallExpression callExp,
            BiFunction<String, Serializable, BsonDocument> func,
            BiFunction<String, Serializable, BsonDocument> reverseFunc) {
        if (!isBinaryValid(callExp)) {
            // not a valid predicate
            LOG.warn("Unsupported predicate [ " + callExp + "] cannot be pushed into Mongo.");
            return null;
        }

        String colName = getColumnName(callExp);

        // fetch literal and ensure it is serializable
        Object literalObj = getLiteral(callExp).get();
        Serializable literal;
        // validate that literal is serializable
        if (literalObj instanceof Serializable) {
            literal = (Serializable) literalObj;
        } else {
            LOG.warn(
                    "Encountered a non-serializable literal of type {}. "
                            + "Cannot push predicate [{}] into Mongo. "
                            + "This is a bug and should be reported.",
                    literalObj.getClass().getCanonicalName(),
                    callExp);
            return null;
        }

        return literalOnRight(callExp)
                ? func.apply(colName, literal)
                : reverseFunc.apply(colName, literal);
    }

    private static Bson toMongoPredicate(Expression expression) {
        if (expression instanceof CallExpression) {
            CallExpression callExp = (CallExpression) expression;
            if (FILTERS.get(callExp.getFunctionDefinition()) == null) {
                // unsupported predicate
                LOG.warn("Unsupported predicate [ " + expression + " ] cannot be pushed into Mongo.");
                return null;
            }
            return FILTERS.get(callExp.getFunctionDefinition()).apply(callExp);
        } else {
            // unsupported predicate
            LOG.warn("Unsupported predicate [ " + expression + " ] cannot be pushed into Mongo.");
            return null;
        }
    }

    private static String getColumnName(CallExpression comp) {
        if (literalOnRight(comp)) {
            return ((FieldReferenceExpression) comp.getChildren().get(0)).getName();
        } else {
            return ((FieldReferenceExpression) comp.getChildren().get(1)).getName();
        }
    }

    private static boolean literalOnRight(CallExpression comp) {
        if (comp.getChildren().size() == 1
                && comp.getChildren().get(0) instanceof FieldReferenceExpression) {
            return true;
        } else if (isLit(comp.getChildren().get(0)) && isRef(comp.getChildren().get(1))) {
            return false;
        } else if (isRef(comp.getChildren().get(0)) && isLit(comp.getChildren().get(1))) {
            return true;
        } else {
            throw new RuntimeException("Invalid binary comparison.");
        }
    }

    private static Optional<?> getLiteral(CallExpression comp) {
        if (literalOnRight(comp)) {
            ValueLiteralExpression valueLiteralExpression =
                    (ValueLiteralExpression) comp.getChildren().get(1);
            return valueLiteralExpression.getValueAs(
                    valueLiteralExpression.getOutputDataType().getConversionClass());
        } else {
            ValueLiteralExpression valueLiteralExpression =
                    (ValueLiteralExpression) comp.getChildren().get(0);
            return valueLiteralExpression.getValueAs(
                    valueLiteralExpression.getOutputDataType().getConversionClass());
        }
    }

    private static boolean isUnaryValid(CallExpression callExpression) {
        return callExpression.getChildren().size() == 1
                && isRef(callExpression.getChildren().get(0));
    }

    private static boolean isBinaryValid(CallExpression callExpression) {
        return callExpression.getChildren().size() == 2
                && (isRef(callExpression.getChildren().get(0))
                && isLit(callExpression.getChildren().get(1))
                || isLit(callExpression.getChildren().get(0))
                && isRef(callExpression.getChildren().get(1)));
    }

    private static boolean isRef(Expression expression) {
        return expression instanceof FieldReferenceExpression;
    }

    private static boolean isLit(Expression expression) {
        return expression instanceof ValueLiteralExpression;
    }

    public static BsonDocument build(List<ResolvedExpression> flinkFilters) {
        if (CollectionUtil.isNullOrEmpty(flinkFilters)) {
            return Filters.empty().toBsonDocument();
        }
        List<Bson> allFilters = new ArrayList<>();
        for (ResolvedExpression flinkFilter : flinkFilters) {
            if (!(flinkFilter instanceof CallExpression)) {
                // unsupported predicate
                LOG.warn("Unsupported predicate [" + flinkFilter + "] cannot be pushed into Mongo.");
                continue;
            }
            CallExpression expression = (CallExpression) flinkFilter;

            FunctionDefinition functionDefinition = expression.getFunctionDefinition();
            BsonDocument bsonDocument = FILTERS.get(functionDefinition).apply(expression);
            if (bsonDocument == null) {
                continue;
            }
            allFilters.add(bsonDocument);
        }
        return Filters.and(allFilters).toBsonDocument();
    }

}

