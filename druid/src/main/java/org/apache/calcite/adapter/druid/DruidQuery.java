/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.druid;

import org.apache.calcite.DataContext;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.interpreter.BindableRel;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.interpreter.Interpreter;
import org.apache.calcite.interpreter.Node;
import org.apache.calcite.interpreter.Sink;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.DateTimeStringUtils;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.TimestampString;
import org.apache.calcite.util.Util;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.joda.time.Interval;

import java.io.IOException;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import static org.apache.calcite.sql.SqlKind.INPUT_REF;

/**
 * Relational expression representing a scan of a Druid data set.
 */
public class DruidQuery extends AbstractRelNode implements BindableRel {

  /**
   * Provides a standard list of supported Calcite operators that can be converted to
   * Druid Expressions. This can be used as is or re-adapted based on underline
   * engine operator syntax.
   */
  public static final List<DruidSqlOperatorConverter> DEFAULT_OPERATORS_LIST =
      ImmutableList.<DruidSqlOperatorConverter>builder()
          .add(new DirectOperatorConversion(SqlStdOperatorTable.EXP, "exp"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.CONCAT, "concat"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.DIVIDE_INTEGER, "div"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.LIKE, "like"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.LN, "log"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.SQRT, "sqrt"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.LOWER, "lower"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.LOG10, "log10"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.REPLACE, "replace"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.UPPER, "upper"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.POWER, "pow"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.ABS, "abs"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.CASE, "case_searched"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.CHAR_LENGTH, "strlen"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.CHARACTER_LENGTH, "strlen"))
          .add(new BinaryOperatorConversion(SqlStdOperatorTable.EQUALS, "=="))
          .add(new BinaryOperatorConversion(SqlStdOperatorTable.NOT_EQUALS, "!="))
          .add(new BinaryOperatorConversion(SqlStdOperatorTable.OR, "||"))
          .add(new BinaryOperatorConversion(SqlStdOperatorTable.AND, "&&"))
          .add(new BinaryOperatorConversion(SqlStdOperatorTable.LESS_THAN, "<"))
          .add(new BinaryOperatorConversion(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, "<="))
          .add(new BinaryOperatorConversion(SqlStdOperatorTable.GREATER_THAN, ">"))
          .add(new BinaryOperatorConversion(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, ">="))
          .add(new BinaryOperatorConversion(SqlStdOperatorTable.PLUS, "+"))
          .add(new BinaryOperatorConversion(SqlStdOperatorTable.MINUS, "-"))
          .add(new BinaryOperatorConversion(SqlStdOperatorTable.MULTIPLY, "*"))
          .add(new BinaryOperatorConversion(SqlStdOperatorTable.DIVIDE, "/"))
          .add(new BinaryOperatorConversion(SqlStdOperatorTable.MOD, "%"))
          .add(new DruidSqlCastConverter())
          .add(new ExtractOperatorConversion())
          .add(new UnaryPrefixOperatorConversion(SqlStdOperatorTable.NOT, "!"))
          .add(new UnaryPrefixOperatorConversion(SqlStdOperatorTable.UNARY_MINUS, "-"))
          .add(new UnarySuffixOperatorConversion(SqlStdOperatorTable.IS_FALSE, "<= 0"))
          .add(new UnarySuffixOperatorConversion(SqlStdOperatorTable.IS_NOT_TRUE, "<= 0"))
          .add(new UnarySuffixOperatorConversion(SqlStdOperatorTable.IS_TRUE, "> 0"))
          .add(new UnarySuffixOperatorConversion(SqlStdOperatorTable.IS_NOT_FALSE, "> 0"))
          .add(new UnarySuffixOperatorConversion(SqlStdOperatorTable.IS_NULL, "== null"))
          .add(new UnarySuffixOperatorConversion(SqlStdOperatorTable.IS_NOT_NULL, "!= null"))
          .add(new FloorOperatorConversion())
          .add(new CeilOperatorConversion())
          .add(new SubstringOperatorConversion())
          .build();
  protected QuerySpec querySpec;

  final RelOptTable table;
  final DruidTable druidTable;
  final ImmutableList<Interval> intervals;
  final ImmutableList<RelNode> rels;
  /**
   * This operator map provides DruidSqlOperatorConverter instance to convert a Calcite RexNode to
   * Druid Expression when possible.
   */
  final Map<SqlOperator, DruidSqlOperatorConverter> converterOperatorMap;

  private static final Pattern VALID_SIG = Pattern.compile("sf?p?(a?|ao)l?");
  private static final String EXTRACT_COLUMN_NAME_PREFIX = "extract";
  private static final String FLOOR_COLUMN_NAME_PREFIX = "floor";
  protected static final String DRUID_QUERY_FETCH = "druid.query.fetch";

  /**
   * Creates a DruidQuery.
   *
   * @param cluster        Cluster
   * @param traitSet       Traits
   * @param table          Table
   * @param druidTable     Druid table
   * @param intervals      Intervals for the query
   * @param rels           Internal relational expressions
   * @param converterOperatorMap mapping of Calcite Sql Operator to Druid Expression API.
   */
  protected DruidQuery(RelOptCluster cluster, RelTraitSet traitSet,
      RelOptTable table, DruidTable druidTable,
      List<Interval> intervals, List<RelNode> rels,
      Map<SqlOperator, DruidSqlOperatorConverter> converterOperatorMap) {
    super(cluster, traitSet);
    this.table = table;
    this.druidTable = druidTable;
    this.intervals = ImmutableList.copyOf(intervals);
    this.rels = ImmutableList.copyOf(rels);
    this.converterOperatorMap = Preconditions.checkNotNull(converterOperatorMap, "Operator map "
        + "can not be null");
    assert isValid(Litmus.THROW, null);
  }
  /** Returns a string describing the operations inside this query.
   *
   * <p>For example, "sfpaol" means {@link TableScan} (s)
   * followed by {@link Filter} (f)
   * followed by {@link Project} (p)
   * followed by {@link Aggregate} (a)
   * followed by {@link Project} (o)
   * followed by {@link Sort} (l).
   *
   * @see #isValidSignature(String)
   */
  String signature() {
    final StringBuilder b = new StringBuilder();
    boolean flag = false;
    for (RelNode rel : rels) {
      b.append(rel instanceof TableScan ? 's'
          : (rel instanceof Project && flag) ? 'o'
          : rel instanceof Filter ? 'f'
          : rel instanceof Aggregate ? 'a'
          : rel instanceof Sort ? 'l'
          : rel instanceof Project ? 'p'
          : '!');
      flag = flag || rel instanceof Aggregate;
    }
    return b.toString();
  }

  @Override public boolean isValid(Litmus litmus, Context context) {
    if (!super.isValid(litmus, context)) {
      return false;
    }
    final String signature = signature();
    if (!isValidSignature(signature)) {
      return litmus.fail("invalid signature [{}]", signature);
    }
    if (rels.isEmpty()) {
      return litmus.fail("must have at least one rel");
    }
    for (int i = 0; i < rels.size(); i++) {
      final RelNode r = rels.get(i);
      if (i == 0) {
        if (!(r instanceof TableScan)) {
          return litmus.fail("first rel must be TableScan, was ", r);
        }
        if (r.getTable() != table) {
          return litmus.fail("first rel must be based on table table");
        }
      } else {
        final List<RelNode> inputs = r.getInputs();
        if (inputs.size() != 1 || inputs.get(0) != rels.get(i - 1)) {
          return litmus.fail("each rel must have a single input");
        }
        if (r instanceof Aggregate) {
          final Aggregate aggregate = (Aggregate) r;
          if (aggregate.getGroupSets().size() != 1
              || aggregate.indicator) {
            return litmus.fail("no grouping sets");
          }
        }
        if (r instanceof Filter) {
          final Filter filter = (Filter) r;
          if (!isValidFilter(filter.getCondition(), this)) {
            return litmus.fail("invalid filter [{}]", filter.getCondition());
          }
        }
        if (r instanceof Sort) {
          final Sort sort = (Sort) r;
          if (sort.offset != null && RexLiteral.intValue(sort.offset) != 0) {
            return litmus.fail("offset not supported");
          }
        }
      }
    }
    return true;
  }

  public Map<SqlOperator, DruidSqlOperatorConverter> getConverterOperatorMap() {
    return converterOperatorMap;
  }

  /**
   * Checks if is possible to Translate RexNode to Druid Simple Filter or Expression Filter
   *
   * @param e Filter RexNode
   * @param druidQuery Druid Query used mostly to provide the operator map converter
   * see this {@link DruidQuery#getConverterOperatorMap()}
   *
   * @return true if the filter can be pushed to Druid
   */
  public static boolean isValidFilter(RexNode e, DruidQuery druidQuery) {
    switch (e.getKind()) {
    case AND:
    case OR:
    case NOT:
      final RexCall call = (RexCall) e;
      for (RexNode rexNode : call.getOperands()) {
        if (!isValidSimpleFilter(rexNode, false)
            && !isValidExpressionFilter(rexNode, druidQuery)) {
          return false;
        }
      }
      return true;
    }
    return isValidSimpleFilter(e, false) || isValidExpressionFilter(e, druidQuery);
  }

  /**
   * Checks tha it is valid expression filter by Translating the rexNode to Druid Expression
   * @param rexNode rexNode to translate to Druid Filter
   * @param druidQuery druid Query used to access configuration eg timezone and
   *                   operator map {@link DruidQuery#getConverterOperatorMap()} etc
   *
   * @return true if it is a valid Expression Filter
   */
  public static boolean isValidExpressionFilter(RexNode rexNode, DruidQuery druidQuery) {
    //currently any expression != null is valid to push as Expression Filter
    return DruidExpressions.toDruidExpression(rexNode, druidQuery.table.getRowType(),
        druidQuery) != null;
  }

  /**
   * @param e RexNode
   * @param boundedComparator whether it is a bound comparator eg =,!=,<=,>=,>,<,between
   *
   * @return True if the filter translates a simple leaf Druid filter (not Expression Filter).
   *
   */
  public static boolean isValidSimpleFilter(RexNode e, boolean boundedComparator) {
    switch (e.getKind()) {
    case INPUT_REF:
      return true;
    case LITERAL:
      return !RexLiteral.isNullLiteral(e);
    case AND:
    case OR:
    case NOT:
    case IN:
    case IS_NULL:
    case IS_NOT_NULL:
      return areValidSimpleFilters(((RexCall) e).getOperands(), false);
    case EQUALS:
    case NOT_EQUALS:
    case LESS_THAN:
    case LESS_THAN_OR_EQUAL:
    case GREATER_THAN:
    case GREATER_THAN_OR_EQUAL:
    case BETWEEN:
      return areValidSimpleFilters(((RexCall) e).getOperands(), true);
    case CAST:
      return isValidLeafCast((RexCall) e, boundedComparator);
    case EXTRACT:
      return TimeExtractionFunction.isValidTimeExtract((RexCall) e);
    case FLOOR:
      return TimeExtractionFunction.isValidTimeFloor((RexCall) e);
    case IS_TRUE:
      return isValidSimpleFilter(((RexCall) e).getOperands().get(0), boundedComparator);
    default:
      return false;
    }
  }

  private static boolean areValidSimpleFilters(List<RexNode> es, boolean boundedComparator) {
    for (RexNode e : es) {
      if (!isValidSimpleFilter(e, boundedComparator)) {
        return false;
      }
    }
    return true;
  }

  /**
   * @param e rexCall CAST call to check if it is a valid leaf CAST
   * @param boundedComparator indicates if we have a bounded comparator
   *
   * @return true if the cast is a leaf node and it has valid to/from types, false otherwise
   */
  private static boolean isValidLeafCast(RexCall e, boolean boundedComparator) {
    assert e.isA(SqlKind.CAST);
    final RexNode input = e.getOperands().get(0);
    if (!input.isA(INPUT_REF)) {
      // it is not a leaf cast don't bother going further.
      return false;
    }
    final SqlTypeName toTypeName = e.getType().getSqlTypeName();
    if (e.getType().getFamily() == SqlTypeFamily.CHARACTER) {
      // CAST of input to character type
      return true;
    }
    if (toTypeName.getFamily() == SqlTypeFamily.NUMERIC && boundedComparator) {
      // CAST of input to numeric type, it is part of a bounded comparison
      return true;
    }
    if (toTypeName == SqlTypeName.DATE || toTypeName == SqlTypeName.TIMESTAMP
        || toTypeName == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
      // CAST of literal to timestamp type
      return true;
    }
    if (toTypeName.getFamily().contains(input.getType())) {
      //same type it is okay to push it
      return true;
    }
    // Currently other CAST operations cannot be pushed to Druid
    return false;
  }

  /** Returns whether a signature represents an sequence of relational operators
   * that can be translated into a valid Druid query. */
  static boolean isValidSignature(String signature) {
    return VALID_SIG.matcher(signature).matches();
  }

  /** Creates a DruidQuery. */
  public static DruidQuery create(RelOptCluster cluster, RelTraitSet traitSet,
      RelOptTable table, DruidTable druidTable, List<RelNode> rels) {
    final ImmutableMap converterOperatorMap = ImmutableMap.<SqlOperator,
        DruidSqlOperatorConverter>builder().putAll(
        Lists.transform(DEFAULT_OPERATORS_LIST, new Function<DruidSqlOperatorConverter,
            Map.Entry<SqlOperator, DruidSqlOperatorConverter>>() {
          @Nullable @Override public Map.Entry<SqlOperator, DruidSqlOperatorConverter> apply(
              final DruidSqlOperatorConverter input) {
            return Maps.immutableEntry(input.calciteOperator(), input);
          }
        })).build();
    return create(cluster, traitSet, table, druidTable, druidTable.intervals, rels,
        converterOperatorMap
    );
  }

  /** Creates a DruidQuery. */
  public static DruidQuery create(RelOptCluster cluster, RelTraitSet traitSet,
      RelOptTable table, DruidTable druidTable, List<RelNode> rels,
      Map<SqlOperator, DruidSqlOperatorConverter> converterOperatorMap) {
    return create(cluster, traitSet, table, druidTable, druidTable.intervals, rels,
        converterOperatorMap
    );
  }

  /**
   * Creates a DruidQuery.
   */
  private static DruidQuery create(RelOptCluster cluster, RelTraitSet traitSet,
      RelOptTable table, DruidTable druidTable, List<Interval> intervals,
      List<RelNode> rels, Map<SqlOperator, DruidSqlOperatorConverter> converterOperatorMap) {
    return new DruidQuery(cluster, traitSet, table, druidTable, intervals, rels,
        converterOperatorMap);
  }

  /** Extends a DruidQuery. */
  public static DruidQuery extendQuery(DruidQuery query, RelNode r) {
    final ImmutableList.Builder<RelNode> builder = ImmutableList.builder();
    return DruidQuery.create(query.getCluster(), r.getTraitSet().replace(query.getConvention()),
        query.getTable(), query.druidTable, query.intervals,
        builder.addAll(query.rels).add(r).build(), query.getConverterOperatorMap());
  }

  /** Extends a DruidQuery. */
  public static DruidQuery extendQuery(DruidQuery query,
      List<Interval> intervals) {
    return DruidQuery.create(query.getCluster(), query.getTraitSet(), query.getTable(),
        query.druidTable, intervals, query.rels, query.getConverterOperatorMap());
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.isEmpty();
    return this;
  }

  @Override public RelDataType deriveRowType() {
    return getCluster().getTypeFactory().createStructType(
        Pair.right(Util.last(rels).getRowType().getFieldList()),
        getQuerySpec().fieldNames);
  }

  public TableScan getTableScan() {
    return (TableScan) rels.get(0);
  }

  public RelNode getTopNode() {
    return Util.last(rels);
  }

  @Override public RelOptTable getTable() {
    return table;
  }

  public DruidTable getDruidTable() {
    return druidTable;
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    for (RelNode rel : rels) {
      if (rel instanceof TableScan) {
        TableScan tableScan = (TableScan) rel;
        pw.item("table", tableScan.getTable().getQualifiedName());
        pw.item("intervals", intervals);
      } else if (rel instanceof Filter) {
        pw.item("filter", ((Filter) rel).getCondition());
      } else if (rel instanceof Project) {
        if (((Project) rel).getInput() instanceof Aggregate) {
          pw.item("post_projects", ((Project) rel).getProjects());
        } else {
          pw.item("projects", ((Project) rel).getProjects());
        }
      } else if (rel instanceof Aggregate) {
        final Aggregate aggregate = (Aggregate) rel;
        pw.item("groups", aggregate.getGroupSet())
            .item("aggs", aggregate.getAggCallList());
      } else if (rel instanceof Sort) {
        final Sort sort = (Sort) rel;
        for (Ord<RelFieldCollation> ord
            : Ord.zip(sort.collation.getFieldCollations())) {
          pw.item("sort" + ord.i, ord.e.getFieldIndex());
        }
        for (Ord<RelFieldCollation> ord
            : Ord.zip(sort.collation.getFieldCollations())) {
          pw.item("dir" + ord.i, ord.e.shortString());
        }
        pw.itemIf("fetch", sort.fetch, sort.fetch != null);
      } else {
        throw new AssertionError("rel type not supported in Druid query "
            + rel);
      }
    }
    return pw;
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    return Util.last(rels)
        .computeSelfCost(planner, mq)
        // Cost increases with the number of fields queried.
        // A plan returning 100 or more columns will have 2x the cost of a
        // plan returning 2 columns.
        // A plan where all extra columns are pruned will be preferred.
        .multiplyBy(
            RelMdUtil.linear(querySpec.fieldNames.size(), 2, 100, 1d, 2d))
        .multiplyBy(getQueryTypeCostMultiplier())
        // a plan with sort pushed to druid is better than doing sort outside of druid
        .multiplyBy(Util.last(rels) instanceof Sort ? 0.1 : 1.0);
  }

  private double getQueryTypeCostMultiplier() {
    // Cost of Select > GroupBy > Timeseries > TopN
    switch (querySpec.queryType) {
    case SELECT:
      return .1;
    case GROUP_BY:
      return .08;
    case TIMESERIES:
      return .06;
    case TOP_N:
      return .04;
    default:
      return .2;
    }
  }

  @Override public void register(RelOptPlanner planner) {
    for (RelOptRule rule : DruidRules.RULES) {
      planner.addRule(rule);
    }
    for (RelOptRule rule : Bindables.RULES) {
      planner.addRule(rule);
    }
  }

  @Override public Class<Object[]> getElementType() {
    return Object[].class;
  }

  @Override public Enumerable<Object[]> bind(DataContext dataContext) {
    return table.unwrap(ScannableTable.class).scan(dataContext);
  }

  @Override public Node implement(InterpreterImplementor implementor) {
    return new DruidQueryNode(implementor.interpreter, this);
  }

  public QuerySpec getQuerySpec() {
    if (querySpec == null) {
      querySpec = deriveQuerySpec();
      assert querySpec != null : this;
    }
    return querySpec;
  }

  protected QuerySpec deriveQuerySpec() {
    final RelDataType rowType = table.getRowType();
    int i = 1;

    RexNode filter = null;
    if (i < rels.size() && rels.get(i) instanceof Filter) {
      final Filter filterRel = (Filter) rels.get(i++);
      filter = filterRel.getCondition();
    }

    List<RexNode> projects = null;
    if (i < rels.size() && rels.get(i) instanceof Project) {
      final Project project = (Project) rels.get(i++);
      projects = project.getProjects();
    }

    ImmutableBitSet groupSet = null;
    List<AggregateCall> aggCalls = null;
    List<String> aggNames = null;
    if (i < rels.size() && rels.get(i) instanceof Aggregate) {
      final Aggregate aggregate = (Aggregate) rels.get(i++);
      groupSet = aggregate.getGroupSet();
      aggCalls = aggregate.getAggCallList();
      aggNames = Util.skip(aggregate.getRowType().getFieldNames(),
          groupSet.cardinality());
    }

    Project postProject = null;
    if (i < rels.size() && rels.get(i) instanceof Project) {
      postProject = (Project) rels.get(i++);
    }

    List<Integer> collationIndexes = null;
    List<Direction> collationDirections = null;
    ImmutableBitSet.Builder numericCollationBitSetBuilder = ImmutableBitSet.builder();
    Integer fetch = null;
    if (i < rels.size() && rels.get(i) instanceof Sort) {
      final Sort sort = (Sort) rels.get(i++);
      collationIndexes = new ArrayList<>();
      collationDirections = new ArrayList<>();
      for (RelFieldCollation fCol : sort.collation.getFieldCollations()) {
        collationIndexes.add(fCol.getFieldIndex());
        collationDirections.add(fCol.getDirection());
        if (sort.getRowType().getFieldList().get(fCol.getFieldIndex()).getType().getFamily()
            == SqlTypeFamily.NUMERIC) {
          numericCollationBitSetBuilder.set(fCol.getFieldIndex());
        }
      }
      fetch = sort.fetch != null ? RexLiteral.intValue(sort.fetch) : null;
    }

    if (i != rels.size()) {
      throw new AssertionError("could not implement all rels");
    }

    return getQuery(rowType, filter, projects, groupSet, aggCalls, aggNames,
        collationIndexes, collationDirections, numericCollationBitSetBuilder.build(), fetch,
        postProject);
  }

  public QueryType getQueryType() {
    return getQuerySpec().queryType;
  }

  public String getQueryString() {
    return getQuerySpec().queryString;
  }

  protected CalciteConnectionConfig getConnectionConfig() {
    return getCluster().getPlanner().getContext().unwrap(CalciteConnectionConfig.class);
  }

  protected QuerySpec getQuery(RelDataType rowType, RexNode filter, List<RexNode> projects,
      ImmutableBitSet groupSet, List<AggregateCall> aggCalls, List<String> aggNames,
      List<Integer> collationIndexes, List<Direction> collationDirections,
      ImmutableBitSet numericCollationIndexes, Integer fetch, Project postProject) {
    final CalciteConnectionConfig config = getConnectionConfig();
    QueryType queryType = QueryType.SCAN;
    final Translator translator = new Translator(druidTable, rowType, config.timeZone(), this);
    List<String> fieldNames = rowType.getFieldNames();
    Set<String> usedFieldNames = Sets.newHashSet(fieldNames);

    // Handle filter
    final Json jsonFilter;
    if (filter != null) {
      jsonFilter = translator.toDruidFilters(filter);
      Preconditions.checkNotNull(
          jsonFilter, DateTimeStringUtils.format("Druid Filter is null instead of [%s]", filter));
    } else {
      jsonFilter = null;
    }

    // Then we handle project
    if (projects != null) {
      translator.clearFieldNameLists();
      final ImmutableList.Builder<String> builder = ImmutableList.builder();
      for (RexNode project : projects) {
        builder.add(translator.translate(project, true, false));
      }
      fieldNames = builder.build();
    }

    // Finally we handle aggregate and sort. Handling of these
    // operators is more complex, since we need to extract
    // the conditions to know whether the query will be
    // executed as a Timeseries, TopN, or GroupBy in Druid
    final List<DimensionSpec> dimensions = new ArrayList<>();
    final List<JsonAggregation> aggregations = new ArrayList<>();
    final List<JsonPostAggregation> postAggs = new ArrayList<>();
    Granularity finalGranularity = Granularity.ALL;
    Direction timeSeriesDirection = null;
    JsonLimit limit = null;
    TimeExtractionDimensionSpec timeExtractionDimensionSpec = null;
    if (groupSet != null) {
      assert aggCalls != null;
      assert aggNames != null;
      assert aggCalls.size() == aggNames.size();

      int timePositionIdx = -1;
      ImmutableList.Builder<String> builder = ImmutableList.builder();
      if (projects != null) {
        for (int groupKey : groupSet) {
          final String fieldName = fieldNames.get(groupKey);
          final RexNode project = projects.get(groupKey);
          if (project instanceof RexInputRef) {
            // Reference could be to the timestamp or druid dimension but no druid metric
            final RexInputRef ref = (RexInputRef) project;
            final String originalFieldName = druidTable.getRowType(getCluster().getTypeFactory())
                .getFieldList().get(ref.getIndex()).getName();
            if (originalFieldName.equals(druidTable.timestampFieldName)) {
              finalGranularity = Granularity.ALL;
              String extractColumnName = SqlValidatorUtil.uniquify(EXTRACT_COLUMN_NAME_PREFIX,
                  usedFieldNames, SqlValidatorUtil.EXPR_SUGGESTER);
              timeExtractionDimensionSpec = TimeExtractionDimensionSpec.makeFullTimeExtract(
                  extractColumnName, config.timeZone());
              dimensions.add(timeExtractionDimensionSpec);
              builder.add(extractColumnName);
              assert timePositionIdx == -1;
              timePositionIdx = groupKey;
            } else {
              dimensions.add(new DefaultDimensionSpec(fieldName));
              builder.add(fieldName);
            }
          } else if (project instanceof RexCall) {
            // Call, check if we should infer granularity
            final RexCall call = (RexCall) project;
            final Granularity funcGranularity = DruidDateTimeUtils.extractGranularity(call);
            if (funcGranularity != null) {
              final String extractColumnName;
              switch (call.getKind()) {
              case EXTRACT:
                // case extract field from time column
                finalGranularity = Granularity.ALL;
                extractColumnName = SqlValidatorUtil.uniquify(EXTRACT_COLUMN_NAME_PREFIX
                        + "_" + funcGranularity.value, usedFieldNames,
                    SqlValidatorUtil.EXPR_SUGGESTER);
                timeExtractionDimensionSpec = TimeExtractionDimensionSpec.makeTimeExtract(
                    funcGranularity, extractColumnName, config.timeZone());
                dimensions.add(timeExtractionDimensionSpec);
                builder.add(extractColumnName);
                break;
              case FLOOR:
                // case floor time column
                if (groupSet.cardinality() > 1) {
                  // case we have more than 1 group by key -> then will have druid group by
                  extractColumnName = SqlValidatorUtil.uniquify(FLOOR_COLUMN_NAME_PREFIX
                          + "_" + funcGranularity.value, usedFieldNames,
                      SqlValidatorUtil.EXPR_SUGGESTER);
                  dimensions.add(
                      TimeExtractionDimensionSpec.makeTimeFloor(funcGranularity,
                          extractColumnName, config.timeZone()));
                  finalGranularity = Granularity.ALL;
                  builder.add(extractColumnName);
                } else {
                  // case timeseries we can not use extraction function
                  finalGranularity = funcGranularity;
                  builder.add(fieldName);
                }
                assert timePositionIdx == -1;
                timePositionIdx = groupKey;
                break;
              default:
                throw new AssertionError();
              }

            } else {
              dimensions.add(new DefaultDimensionSpec(fieldName));
              builder.add(fieldName);
            }
          } else {
            throw new AssertionError("incompatible project expression: " + project);
          }
        }
      } else {
        for (int groupKey : groupSet) {
          final String s = fieldNames.get(groupKey);
          if (s.equals(druidTable.timestampFieldName)) {
            finalGranularity = Granularity.ALL;
            // Generate unique name as timestampFieldName is taken
            String extractColumnName = SqlValidatorUtil.uniquify(EXTRACT_COLUMN_NAME_PREFIX,
                usedFieldNames, SqlValidatorUtil.EXPR_SUGGESTER);
            timeExtractionDimensionSpec = TimeExtractionDimensionSpec.makeFullTimeExtract(
                extractColumnName, config.timeZone());
            dimensions.add(timeExtractionDimensionSpec);
            builder.add(extractColumnName);
            assert timePositionIdx == -1;
            timePositionIdx = groupKey;
          } else {
            dimensions.add(new DefaultDimensionSpec(s));
            builder.add(s);
          }
        }
      }

      for (Pair<AggregateCall, String> agg : Pair.zip(aggCalls, aggNames)) {
        final JsonAggregation jsonAggregation =
            getJsonAggregation(fieldNames, agg.right, agg.left, projects, translator);
        aggregations.add(jsonAggregation);
        builder.add(jsonAggregation.name);
      }

      fieldNames = builder.build();

      if (postProject != null) {
        builder = ImmutableList.builder();
        for (Pair<RexNode, String> pair : postProject.getNamedProjects()) {
          String fieldName = pair.right;
          RexNode rex = pair.left;
          builder.add(fieldName);
          // Render Post JSON object when PostProject exists. In DruidPostAggregationProjectRule
          // all check has been done to ensure all RexCall rexNode can be pushed in.
          if (rex instanceof RexCall) {
            DruidQuery.JsonPostAggregation jsonPost = getJsonPostAggregation(fieldName, rex,
                postProject.getInput());
            postAggs.add(jsonPost);
          }
        }
        fieldNames = builder.build();
      }

      ImmutableList<JsonCollation> collations = null;
      boolean sortsMetric = false;
      if (collationIndexes != null) {
        assert collationDirections != null;
        ImmutableList.Builder<JsonCollation> colBuilder =
            ImmutableList.builder();
        for (Pair<Integer, Direction> p : Pair.zip(collationIndexes, collationDirections)) {
          final String dimensionOrder = numericCollationIndexes.get(p.left) ? "numeric"
              : "alphanumeric";
          colBuilder.add(
              new JsonCollation(fieldNames.get(p.left),
                  p.right == Direction.DESCENDING ? "descending" : "ascending", dimensionOrder));
          if (p.left >= groupSet.cardinality() && p.right == Direction.DESCENDING) {
            // Currently only support for DESC in TopN
            sortsMetric = true;
          } else if (p.left == timePositionIdx) {
            assert timeSeriesDirection == null;
            timeSeriesDirection = p.right;
          }
        }
        collations = colBuilder.build();
      }

      limit = new JsonLimit("default", fetch, collations);

      if (dimensions.isEmpty() && (collations == null || timeSeriesDirection != null)) {
        queryType = QueryType.TIMESERIES;
        assert fetch == null;
      } else if (dimensions.size() == 1
          && finalGranularity == Granularity.ALL
          && sortsMetric
          && collations.size() == 1
          && fetch != null
          && config.approximateTopN()) {
        queryType = QueryType.TOP_N;
      } else {
        queryType = QueryType.GROUP_BY;
      }
    } else {
      assert aggCalls == null;
      assert aggNames == null;
      assert collationIndexes == null || collationIndexes.isEmpty();
      assert collationDirections == null || collationDirections.isEmpty();
    }

    final StringWriter sw = new StringWriter();
    final JsonFactory factory = new JsonFactory();
    try {
      final JsonGenerator generator = factory.createGenerator(sw);

      switch (queryType) {
      case TIMESERIES:
        generator.writeStartObject();

        generator.writeStringField("queryType", "timeseries");
        generator.writeStringField("dataSource", druidTable.dataSource);
        generator.writeBooleanField("descending", timeSeriesDirection != null
            && timeSeriesDirection == Direction.DESCENDING);
        generator.writeStringField("granularity", finalGranularity.value);
        writeFieldIf(generator, "filter", jsonFilter);
        writeField(generator, "aggregations", aggregations);
        writeFieldIf(generator, "postAggregations", postAggs.size() > 0 ? postAggs : null);
        writeField(generator, "intervals", intervals);

        generator.writeFieldName("context");
        // The following field is necessary to conform with SQL semantics (CALCITE-1589)
        generator.writeStartObject();
        final boolean isCountStar = Granularity.ALL == finalGranularity
            && aggregations.size() == 1
            && aggregations.get(0).type.equals("count");
        //Count(*) returns 0 if result set is empty thus need to set skipEmptyBuckets to false
        generator.writeBooleanField("skipEmptyBuckets", !isCountStar);
        generator.writeEndObject();

        generator.writeEndObject();
        break;

      case TOP_N:
        generator.writeStartObject();

        generator.writeStringField("queryType", "topN");
        generator.writeStringField("dataSource", druidTable.dataSource);
        generator.writeStringField("granularity", finalGranularity.value);
        writeField(generator, "dimension", dimensions.get(0));
        generator.writeStringField("metric", fieldNames.get(collationIndexes.get(0)));
        writeFieldIf(generator, "filter", jsonFilter);
        writeField(generator, "aggregations", aggregations);
        writeFieldIf(generator, "postAggregations", postAggs.size() > 0 ? postAggs : null);
        writeField(generator, "intervals", intervals);
        generator.writeNumberField("threshold", fetch);

        generator.writeEndObject();
        break;

      case GROUP_BY:
        generator.writeStartObject();
        generator.writeStringField("queryType", "groupBy");
        generator.writeStringField("dataSource", druidTable.dataSource);
        generator.writeStringField("granularity", finalGranularity.value);
        writeField(generator, "dimensions", dimensions);
        writeFieldIf(generator, "limitSpec", limit);
        writeFieldIf(generator, "filter", jsonFilter);
        writeField(generator, "aggregations", aggregations);
        writeFieldIf(generator, "postAggregations", postAggs.size() > 0 ? postAggs : null);
        writeField(generator, "intervals", intervals);
        writeFieldIf(generator, "having", null);

        generator.writeEndObject();
        break;

      case SELECT:
        generator.writeStartObject();

        generator.writeStringField("queryType", "select");
        generator.writeStringField("dataSource", druidTable.dataSource);
        generator.writeBooleanField("descending", false);
        writeField(generator, "intervals", intervals);
        writeFieldIf(generator, "filter", jsonFilter);
        writeField(generator, "dimensions", translator.dimensions);
        writeField(generator, "metrics", translator.metrics);
        generator.writeStringField("granularity", finalGranularity.value);

        generator.writeFieldName("pagingSpec");
        generator.writeStartObject();
        generator.writeNumberField("threshold", fetch != null ? fetch
            : CalciteConnectionProperty.DRUID_FETCH.wrap(new Properties()).getInt());
        generator.writeBooleanField("fromNext", true);
        generator.writeEndObject();

        generator.writeFieldName("context");
        generator.writeStartObject();
        generator.writeBooleanField(DRUID_QUERY_FETCH, fetch != null);
        generator.writeEndObject();

        generator.writeEndObject();
        break;

      case SCAN:
        generator.writeStartObject();

        generator.writeStringField("queryType", "scan");
        generator.writeStringField("dataSource", druidTable.dataSource);
        writeField(generator, "intervals", intervals);
        writeFieldIf(generator, "filter", jsonFilter);
        writeField(generator, "columns",
            Lists.transform(fieldNames, new Function<String, String>() {
              @Override public String apply(String s) {
                return s.equals(druidTable.timestampFieldName)
                    ? DruidTable.DEFAULT_TIMESTAMP_COLUMN : s;
              }
            }));
        generator.writeStringField("granularity", finalGranularity.value);
        generator.writeStringField("resultFormat", "compactedList");
        if (fetch != null) {
          generator.writeNumberField("limit", fetch);
        }

        generator.writeEndObject();
        break;

      default:
        throw new AssertionError("unknown query type " + queryType);
      }

      generator.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

    return new QuerySpec(queryType, sw.toString(), fieldNames);
  }

  protected JsonAggregation getJsonAggregation(List<String> fieldNames,
      String name, AggregateCall aggCall, List<RexNode> projects, Translator translator) {
    final List<String> list = new ArrayList<>();
    for (Integer arg : aggCall.getArgList()) {
      list.add(fieldNames.get(arg));
    }
    final String only = Iterables.getFirst(list, null);
    final boolean fractional;
    final RelDataType type = aggCall.getType();
    final SqlTypeName sqlTypeName = type.getSqlTypeName();
    if (SqlTypeFamily.APPROXIMATE_NUMERIC.getTypeNames().contains(sqlTypeName)) {
      fractional = true;
    } else if (SqlTypeFamily.INTEGER.getTypeNames().contains(sqlTypeName)) {
      fractional = false;
    } else if (SqlTypeFamily.EXACT_NUMERIC.getTypeNames().contains(sqlTypeName)) {
      // Decimal
      assert sqlTypeName == SqlTypeName.DECIMAL;
      if (type.getScale() == 0) {
        fractional = false;
      } else {
        fractional = true;
      }
    } else {
      // Cannot handle this aggregate function type
      throw new AssertionError("unknown aggregate type " + type);
    }

    JsonAggregation aggregation;

    CalciteConnectionConfig config = getConnectionConfig();

    // Convert from a complex metric
    ComplexMetric complexMetric = druidTable.resolveComplexMetric(only, aggCall);

    switch (aggCall.getAggregation().getKind()) {
    case COUNT:
      if (aggCall.isDistinct()) {
        if (aggCall.isApproximate() || config.approximateDistinctCount()) {
          if (complexMetric == null) {
            aggregation = new JsonCardinalityAggregation("cardinality", name, list);
          } else {
            aggregation = new JsonAggregation(complexMetric.getMetricType(), name,
                    complexMetric.getMetricName());
          }
          break;
        } else {
          // Gets thrown if one of the rules allows a count(distinct ...) through
          // when approximate results were not told be acceptable.
          throw new UnsupportedOperationException("Cannot push " + aggCall
              + " because an approximate count distinct is not acceptable.");
        }
      }
      if (aggCall.getArgList().size() == 1) {
        // case we have count(column) push it as count(*) where column is not null
        final JsonFilter matchNulls = new JsonSelector(only, null, null);
        final JsonFilter filterOutNulls = new JsonCompositeFilter(JsonFilter.Type.NOT, matchNulls);
        aggregation = new JsonFilteredAggregation(filterOutNulls,
            new JsonAggregation("count", name, only));
      } else {
        aggregation = new JsonAggregation("count", name, only);
      }

      break;
    case SUM:
    case SUM0:
      aggregation = new JsonAggregation(fractional ? "doubleSum" : "longSum", name, only);
      break;
    case MIN:
      aggregation = new JsonAggregation(fractional ? "doubleMin" : "longMin", name, only);
      break;
    case MAX:
      aggregation = new JsonAggregation(fractional ? "doubleMax" : "longMax", name, only);
      break;
    default:
      throw new AssertionError("unknown aggregate " + aggCall);
    }

    // Check for filters
    if (aggCall.hasFilter()) {
      RexNode filterNode = projects.get(aggCall.filterArg);
      JsonFilter druidFilter = translator.toDruidFilters(projects.get(aggCall.filterArg));
      Preconditions.checkNotNull(
          druidFilter, DateTimeStringUtils.format("Druid Filter is null instead of [%s]",
              filterNode));
      aggregation = new JsonFilteredAggregation(druidFilter, aggregation);
    }

    return aggregation;
  }

  public JsonPostAggregation getJsonPostAggregation(String name, RexNode rexNode, RelNode rel) {
    if (rexNode instanceof RexCall) {
      List<JsonPostAggregation> fields = new ArrayList<>();
      for (RexNode ele : ((RexCall) rexNode).getOperands()) {
        JsonPostAggregation field = getJsonPostAggregation("", ele, rel);
        if (field == null) {
          throw new RuntimeException("Unchecked types that cannot be parsed as Post Aggregator");
        }
        fields.add(field);
      }
      switch (rexNode.getKind()) {
      case PLUS:
        return new JsonArithmetic(name, "+", fields, null);
      case MINUS:
        return new JsonArithmetic(name, "-", fields, null);
      case DIVIDE:
        return new JsonArithmetic(name, "quotient", fields, null);
      case TIMES:
        return new JsonArithmetic(name, "*", fields, null);
      case CAST:
        return getJsonPostAggregation(name, ((RexCall) rexNode).getOperands().get(0),
            rel);
      default:
      }
    } else if (rexNode instanceof RexInputRef) {
      // Subtract only number of grouping columns as offset because for now only Aggregates
      // without grouping sets (i.e. indicator columns size is zero) are allowed to pushed
      // in Druid Query.
      Integer indexSkipGroup = ((RexInputRef) rexNode).getIndex()
          - ((Aggregate) rel).getGroupCount();
      AggregateCall aggCall = ((Aggregate) rel).getAggCallList().get(indexSkipGroup);
      // Use either the hyper unique estimator, or the theta sketch one.
      // Hyper unique is used by default.
      if (aggCall.isDistinct()
          && aggCall.getAggregation().getKind() == SqlKind.COUNT) {
        final String fieldName = rel.getRowType().getFieldNames()
                .get(((RexInputRef) rexNode).getIndex());

        List<String> fieldNames = ((Aggregate) rel).getInput().getRowType().getFieldNames();
        String complexName = fieldNames.get(aggCall.getArgList().get(0));
        ComplexMetric metric = druidTable.resolveComplexMetric(complexName, aggCall);

        if (metric != null) {
          switch (metric.getDruidType()) {
          case THETA_SKETCH:
            return new JsonThetaSketchEstimate("", fieldName);
          case HYPER_UNIQUE:
            return new JsonHyperUniqueCardinality("", fieldName);
          default:
            throw new AssertionError("Can not translate complex metric type: "
                    + metric.getDruidType());
          }
        }
        // Count distinct on a non-complex column.
        return new JsonHyperUniqueCardinality("", fieldName);
      }
      return new JsonFieldAccessor("",
          rel.getRowType().getFieldNames().get(((RexInputRef) rexNode).getIndex()));
    } else if (rexNode instanceof RexLiteral) {
      // Druid constant post aggregator only supports numeric value for now.
      // (http://druid.io/docs/0.10.0/querying/post-aggregations.html) Accordingly, all
      // numeric type of RexLiteral can only have BigDecimal value, so filter out unsupported
      // constant by checking the type of RexLiteral value.
      if (((RexLiteral) rexNode).getValue3() instanceof BigDecimal) {
        return new JsonConstant("",
            ((BigDecimal) ((RexLiteral) rexNode).getValue3()).doubleValue());
      }
    }
    throw new RuntimeException("Unchecked types that cannot be parsed as Post Aggregator");
  }

  protected static void writeField(JsonGenerator generator, String fieldName,
      Object o) throws IOException {
    generator.writeFieldName(fieldName);
    writeObject(generator, o);
  }

  protected static void writeFieldIf(JsonGenerator generator, String fieldName,
      Object o) throws IOException {
    if (o != null) {
      writeField(generator, fieldName, o);
    }
  }

  protected static void writeArray(JsonGenerator generator, List<?> elements)
      throws IOException {
    generator.writeStartArray();
    for (Object o : elements) {
      writeObject(generator, o);
    }
    generator.writeEndArray();
  }

  protected static void writeObject(JsonGenerator generator, Object o)
      throws IOException {
    if (o instanceof String) {
      String s = (String) o;
      generator.writeString(s);
    } else if (o instanceof Interval) {
      generator.writeString(o.toString());
    } else if (o instanceof Integer) {
      Integer i = (Integer) o;
      generator.writeNumber(i);
    } else if (o instanceof List) {
      writeArray(generator, (List<?>) o);
    } else if (o instanceof Json) {
      ((Json) o).write(generator);
    } else {
      throw new AssertionError("not a json object: " + o);
    }
  }

  /** Generates a JSON string to query metadata about a data source. */
  static String metadataQuery(String dataSourceName,
      List<Interval> intervals) {
    final StringWriter sw = new StringWriter();
    final JsonFactory factory = new JsonFactory();
    try {
      final JsonGenerator generator = factory.createGenerator(sw);
      generator.writeStartObject();
      generator.writeStringField("queryType", "segmentMetadata");
      generator.writeStringField("dataSource", dataSourceName);
      generator.writeBooleanField("merge", true);
      generator.writeBooleanField("lenientAggregatorMerge", true);
      generator.writeArrayFieldStart("analysisTypes");
      generator.writeString("aggregators");
      generator.writeEndArray();
      writeFieldIf(generator, "intervals", intervals);
      generator.writeEndObject();
      generator.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return sw.toString();
  }

  /** Druid query specification. */
  public static class QuerySpec {
    final QueryType queryType;
    final String queryString;
    final List<String> fieldNames;

    QuerySpec(QueryType queryType, String queryString,
        List<String> fieldNames) {
      this.queryType = Preconditions.checkNotNull(queryType);
      this.queryString = Preconditions.checkNotNull(queryString);
      this.fieldNames = ImmutableList.copyOf(fieldNames);
    }

    @Override public int hashCode() {
      return Objects.hash(queryType, queryString, fieldNames);
    }

    @Override public boolean equals(Object obj) {
      return obj == this
          || obj instanceof QuerySpec
          && queryType == ((QuerySpec) obj).queryType
          && queryString.equals(((QuerySpec) obj).queryString)
          && fieldNames.equals(((QuerySpec) obj).fieldNames);
    }

    @Override public String toString() {
      return "{queryType: " + queryType
          + ", queryString: " + queryString
          + ", fieldNames: " + fieldNames + "}";
    }

    public String getQueryString(String pagingIdentifier, int offset) {
      if (pagingIdentifier == null) {
        return queryString;
      }
      return queryString.replace("\"threshold\":",
          "\"pagingIdentifiers\":{\"" + pagingIdentifier + "\":" + offset
              + "},\"threshold\":");
    }
  }

  /** Translates scalar expressions to Druid field references. */
  @VisibleForTesting
  protected static class Translator {
    final List<String> dimensions = new ArrayList<>();
    final List<String> metrics = new ArrayList<>();
    final DruidTable druidTable;
    final RelDataType rowType;
    final String timeZone;
    final DruidQuery druidQuery;
    final SimpleDateFormat dateFormatter;

    Translator(DruidTable druidTable, RelDataType rowType, String timeZone, DruidQuery druidQuery) {
      this.druidTable = druidTable;
      this.rowType = rowType;
      this.druidQuery = druidQuery;
      for (RelDataTypeField f : rowType.getFieldList()) {
        final String fieldName = f.getName();
        if (druidTable.isMetric(fieldName)) {
          metrics.add(fieldName);
        } else if (!druidTable.timestampFieldName.equals(fieldName)
            && !DruidTable.DEFAULT_TIMESTAMP_COLUMN.equals(fieldName)) {
          dimensions.add(fieldName);
        }
      }
      this.timeZone = timeZone;
      this.dateFormatter = new SimpleDateFormat(TimeExtractionFunction.ISO_TIME_FORMAT,
          Locale.ROOT);
      if (timeZone != null) {
        this.dateFormatter.setTimeZone(TimeZone.getTimeZone(timeZone));
      }
    }

    protected void clearFieldNameLists() {
      dimensions.clear();
      metrics.clear();
    }

    @SuppressWarnings("incomplete-switch")
    /**
     * formatDateString is used to format timestamp values to druid format using
     * {@link DruidQuery.Translator#dateFormatter}. This is needed when pushing timestamp
     * comparisons to druid using TimeFormatExtractionFunction that returns a string value.
     */
    String translate(RexNode e, boolean set, boolean formatDateString) {
      int index = -1;
      switch (e.getKind()) {
      case INPUT_REF:
        final RexInputRef ref = (RexInputRef) e;
        index = ref.getIndex();
        break;
      case CAST:
        return tr(e, 0, set, formatDateString);
      case LITERAL:
        final RexLiteral rexLiteral = (RexLiteral) e;
        if (!formatDateString) {
          return Objects.toString(rexLiteral.getValue3());
        } else {
          // Case when we are passing to druid as an extractionFunction
          // Need to format the timestamp String in druid format.
          TimestampString timestampString = DruidDateTimeUtils
              .literalValue(e, TimeZone.getTimeZone(timeZone));
          if (timestampString == null) {
            throw new AssertionError(
                "Cannot translate Literal" + e + " of type "
                    + rexLiteral.getTypeName() + " to TimestampString");
          }
          return dateFormatter.format(timestampString.getMillisSinceEpoch());
        }
      case FLOOR:
      case EXTRACT:
        final RexCall call = (RexCall) e;
        assert DruidDateTimeUtils.extractGranularity(call) != null;
        index = RelOptUtil.InputFinder.bits(e).asList().get(0);
        break;
      case IS_TRUE:
        return ""; // the fieldName for which this is the filter will be added separately
      }
      if (index == -1) {
        return null;
      }
      final String fieldName = rowType.getFieldList().get(index).getName();
      if (set) {
        if (druidTable.metricFieldNames.contains(fieldName)) {
          metrics.add(fieldName);
        } else if (!druidTable.timestampFieldName.equals(fieldName)
            && !DruidTable.DEFAULT_TIMESTAMP_COLUMN.equals(fieldName)) {
          dimensions.add(fieldName);
        }
      }
      return fieldName;
    }

    @Nullable
    private JsonFilter toSimpleDruidFilter(RexNode e) {
      final RexCall call;
      if (e.isAlwaysTrue()) {
        return JsonExpressionFilter.alwaysTrue();
      }
      if (e.isAlwaysFalse()) {
        return JsonExpressionFilter.alwaysFalse();
      }
      switch (e.getKind()) {
      case EQUALS:
      case NOT_EQUALS:
      case GREATER_THAN:
      case GREATER_THAN_OR_EQUAL:
      case LESS_THAN:
      case LESS_THAN_OR_EQUAL:
      case IN:
      case BETWEEN:
      case IS_NULL:
      case IS_NOT_NULL:
        call = (RexCall) e;
        int posRef;
        int posConstant;
        if (call.getOperands().size() == 1) { // IS NULL and IS NOT NULL
          posRef = 0;
          posConstant = -1;
        } else if (RexUtil.isConstant(call.getOperands().get(1))) {
          posRef = 0;
          posConstant = 1;
        } else if (RexUtil.isConstant(call.getOperands().get(0))) {
          posRef = 1;
          posConstant = 0;
        } else {
          return null;
        }
        final boolean constantIsNumeric;
        final RexNode posRefNode = call.getOperands().get(posRef);
        if (posConstant != -1) {
          final RexNode constantNode =  call.getOperands().get(posConstant);
          constantIsNumeric = constantNode.getType().getFamily() == SqlTypeFamily.NUMERIC;
        } else {
          constantIsNumeric = false;
        }

        final boolean numeric = posRefNode.getType().getFamily() == SqlTypeFamily.NUMERIC
            || constantIsNumeric;
        boolean formatDateString = false;
        final Granularity granularity = DruidDateTimeUtils.extractGranularity(posRefNode);
        // in case no extraction the field will be omitted from the serialization
        ExtractionFunction extractionFunction = null;
        if (granularity != null) {
          switch (posRefNode.getKind()) {
          case EXTRACT:
            if (!TimeExtractionFunction.isValidTimeExtract((RexCall) posRefNode)) {
              return null;
            }
            extractionFunction =
                TimeExtractionFunction.createExtractFromGranularity(granularity, timeZone);
            break;
          case FLOOR:
            if (!TimeExtractionFunction.isValidTimeFloor((RexCall) posRefNode)) {
              return null;
            }
            extractionFunction =
                TimeExtractionFunction.createFloorFromGranularity(granularity, timeZone);
            formatDateString = true;
            break;

          }
        }
        String dimName = tr(e, posRef, formatDateString);
        if (dimName == null) {
          return null;
        }
        if (dimName.equals(DruidConnectionImpl.DEFAULT_RESPONSE_TIMESTAMP_COLUMN)) {
          // We need to use Druid default column name to refer to the time dimension in a filter
          dimName = DruidTable.DEFAULT_TIMESTAMP_COLUMN;
        }

        switch (e.getKind()) {
        case EQUALS:
          // extractionFunction should be null because if we are using an extraction function
          // we have guarantees about the format of the output and thus we can apply the
          // normal selector
          if (numeric && extractionFunction == null) {
            String constantValue = tr(e, posConstant, formatDateString);
            return new JsonBound(dimName, constantValue, false, constantValue, false,
                numeric, extractionFunction);
          }
          return new JsonSelector(dimName, tr(e, posConstant, formatDateString),
              extractionFunction);
        case NOT_EQUALS:
          // extractionFunction should be null because if we are using an extraction function
          // we have guarantees about the format of the output and thus we can apply the
          // normal selector
          if (numeric && extractionFunction == null) {
            String constantValue = tr(e, posConstant, formatDateString);
            return new JsonCompositeFilter(JsonFilter.Type.OR,
                new JsonBound(dimName, constantValue, true, null, false,
                    numeric, extractionFunction),
                new JsonBound(dimName, null, false, constantValue, true,
                    numeric, extractionFunction));
          }
          return new JsonCompositeFilter(JsonFilter.Type.NOT,
              new JsonSelector(dimName, tr(e, posConstant, formatDateString), extractionFunction));
        case GREATER_THAN:
          return new JsonBound(dimName, tr(e, posConstant, formatDateString),
              true, null, false, numeric, extractionFunction);
        case GREATER_THAN_OR_EQUAL:
          return new JsonBound(dimName, tr(e, posConstant, formatDateString),
              false, null, false, numeric, extractionFunction);
        case LESS_THAN:
          return new JsonBound(dimName, null, false,
              tr(e, posConstant, formatDateString), true, numeric, extractionFunction);
        case LESS_THAN_OR_EQUAL:
          return new JsonBound(dimName, null, false,
              tr(e, posConstant, formatDateString), false, numeric, extractionFunction);
        case IN:
          ImmutableList.Builder<String> listBuilder = ImmutableList.builder();
          for (RexNode rexNode: call.getOperands()) {
            if (rexNode.getKind() == SqlKind.LITERAL) {
              listBuilder.add(Objects.toString(((RexLiteral) rexNode).getValue3()));
            }
          }
          return new JsonInFilter(dimName, listBuilder.build(), extractionFunction);
        case BETWEEN:
          return new JsonBound(dimName, tr(e, 2, formatDateString), false,
              tr(e, 3, formatDateString), false, numeric, extractionFunction);
        case IS_NULL:
          return new JsonSelector(dimName, null, extractionFunction);
        case IS_NOT_NULL:
          return new JsonCompositeFilter(JsonFilter.Type.NOT,
              new JsonSelector(dimName, null, extractionFunction));
        default:
          return null;
        }
      default:
        return null;
      }
    }

    @Nullable
    private JsonFilter toDruidFilters(final RexNode rexNode) {
      switch (rexNode.getKind()) {
      case IS_TRUE:
      case IS_NOT_FALSE:
        return toDruidFilters(Iterables.getOnlyElement(((RexCall) rexNode).getOperands()));
      case IS_NOT_TRUE:
      case IS_FALSE:
        final JsonFilter simpleFilter = toDruidFilters(Iterables
            .getOnlyElement(((RexCall) rexNode).getOperands()));
        return simpleFilter != null ? new JsonCompositeFilter(JsonFilter.Type.NOT, simpleFilter)
            : simpleFilter;
      case AND:
      case OR:
      case NOT:
        final RexCall call = (RexCall) rexNode;
        final List<JsonFilter> jsonFilters = Lists.newArrayList();
        for (final RexNode e : call.getOperands()) {
          final JsonFilter druidFilter = toDruidFilters(e);
          if (druidFilter == null) {
            return null;
          }
          jsonFilters.add(druidFilter);
        }
        return new JsonCompositeFilter(JsonFilter.Type.valueOf(rexNode.getKind().name()),
            jsonFilters);
      default:
        final JsonFilter simpleLeafFilter;
        if (isValidSimpleFilter(rexNode, false)) {
          simpleLeafFilter = toSimpleDruidFilter(rexNode);
        } else {
          simpleLeafFilter = null;
        }
        return simpleLeafFilter == null ? toDruidExpressionFilter(rexNode) : simpleLeafFilter;
      }
    }

    @Nullable
    private JsonFilter toDruidExpressionFilter(RexNode rexNode) {
      final String expression = DruidExpressions.toDruidExpression(rexNode, rowType, druidQuery);
      return expression == null ? null : new JsonExpressionFilter(expression);
    }

    private String tr(RexNode call, int index, boolean formatDateString) {
      return tr(call, index, false, formatDateString);
    }

    private String tr(RexNode call, int index, boolean set, boolean formatDateString) {
      return translate(((RexCall) call).getOperands().get(index), set, formatDateString);
    }
  }

  /** Interpreter node that executes a Druid query and sends the results to a
   * {@link Sink}. */
  private static class DruidQueryNode implements Node {
    private final Sink sink;
    private final DruidQuery query;
    private final QuerySpec querySpec;

    DruidQueryNode(Interpreter interpreter, DruidQuery query) {
      this.query = query;
      this.sink = interpreter.sink(query);
      this.querySpec = query.getQuerySpec();
      Hook.QUERY_PLAN.run(querySpec);
    }

    public void run() throws InterruptedException {
      final List<ColumnMetaData.Rep> fieldTypes = new ArrayList<>();
      for (RelDataTypeField field : query.getRowType().getFieldList()) {
        fieldTypes.add(getPrimitive(field));
      }
      final DruidConnectionImpl connection =
          new DruidConnectionImpl(query.druidTable.schema.url,
              query.druidTable.schema.coordinatorUrl);
      final boolean limitQuery = containsLimit(querySpec);
      final DruidConnectionImpl.Page page = new DruidConnectionImpl.Page();
      do {
        final String queryString =
            querySpec.getQueryString(page.pagingIdentifier, page.offset);
        connection.request(querySpec.queryType, queryString, sink,
            querySpec.fieldNames, fieldTypes, page);
      } while (!limitQuery
          && page.pagingIdentifier != null
          && page.totalRowCount > 0);
    }

    private static boolean containsLimit(QuerySpec querySpec) {
      return querySpec.queryString.contains("\"context\":{\""
          + DRUID_QUERY_FETCH + "\":true");
    }

    private ColumnMetaData.Rep getPrimitive(RelDataTypeField field) {
      switch (field.getType().getSqlTypeName()) {
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return ColumnMetaData.Rep.JAVA_SQL_TIMESTAMP;
      case BIGINT:
        return ColumnMetaData.Rep.LONG;
      case INTEGER:
        return ColumnMetaData.Rep.INTEGER;
      case SMALLINT:
        return ColumnMetaData.Rep.SHORT;
      case TINYINT:
        return ColumnMetaData.Rep.BYTE;
      case REAL:
        return ColumnMetaData.Rep.FLOAT;
      case DOUBLE:
      case FLOAT:
        return ColumnMetaData.Rep.DOUBLE;
      default:
        return null;
      }
    }
  }

  /** Object that knows how to write itself to a
   * {@link com.fasterxml.jackson.core.JsonGenerator}. */
  public interface Json {
    void write(JsonGenerator generator) throws IOException;
  }

  /** Aggregation element of a Druid "groupBy" or "topN" query. */
  private static class JsonAggregation implements Json {
    final String type;
    final String name;
    final String fieldName;

    private JsonAggregation(String type, String name, String fieldName) {
      this.type = type;
      this.name = name;
      this.fieldName = fieldName;
    }

    public void write(JsonGenerator generator) throws IOException {
      generator.writeStartObject();
      generator.writeStringField("type", type);
      generator.writeStringField("name", name);
      writeFieldIf(generator, "fieldName", fieldName);
      generator.writeEndObject();
    }
  }

  /** Collation element of a Druid "groupBy" query. */
  private static class JsonLimit implements Json {
    final String type;
    final Integer limit;
    final ImmutableList<JsonCollation> collations;

    private JsonLimit(String type, Integer limit, ImmutableList<JsonCollation> collations) {
      this.type = type;
      this.limit = limit;
      this.collations = collations;
    }

    public void write(JsonGenerator generator) throws IOException {
      generator.writeStartObject();
      generator.writeStringField("type", type);
      writeFieldIf(generator, "limit", limit);
      writeFieldIf(generator, "columns", collations);
      generator.writeEndObject();
    }
  }

  /** Collation element of a Druid "groupBy" query. */
  private static class JsonCollation implements Json {
    final String dimension;
    final String direction;
    final String dimensionOrder;

    private JsonCollation(String dimension, String direction, String dimensionOrder) {
      this.dimension = dimension;
      this.direction = direction;
      this.dimensionOrder = dimensionOrder;
    }

    public void write(JsonGenerator generator) throws IOException {
      generator.writeStartObject();
      generator.writeStringField("dimension", dimension);
      writeFieldIf(generator, "direction", direction);
      writeFieldIf(generator, "dimensionOrder", dimensionOrder);
      generator.writeEndObject();
    }
  }

  /** Aggregation element that calls the "cardinality" function. */
  private static class JsonCardinalityAggregation extends JsonAggregation {
    final List<String> fieldNames;

    private JsonCardinalityAggregation(String type, String name,
        List<String> fieldNames) {
      super(type, name, null);
      this.fieldNames = fieldNames;
    }

    public void write(JsonGenerator generator) throws IOException {
      generator.writeStartObject();
      generator.writeStringField("type", type);
      generator.writeStringField("name", name);
      writeFieldIf(generator, "fieldNames", fieldNames);
      generator.writeEndObject();
    }
  }

  /** Aggregation element that contains a filter */
  private static class JsonFilteredAggregation extends JsonAggregation {
    final JsonFilter filter;
    final JsonAggregation aggregation;

    private JsonFilteredAggregation(JsonFilter filter, JsonAggregation aggregation) {
      // Filtered aggregations don't use the "name" and "fieldName" fields directly,
      // but rather use the ones defined in their "aggregation" field.
      super("filtered", aggregation.name, aggregation.fieldName);
      this.filter = filter;
      this.aggregation = aggregation;
      // The aggregation cannot be a JsonFilteredAggregation
      assert !(aggregation instanceof JsonFilteredAggregation);
    }

    @Override public void write(JsonGenerator generator) throws IOException {
      generator.writeStartObject();
      generator.writeStringField("type", type);
      writeField(generator, "filter", filter);
      writeField(generator, "aggregator", aggregation);
      generator.writeEndObject();
    }
  }

  /** Filter element of a Druid "groupBy" or "topN" query. */
  private abstract static class JsonFilter implements Json {
    /**
     * Supported filter types
     * */
    protected enum Type {
      AND,
      OR,
      NOT,
      SELECTOR,
      IN,
      BOUND,
      EXPRESSION;

      public String lowercase() {
        return name().toLowerCase(Locale.ROOT);
      }
    }

    final Type type;

    private JsonFilter(Type type) {
      this.type = type;
    }
  }

  /**
   * Druid Expression filter.
   */
  private static class JsonExpressionFilter extends JsonFilter {
    private final String expression;

    private JsonExpressionFilter(String expression) {
      super(Type.EXPRESSION);
      this.expression = Preconditions.checkNotNull(expression);
    }

    @Override public void write(JsonGenerator generator) throws IOException {
      generator.writeStartObject();
      generator.writeStringField("type", type.lowercase());
      generator.writeStringField("expression", expression);
      generator.writeEndObject();
    }

    /** We need to push to Druid an expression that always evaluates to true. */
    public static final JsonExpressionFilter alwaysTrue() {
      return new JsonExpressionFilter("1 == 1");
    }

    /** We need to push to Druid an expression that always evaluates to false. */
    public static final JsonExpressionFilter alwaysFalse() {
      return new JsonExpressionFilter("1 == 2");
    }
  }


  /** Equality filter. */
  private static class JsonSelector extends JsonFilter {
    private final String dimension;
    private final String value;
    private final ExtractionFunction extractionFunction;

    private JsonSelector(String dimension, String value,
        ExtractionFunction extractionFunction) {
      super(Type.SELECTOR);
      this.dimension = dimension;
      this.value = value;
      this.extractionFunction = extractionFunction;
    }

    public void write(JsonGenerator generator) throws IOException {
      generator.writeStartObject();
      generator.writeStringField("type", type.lowercase());
      generator.writeStringField("dimension", dimension);
      generator.writeStringField("value", value);
      writeFieldIf(generator, "extractionFn", extractionFunction);
      generator.writeEndObject();
    }
  }

  /** Bound filter. */
  @VisibleForTesting
  protected static class JsonBound extends JsonFilter {
    private final String dimension;
    private final String lower;
    private final boolean lowerStrict;
    private final String upper;
    private final boolean upperStrict;
    private final boolean alphaNumeric;
    private final ExtractionFunction extractionFunction;

    private JsonBound(String dimension, String lower,
        boolean lowerStrict, String upper, boolean upperStrict,
        boolean alphaNumeric, ExtractionFunction extractionFunction) {
      super(Type.BOUND);
      this.dimension = dimension;
      this.lower = lower;
      this.lowerStrict = lowerStrict;
      this.upper = upper;
      this.upperStrict = upperStrict;
      this.alphaNumeric = alphaNumeric;
      this.extractionFunction = extractionFunction;
    }

    public void write(JsonGenerator generator) throws IOException {
      generator.writeStartObject();
      generator.writeStringField("type", type.lowercase());
      generator.writeStringField("dimension", dimension);
      if (lower != null) {
        generator.writeStringField("lower", lower);
        generator.writeBooleanField("lowerStrict", lowerStrict);
      }
      if (upper != null) {
        generator.writeStringField("upper", upper);
        generator.writeBooleanField("upperStrict", upperStrict);
      }
      if (alphaNumeric) {
        generator.writeStringField("ordering", "numeric");
      } else {
        generator.writeStringField("ordering", "lexicographic");
      }
      writeFieldIf(generator, "extractionFn", extractionFunction);
      generator.writeEndObject();
    }
  }

  /** Filter that combines other filters using a boolean operator. */
  private static class JsonCompositeFilter extends JsonFilter {
    private final List<? extends JsonFilter> fields;

    private JsonCompositeFilter(Type type,
        Iterable<? extends JsonFilter> fields) {
      super(type);
      this.fields = ImmutableList.copyOf(fields);
    }

    private JsonCompositeFilter(Type type, JsonFilter... fields) {
      this(type, ImmutableList.copyOf(fields));
    }

    public void write(JsonGenerator generator) throws IOException {
      generator.writeStartObject();
      generator.writeStringField("type", type.lowercase());
      switch (type) {
      case NOT:
        writeField(generator, "field", fields.get(0));
        break;
      default:
        writeField(generator, "fields", fields);
      }
      generator.writeEndObject();
    }
  }

  /** IN filter. */
  protected static class JsonInFilter extends JsonFilter {
    private final String dimension;
    private final List<String> values;
    private final ExtractionFunction extractionFunction;

    private JsonInFilter(String dimension, List<String> values,
        ExtractionFunction extractionFunction) {
      super(Type.IN);
      this.dimension = dimension;
      this.values = values;
      this.extractionFunction = extractionFunction;
    }

    public void write(JsonGenerator generator) throws IOException {
      generator.writeStartObject();
      generator.writeStringField("type", type.lowercase());
      generator.writeStringField("dimension", dimension);
      writeField(generator, "values", values);
      writeFieldIf(generator, "extractionFn", extractionFunction);
      generator.writeEndObject();
    }
  }

  /** Post-Aggregator Post aggregator abstract writer */
  protected abstract static class JsonPostAggregation implements Json {
    final String type;
    String name;

    private JsonPostAggregation(String name, String type) {
      this.type = type;
      this.name = name;
    }

    // Expects all subclasses to write the EndObject item
    public void write(JsonGenerator generator) throws IOException {
      generator.writeStartObject();
      generator.writeStringField("type", type);
      generator.writeStringField("name", name);
    }

    public void setName(String name) {
      this.name = name;
    }

    public abstract JsonPostAggregation copy();
  }

  /** FieldAccessor Post aggregator writer */
  private static class JsonFieldAccessor extends JsonPostAggregation {
    final String fieldName;

    private JsonFieldAccessor(String name, String fieldName) {
      super(name, "fieldAccess");
      this.fieldName = fieldName;
    }

    public void write(JsonGenerator generator) throws IOException {
      super.write(generator);
      generator.writeStringField("fieldName", fieldName);
      generator.writeEndObject();
    }

    /**
     * Leaf node in Post-aggs Json Tree, return an identical leaf node.
     */

    public JsonPostAggregation copy() {
      return new JsonFieldAccessor(this.name, this.fieldName);
    }
  }

  /** Constant Post aggregator writer */
  private static class JsonConstant extends JsonPostAggregation {
    final double value;

    private JsonConstant(String name, double value) {
      super(name, "constant");
      this.value = value;
    }

    public void write(JsonGenerator generator) throws IOException {
      super.write(generator);
      generator.writeNumberField("value", value);
      generator.writeEndObject();
    }

    /**
     * Leaf node in Post-aggs Json Tree, return an identical leaf node.
     */

    public JsonPostAggregation copy() {
      return new JsonConstant(this.name, this.value);
    }
  }

  /** Greatest/Leastest Post aggregator writer */
  private static class JsonGreatestLeast extends JsonPostAggregation {
    final List<JsonPostAggregation> fields;
    final boolean fractional;
    final boolean greatest;

    private JsonGreatestLeast(String name, List<JsonPostAggregation> fields,
                              boolean fractional, boolean greatest) {
      super(name, greatest ? (fractional ? "doubleGreatest" : "longGreatest")
          : (fractional ? "doubleLeast" : "longLeast"));
      this.fields = fields;
      this.fractional = fractional;
      this.greatest = greatest;
    }

    public void write(JsonGenerator generator) throws IOException {
      super.write(generator);
      writeFieldIf(generator, "fields", fields);
      generator.writeEndObject();
    }

    /**
     * Non-leaf node in Post-aggs Json Tree, recursively copy the leaf node.
     */

    public JsonPostAggregation copy() {
      ImmutableList.Builder<JsonPostAggregation> builder = ImmutableList.builder();
      for (JsonPostAggregation field : fields) {
        builder.add(field.copy());
      }
      return new JsonGreatestLeast(name, builder.build(), fractional, greatest);
    }
  }

  /** Arithmetic Post aggregator writer */
  private static class JsonArithmetic extends JsonPostAggregation {
    final String fn;
    final List<JsonPostAggregation> fields;
    final String ordering;

    private JsonArithmetic(String name, String fn, List<JsonPostAggregation> fields,
                           String ordering) {
      super(name, "arithmetic");
      this.fn = fn;
      this.fields = fields;
      this.ordering = ordering;
    }

    public void write(JsonGenerator generator) throws IOException {
      super.write(generator);
      generator.writeStringField("fn", fn);
      writeFieldIf(generator, "fields", fields);
      writeFieldIf(generator, "ordering", ordering);
      generator.writeEndObject();
    }

    /**
     * Non-leaf node in Post-aggs Json Tree, recursively copy the leaf node.
     */

    public JsonPostAggregation copy() {
      ImmutableList.Builder<JsonPostAggregation> builder = ImmutableList.builder();
      for (JsonPostAggregation field : fields) {
        builder.add(field.copy());
      }
      return new JsonArithmetic(name, fn, builder.build(), ordering);
    }
  }

  /** HyperUnique Cardinality Post aggregator writer */
  private static class JsonHyperUniqueCardinality extends JsonPostAggregation {
    final String fieldName;

    private JsonHyperUniqueCardinality(String name, String fieldName) {
      super(name, "hyperUniqueCardinality");
      this.fieldName = fieldName;
    }

    public void write(JsonGenerator generator) throws IOException {
      super.write(generator);
      generator.writeStringField("fieldName", fieldName);
      generator.writeEndObject();
    }

    /**
     * Leaf node in Post-aggs Json Tree, return an identical leaf node.
     */

    public JsonPostAggregation copy() {
      return new JsonHyperUniqueCardinality(this.name, this.fieldName);
    }
  }

  /** Theta Sketch Estimator for Post aggregation */
  private static class JsonThetaSketchEstimate extends JsonPostAggregation {
    final String fieldName;

    private JsonThetaSketchEstimate(String name, String fieldName) {
      super(name, "thetaSketchEstimate");
      this.fieldName = fieldName;
    }

    @Override public JsonPostAggregation copy() {
      return new JsonThetaSketchEstimate(name, fieldName);
    }

    @Override public void write(JsonGenerator generator) throws IOException {
      super.write(generator);
      // Druid spec for ThetaSketchEstimate requires a field accessor
      writeField(generator, "field", new JsonFieldAccessor("", fieldName));
      generator.writeEndObject();
    }
  }

  /**
   * @return index of the timestamp ref or -1 if not present
   */
  public int getTimestampFieldIndex() {
    for (int i = 0; i < this.getRowType().getFieldCount(); i++) {
      if (this.druidTable.timestampFieldName.equals(
          this.getRowType().getFieldList().get(i).getName())) {
        return i;
      }
    }
    return -1;
  }
}

// End DruidQuery.java
