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
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.joda.time.Interval;

import java.io.IOException;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

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
          .add(new DirectOperatorConversion(SqlStdOperatorTable.SIN, "sin"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.COS, "cos"))
          .add(new DirectOperatorConversion(SqlStdOperatorTable.TAN, "tan"))
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

  /**
   * @param rexNode    leaf Input Ref to Druid Column
   * @param rowType    row type
   * @param druidQuery druid query
   *
   * @return {@link Pair} of Column name and Extraction Function on the top of the input ref or
   * {@link Pair of(null, null)} when can not translate to valid Druid column
   */
  public static Pair<String, ExtractionFunction> toDruidColumn(RexNode rexNode,
      RelDataType rowType, DruidQuery druidQuery
  ) {
    final String columnName;
    final ExtractionFunction extractionFunction;
    final Granularity granularity;
    switch (rexNode.getKind()) {
    case INPUT_REF:
      columnName = extractColumnName(rexNode, rowType, druidQuery);
      //@TODO we can remove this ugly check by treating druid time columns as LONG
      if (rexNode.getType().getFamily() == SqlTypeFamily.DATE
          || rexNode.getType().getFamily() == SqlTypeFamily.TIMESTAMP) {
        extractionFunction = TimeExtractionFunction
            .createDefault(druidQuery.getConnectionConfig().timeZone());
      } else {
        extractionFunction = null;
      }
      break;
    case EXTRACT:
      granularity = DruidDateTimeUtils.extractGranularity(rexNode);
      if (granularity == null) {
        // unknown Granularity
        return Pair.of(null, null);
      }
      if (!TimeExtractionFunction.isValidTimeExtract((RexCall) rexNode)) {
        return Pair.of(null, null);
      }
      extractionFunction =
          TimeExtractionFunction.createExtractFromGranularity(granularity,
              druidQuery.getConnectionConfig().timeZone()
          );
      columnName =
          extractColumnName(((RexCall) rexNode).getOperands().get(1), rowType, druidQuery);

      break;
    case FLOOR:
      granularity = DruidDateTimeUtils.extractGranularity(rexNode);
      if (granularity == null) {
        // unknown Granularity
        return Pair.of(null, null);
      }
      if (!TimeExtractionFunction.isValidTimeFloor((RexCall) rexNode)) {
        return Pair.of(null, null);
      }
      extractionFunction =
          TimeExtractionFunction
              .createFloorFromGranularity(granularity, druidQuery.getConnectionConfig().timeZone());
      columnName =
          extractColumnName(((RexCall) rexNode).getOperands().get(0), rowType, druidQuery);
      break;
    case CAST:
      // CASE we have a cast over RexRef. Check that cast is valid
      if (!isValidLeafCast(rexNode)) {
        return Pair.of(null, null);
      }
      columnName =
          extractColumnName(((RexCall) rexNode).getOperands().get(0), rowType, druidQuery);
      // CASE CAST to TIME/DATE need to make sure that we have valid extraction fn
      final SqlTypeName toTypeName = rexNode.getType().getSqlTypeName();
      if (toTypeName.getFamily() == SqlTypeFamily.TIMESTAMP
          || toTypeName.getFamily() == SqlTypeFamily.DATETIME) {
        extractionFunction = TimeExtractionFunction.translateCastToTimeExtract(rexNode,
            TimeZone.getTimeZone(druidQuery.getConnectionConfig().timeZone())
        );
        if (extractionFunction == null) {
          // no extraction Function means cast is not valid thus bail out
          return Pair.of(null, null);
        }
      } else {
        extractionFunction = null;
      }
      break;
    default:
      return Pair.of(null, null);
    }
    return Pair.of(columnName, extractionFunction);
  }

  private static boolean isValidLeafCast(RexNode rexNode) {
    assert rexNode.isA(SqlKind.CAST);
    final RexNode input = ((RexCall) rexNode).getOperands().get(0);
    if (!input.isA(SqlKind.INPUT_REF)) {
      // it is not a leaf cast don't bother going further.
      return false;
    }
    final SqlTypeName toTypeName = rexNode.getType().getSqlTypeName();
    if (toTypeName.getFamily() == SqlTypeFamily.CHARACTER) {
      // CAST of input to character type
      return true;
    }
    if (toTypeName.getFamily() == SqlTypeFamily.NUMERIC) {
      // CAST of input to numeric type, it is part of a bounded comparison
      return true;
    }
    if (toTypeName.getFamily() == SqlTypeFamily.TIMESTAMP
        || toTypeName.getFamily() == SqlTypeFamily.DATETIME) {
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

  /**
   * @param rexNode Druid input ref node
   * @param rowType rowType
   * @param query Druid Query
   *
   * @return Druid column name or null when not possible to translate.
   */
  @Nullable
  public static String extractColumnName(RexNode rexNode, RelDataType rowType, DruidQuery query) {

    if (rexNode.getKind() == SqlKind.INPUT_REF) {
      final RexInputRef ref = (RexInputRef) rexNode;
      final String columnName = rowType.getFieldNames().get(ref.getIndex());
      if (columnName == null) {
        return null;
      }
      //this a nasty hack since calcite has this un-direct renaming of timestamp to __time
      if (query.getDruidTable().timestampFieldName.equals(columnName)) {
        return DruidTable.DEFAULT_TIMESTAMP_COLUMN;
      }
      return columnName;
    }
    return null;
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
          final DruidJsonFilter druidJsonFilter = DruidJsonFilter
              .toDruidFilters(filter.getCondition(), filter.getInput().getRowType(), this);
          if (druidJsonFilter == null) {
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

    Filter filterRel = null;
    if (i < rels.size() && rels.get(i) instanceof Filter) {
      filterRel = (Filter) rels.get(i++);
    }

    Project project = null;
    if (i < rels.size() && rels.get(i) instanceof Project) {
      project = (Project) rels.get(i++);
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

    return getQuery(rowType, filterRel, project, groupSet, aggCalls, aggNames,
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

  /**
   * Translates Filter rel to Druid Filter Json object if possible.
   * Currently Filter rel input has to be Druid Table scan
   *
   * @param filterRel input filter rel
   * @param druidQuery Druid query
   *
   * @return DruidJson Filter or null if can not translate one of filters
   */
  @Nullable
  private static DruidJsonFilter computeFilter(@Nullable Filter filterRel,
      DruidQuery druidQuery
  ) {
    if (filterRel == null) {
      return null;
    }
    final RexNode filter = filterRel.getCondition();
    final RelDataType inputRowType = filterRel.getInput().getRowType();
    if (filter != null) {
      return DruidJsonFilter.toDruidFilters(filter, inputRowType, druidQuery);
    }
    return null;
  }

  /**
   * Translates list of projects to Druid Column names and Virtual Columns if any
   * We can not use {@link Pair#zip(Object[], Object[])}, since size can be different
   *
   * @param projectRel       Project Rel
   *
   * @param druidQuery Druid query
   *
   * @return Pair of list of Druid Columns and Expression Virtual Columns or null when can not
   * translate one of the projects.
   */
  @Nullable
  protected static Pair<List<String>, List<VirtualColumn>> computeProjectAsScan(
      @Nullable Project projectRel, RelDataType inputRowType, DruidQuery druidQuery
  ) {
    if (projectRel == null) {
      return null;
    }
    final Set<String> usedFieldNames = new HashSet<>();
    final ImmutableList.Builder<VirtualColumn> virtualColumnsBuilder = ImmutableList.builder();
    final ImmutableList.Builder<String> projectedColumnsBuilder = ImmutableList.builder();
    final List<RexNode> projects = projectRel.getProjects();
    for (RexNode project : projects) {
      Pair<String, ExtractionFunction> druidColumn =
          toDruidColumn(project, inputRowType, druidQuery);
      if (druidColumn.left == null || druidColumn.right != null) {
        // It is a complex project pushed as expression
        final String expression = DruidExpressions
            .toDruidExpression(project, inputRowType, druidQuery);
        if (expression == null) {
          return null;
        }
        final String virColName = SqlValidatorUtil.uniquify("vc",
            usedFieldNames, SqlValidatorUtil.EXPR_SUGGESTER
        );
        virtualColumnsBuilder.add(VirtualColumn.builder()
            .withName(virColName)
            .withExperession(expression).withType(
                DruidExpressions.EXPRESSION_TYPES.get(project.getType().getSqlTypeName()))
            .build());
        usedFieldNames.add(virColName);
        projectedColumnsBuilder.add(virColName);
      } else {
        // simple inputRef or extractable function
        projectedColumnsBuilder.add(druidColumn.left);
      }
    }
    return Pair.<List<String>, List<VirtualColumn>>of(projectedColumnsBuilder.build(),
        virtualColumnsBuilder.build()
    );
  }

  /**
   * @param projectNode Project under the Aggregates if any
   * @param groupSet ids of grouping keys as they are listed in {@code projects} list
   * @param inputRowType Input row type under the project
   * @param druidQuery Druid Query
   *
   * @return Pair of: Ordered {@link List<DimensionSpec>} containing the group by dimensions
   * and {@link List<VirtualColumn>} containing Druid virtual column projections or Null,
   * if translation is not possible. Note that the size of lists can be different.
   */
  @Nullable
  protected static Pair<List<DimensionSpec>, List<VirtualColumn>> computeProjectGroupSet(
      @Nullable Project projectNode, ImmutableBitSet groupSet,
      RelDataType inputRowType,
      DruidQuery druidQuery
  ) {
    final List<DimensionSpec> dimensionSpecList = new ArrayList<>();
    final List<VirtualColumn> virtualColumnList = new ArrayList<>();
    final Set<String> usedFieldNames = new HashSet<>();
    for (int groupKey : groupSet) {
      final DimensionSpec dimensionSpec;
      final RexNode project;
      if (projectNode == null) {
        project =  RexInputRef.of(groupKey, inputRowType);
      } else {
        project = projectNode.getProjects().get(groupKey);
      }

      Pair<String, ExtractionFunction> druidColumn =
          toDruidColumn(project, inputRowType, druidQuery);
      if (druidColumn.left != null && druidColumn.right == null) {
        //SIMPLE INPUT REF
        dimensionSpec = new DefaultDimensionSpec(druidColumn.left, druidColumn.left,
            DruidExpressions.EXPRESSION_TYPES.get(project.getType().getSqlTypeName())
        );
        usedFieldNames.add(druidColumn.left);
      } else if (druidColumn.left != null && druidColumn.right != null) {
       // CASE it is an extraction Dimension
        final String columnPrefix;
        //@TODO Remove it! Switch statement is not really needed it is here to make tests pass.
        if (project.getKind() == SqlKind.EXTRACT) {
          columnPrefix =
              EXTRACT_COLUMN_NAME_PREFIX + "_" + Objects
                  .requireNonNull(DruidDateTimeUtils.extractGranularity(project)).value;
        } else if (project.getKind() == SqlKind.FLOOR) {
          columnPrefix =
              FLOOR_COLUMN_NAME_PREFIX + "_" + Objects
                  .requireNonNull(DruidDateTimeUtils.extractGranularity(project)).value;
        } else  {
          columnPrefix = "extract";
        }
        final String uniqueExtractColumnName = SqlValidatorUtil
            .uniquify(columnPrefix, usedFieldNames,
                SqlValidatorUtil.EXPR_SUGGESTER
            );
        dimensionSpec = new ExtractionDimensionSpec(druidColumn.left,
            druidColumn.right, uniqueExtractColumnName
        );
        usedFieldNames.add(uniqueExtractColumnName);
      } else {
        // CASE it is Expression
        final String expression = DruidExpressions
            .toDruidExpression(project, inputRowType, druidQuery);
        if (Strings.isNullOrEmpty(expression)) {
          return null;
        }
        final String name = SqlValidatorUtil
            .uniquify("vc", usedFieldNames,
                SqlValidatorUtil.EXPR_SUGGESTER
            );
        VirtualColumn vc = new VirtualColumn(name, expression,
            DruidExpressions.EXPRESSION_TYPES.get(project.getType().getSqlTypeName())
        );
        virtualColumnList.add(vc);
        dimensionSpec = new DefaultDimensionSpec(name, name,
            DruidExpressions.EXPRESSION_TYPES.get(project.getType().getSqlTypeName())
        );
        usedFieldNames.add(name);

      }

      dimensionSpecList.add(dimensionSpec);
    }
    return Pair.of(dimensionSpecList, virtualColumnList);
  }

  /**
   * Translates Aggregators Calls to Druid Json Aggregators when possible.
   *
   * @param aggCalls List of Agg Calls to translate
   * @param aggNames Lit of Agg names
   * @param project Input project under the Agg Calls, if null means we have TableScan->Agg
   * @param druidQuery Druid Query Rel
   *
   * @return List of Valid Druid Json Aggregate or null if any of the aggregates is not supported
   */
  @Nullable
  protected static List<JsonAggregation> computeDruidJsonAgg(List<AggregateCall> aggCalls,
      List<String> aggNames, @Nullable Project project,
      DruidQuery druidQuery
  ) {
    final List<JsonAggregation> aggregations = new ArrayList<>();
    for (Pair<AggregateCall, String> agg : Pair.zip(aggCalls, aggNames)) {
      final String fieldName;
      final String expression;
      final  AggregateCall aggCall = agg.left;
      final RexNode filterNode;
      // Type check First
      final RelDataType type = aggCall.getType();
      final SqlTypeName sqlTypeName = type.getSqlTypeName();
      final boolean isNotAcceptedType;
      if (SqlTypeFamily.APPROXIMATE_NUMERIC.getTypeNames().contains(sqlTypeName)
          || SqlTypeFamily.INTEGER.getTypeNames().contains(sqlTypeName)) {
        isNotAcceptedType = false;
      } else if (SqlTypeFamily.EXACT_NUMERIC.getTypeNames().contains(sqlTypeName) && (
          type.getScale() == 0 || druidQuery.getConnectionConfig().approximateDecimal())) {
        // Decimal, If scale is zero or we allow approximating decimal, we can proceed
        isNotAcceptedType = false;
      } else {
        isNotAcceptedType = true;
      }
      if (isNotAcceptedType) {
        return null;
      }

      // Extract filters
      if (project != null && aggCall.hasFilter()) {
        filterNode = project.getProjects().get(aggCall.filterArg);
      } else {
        filterNode = null;
      }
      if (aggCall.getArgList().size() == 0) {
        fieldName = null;
        expression = null;
      } else {
        int index = Iterables.getOnlyElement(aggCall.getArgList());
        if (project == null) {
          fieldName = druidQuery.table.getRowType().getFieldNames().get(index);
          expression = null;
        } else {
          final RexNode rexNode = project.getProjects().get(index);
          final RelDataType inputRowType = project.getInput().getRowType();
          if (rexNode.isA(SqlKind.INPUT_REF)) {
            expression = null;
            fieldName =
                extractColumnName(rexNode, inputRowType, druidQuery);
          } else {
            expression = DruidExpressions
                .toDruidExpression(rexNode, inputRowType, druidQuery);
            if (Strings.isNullOrEmpty(expression)) {
              return null;
            }
            fieldName = null;
          }
        }
        //One should be not null and the other should be null.
        assert expression == null ^ fieldName == null;
      }

      final JsonAggregation jsonAggregation = getJsonAggregation(agg.right, agg.left, filterNode,
          fieldName, expression,
          druidQuery
      );
      if (jsonAggregation == null) {
        return null;
      }
      aggregations.add(jsonAggregation);
    }
    return aggregations;
  }

  protected QuerySpec getQuery(RelDataType rowType, Filter filter, Project project,
      ImmutableBitSet groupSet, List<AggregateCall> aggCalls, List<String> aggNames,
      List<Integer> collationIndexes, List<Direction> collationDirections,
      ImmutableBitSet numericCollationIndexes, Integer fetch, Project postProject) {
    final CalciteConnectionConfig config = getConnectionConfig();
    final QueryType queryType;
    // Handle filter
    final DruidJsonFilter jsonFilter = computeFilter(filter, this);

    // Then we handle project and aggs
    if (groupSet == null) {
      //It is Scan Query since no Grouping
      assert aggCalls == null;
      assert aggNames == null;
      assert collationIndexes == null || collationIndexes.isEmpty();
      assert collationDirections == null || collationDirections.isEmpty();
      final List<String> columnsNames;
      final List<VirtualColumn> virtualColumnList = new ArrayList<>();
      final ScanQueryBuilder scanQueryBuilder = new ScanQueryBuilder();
      if (project != null) {
        //project some fields only
        Pair<List<String>, List<VirtualColumn>> projectResult = computeProjectAsScan(
            project, project.getInput().getRowType(), this);
        columnsNames = projectResult.left;
        virtualColumnList.addAll(projectResult.right);
      } else {
        //Scan all the fields
        columnsNames = rowType.getFieldNames();
      }

      final String queryString = scanQueryBuilder.setDataSource(druidTable.dataSource)
          .setIntervals(intervals)
          .setColumns(columnsNames)
          .setVirtualColumnList(virtualColumnList)
          .setJsonFilter(jsonFilter)
          .setFetchLimit(fetch)
          .createScanQuery()
          .toQuery();
      return new QuerySpec(QueryType.SCAN, Preconditions.checkNotNull(queryString), columnsNames);
    }

    // At this Stage we have a valid Aggregate thus Query is one of Timeseries, TopN, or GroupBy
    // Handling aggregate and sort is more complex, since
    // we need to extract the conditions to know whether the query will be executed as a
    // Timeseries, TopN, or GroupBy in Druid
    assert aggCalls != null;
    assert aggNames != null;
    assert aggCalls.size() == aggNames.size();

    final List<JsonPostAggregation> postAggs = new ArrayList<>();

    final Granularity queryGranularity;
    Direction timeSeriesDirection = null;
    final JsonLimit limit;
    String topNMetricColumnName = null;
    final RelDataType aggInputRowType = table.getRowType();
    List<String> fieldNames = new ArrayList<>();
    Pair<List<DimensionSpec>, List<VirtualColumn>> projectGroupSet = computeProjectGroupSet(
        project, groupSet, aggInputRowType, this);

    final List<DimensionSpec> groupByKeyDims = projectGroupSet.left;
    final List<VirtualColumn> virtualColumnList = projectGroupSet.right;
    for (DimensionSpec dim : groupByKeyDims) {
      fieldNames.add(dim.getOutputName());
    }
    final List<JsonAggregation> aggregations = computeDruidJsonAgg(aggCalls, aggNames, project,
        this
    );
    for (JsonAggregation jsonAgg : aggregations) {
      fieldNames.add(jsonAgg.name);
    }

    //Then we handle projects after aggregates as Druid Post Aggregates
    if (postProject != null) {
      final ImmutableList.Builder<String> postProjectDimListBuilder = ImmutableList.builder();
      for (Pair<RexNode, String> pair : postProject.getNamedProjects()) {
        String fieldName = pair.right;
        RexNode rex = pair.left;
        postProjectDimListBuilder.add(fieldName);
        // Render Post JSON object when PostProject exists. In DruidPostAggregationProjectRule
        // all check has been done to ensure all RexCall rexNode can be pushed in.
        if (rex instanceof RexCall) {
          JsonPostAggregation jsonPost = getJsonPostAggregation(fieldName, rex,
              postProject.getInput()
          );
          postAggs.add(jsonPost);
        }
      }
      fieldNames = postProjectDimListBuilder.build();
    }

    final Granularity timeseriesGranularity;
    if (groupByKeyDims.size() == 1) {
      DimensionSpec dimensionSpec = Iterables.getOnlyElement(groupByKeyDims);
      Granularity granularity = ExtractionDimensionSpec.toQueryGranularity(dimensionSpec);
      timeseriesGranularity = granularity;
    } else {
      timeseriesGranularity = null;
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
                p.right == Direction.DESCENDING ? "descending" : "ascending", dimensionOrder
            ));
        if (p.left >= groupSet.cardinality() && p.right == Direction.DESCENDING) {
          // Currently only support for DESC in TopN
          sortsMetric = true;
        } else if (timeseriesGranularity != null && groupSet.get(p.left)) {
          //CASE SORT BY TIME COLUMN
          assert timeSeriesDirection == null;
          timeSeriesDirection = p.right;
        }
      }
      collations = colBuilder.build();
    }

    limit = new JsonLimit("default", fetch, collations);

    //Infer the query Type
    if ((groupByKeyDims.isEmpty() || timeseriesGranularity != null)
        && (collations == null || timeSeriesDirection != null)) {
      queryType = QueryType.TIMESERIES;
      queryGranularity = timeseriesGranularity == null ? Granularity.ALL : timeseriesGranularity;
      assert fetch == null;
    } else if (config.approximateTopN()
        && groupByKeyDims.size() == 1
        && sortsMetric
        && collations.size() == 1
        && fetch != null) {
      queryType = QueryType.TOP_N;
      queryGranularity = Granularity.ALL;
      topNMetricColumnName = fieldNames.get(collationIndexes.get(0));
    } else {
      queryType = QueryType.GROUP_BY;
      queryGranularity = Granularity.ALL;
    }

    final String queryString = generateQuery(queryType, queryGranularity, jsonFilter,
        groupByKeyDims,
        virtualColumnList, aggregations, postAggs, timeSeriesDirection, limit, fetch,
        topNMetricColumnName
    );

    return new QuerySpec(queryType, queryString, fieldNames);
  }

  private String generateQuery(QueryType queryType, Granularity finalGranularity,
      DruidJsonFilter jsonFilter, List<DimensionSpec> dimensions,
      List<VirtualColumn> virtualColumnList,
      List<JsonAggregation> aggregations, List<JsonPostAggregation> postAggs,
      Direction timeSeriesDirection, JsonLimit limit, Integer fetchLimit,
      String topNMetricColumnName
  ) {
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
        writeFieldIf(generator, "virtualColumns",
            virtualColumnList.size() > 0 ? virtualColumnList : null
        );
        generator.writeStringField("metric", topNMetricColumnName);
        writeFieldIf(generator, "filter", jsonFilter);
        writeField(generator, "aggregations", aggregations);
        writeFieldIf(generator, "postAggregations", postAggs.size() > 0 ? postAggs : null);
        writeField(generator, "intervals", intervals);
        generator.writeNumberField("threshold", fetchLimit);

        generator.writeEndObject();
        break;

      case GROUP_BY:
        generator.writeStartObject();
        generator.writeStringField("queryType", "groupBy");
        generator.writeStringField("dataSource", druidTable.dataSource);
        generator.writeStringField("granularity", finalGranularity.value);
        writeField(generator, "dimensions", dimensions);
        writeFieldIf(generator, "virtualColumns",
            virtualColumnList.size() > 0 ? virtualColumnList : null
        );
        writeFieldIf(generator, "limitSpec", limit);
        writeFieldIf(generator, "filter", jsonFilter);
        writeField(generator, "aggregations", aggregations);
        writeFieldIf(generator, "postAggregations", postAggs.size() > 0 ? postAggs : null);
        writeField(generator, "intervals", intervals);
        writeFieldIf(generator, "having", null);

        generator.writeEndObject();
        break;

      default:
        throw new AssertionError("unknown query type " + queryType);
      }

      generator.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return sw.toString();
  }

  /**
   * Druid Scan Query Body
   */
  static class ScanQuery {

    private String dataSource;

    private List<Interval> intervals;

    private DruidJsonFilter jsonFilter;

    private List<VirtualColumn> virtualColumnList;

    private List<String> columns;

    private Integer fetchLimit;

    ScanQuery(String dataSource, List<Interval> intervals,
        DruidJsonFilter jsonFilter,
        List<VirtualColumn> virtualColumnList,
        List<String> columns,
        Integer fetchLimit
    ) {
      this.dataSource = dataSource;
      this.intervals = intervals;
      this.jsonFilter = jsonFilter;
      this.virtualColumnList = virtualColumnList;
      this.columns = columns;
      this.fetchLimit = fetchLimit;
    }

    public String toQuery() {
      try {
        final StringWriter sw = new StringWriter();
        final JsonFactory factory = new JsonFactory();
        final JsonGenerator generator = factory.createGenerator(sw);
        generator.writeStartObject();
        generator.writeStringField("queryType", "scan");
        generator.writeStringField("dataSource", dataSource);
        writeField(generator, "intervals", intervals);
        writeFieldIf(generator, "filter", jsonFilter);
        writeFieldIf(generator, "virtualColumns",
            virtualColumnList.size() > 0 ? virtualColumnList : null
        );
        writeField(generator, "columns", columns);
        generator.writeStringField("resultFormat", "compactedList");
        if (fetchLimit != null) {
          generator.writeNumberField("limit", fetchLimit);
        }
        generator.writeEndObject();
        generator.close();

        return sw.toString();
      } catch (IOException e) {
        Throwables.propagate(e);
      }
      return null;
    }
  }

  @Nullable
  private static JsonAggregation getJsonAggregation(
      String name, AggregateCall aggCall, RexNode filterNode, String fieldName,
      String aggExpression,
      DruidQuery druidQuery
  ) {
    final boolean fractional;
    final RelDataType type = aggCall.getType();
    final SqlTypeName sqlTypeName = type.getSqlTypeName();
    final JsonAggregation aggregation;
    final CalciteConnectionConfig config = druidQuery.getConnectionConfig();

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
      return null;
    }

    // Convert from a complex metric
    ComplexMetric complexMetric = druidQuery.druidTable.resolveComplexMetric(fieldName, aggCall);

    switch (aggCall.getAggregation().getKind()) {
    case COUNT:
      if (aggCall.isDistinct()) {
        if (aggCall.isApproximate() || config.approximateDistinctCount()) {
          if (complexMetric == null) {
            aggregation = new JsonCardinalityAggregation("cardinality", name,
                ImmutableList.of(fieldName)
            );
          } else {
            aggregation = new JsonAggregation(complexMetric.getMetricType(), name,
                    complexMetric.getMetricName(), null
            );
          }
          break;
        } else {
          // when approximate results were not told be acceptable.
          return null;
        }
      }
      if (aggCall.getArgList().size() == 1 && !aggCall.isDistinct()) {
        // case we have count(column) push it as count(*) where column is not null
        final DruidJsonFilter matchNulls;
        if (fieldName == null) {
          matchNulls = new DruidJsonFilter.JsonExpressionFilter(aggExpression + " == null");
        } else {
          matchNulls = DruidJsonFilter.getSelectorFilter(fieldName, null, null);
        }
        aggregation = new JsonFilteredAggregation(DruidJsonFilter.toNotDruidFilter(matchNulls),
            new JsonAggregation("count", name, fieldName, aggExpression)
        );
      } else if (!aggCall.isDistinct()) {
        aggregation = new JsonAggregation("count", name, fieldName, aggExpression);
      } else {
        aggregation = null;
      }

      break;
    case SUM:
    case SUM0:
      aggregation = new JsonAggregation(fractional ? "doubleSum" : "longSum", name, fieldName,
          aggExpression
      );
      break;
    case MIN:
      aggregation = new JsonAggregation(fractional ? "doubleMin" : "longMin", name, fieldName,
          aggExpression
      );
      break;
    case MAX:
      aggregation = new JsonAggregation(fractional ? "doubleMax" : "longMax", name, fieldName,
          aggExpression
      );
      break;
    default:
      return null;
    }

    if (aggregation == null) {
      return null;
    }
    // translate filters
    if (filterNode != null) {
      DruidJsonFilter druidFilter = DruidJsonFilter
          .toDruidFilters(filterNode, druidQuery.table.getRowType(), druidQuery);
      if (druidFilter == null) {
        //can not translate filter
        return null;
      }
      return new JsonFilteredAggregation(druidFilter, aggregation);
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
    } else if (o instanceof DruidJson) {
      ((DruidJson) o).write(generator);
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
      case TIMESTAMP:
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

  /** Aggregation element of a Druid "groupBy" or "topN" query. */
  private static class JsonAggregation implements DruidJson {
    final String type;
    final String name;
    final String fieldName;
    final String expression;

    private JsonAggregation(String type, String name, String fieldName, String expression) {
      this.type = type;
      this.name = name;
      this.fieldName = fieldName;
      this.expression = expression;
    }

    public void write(JsonGenerator generator) throws IOException {
      generator.writeStartObject();
      generator.writeStringField("type", type);
      generator.writeStringField("name", name);
      writeFieldIf(generator, "fieldName", fieldName);
      writeFieldIf(generator, "expression", expression);
      generator.writeEndObject();
    }
  }

  /** Collation element of a Druid "groupBy" query. */
  private static class JsonLimit implements DruidJson {
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
  private static class JsonCollation implements DruidJson {
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
      super(type, name, null, null);
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
    final DruidJsonFilter filter;
    final JsonAggregation aggregation;

    private JsonFilteredAggregation(DruidJsonFilter filter, JsonAggregation aggregation) {
      // Filtered aggregations don't use the "name" and "fieldName" fields directly,
      // but rather use the ones defined in their "aggregation" field.
      super("filtered", aggregation.name, aggregation.fieldName, null);
      this.filter = filter;
      this.aggregation = aggregation;
    }

    @Override public void write(JsonGenerator generator) throws IOException {
      generator.writeStartObject();
      generator.writeStringField("type", type);
      writeField(generator, "filter", filter);
      writeField(generator, "aggregator", aggregation);
      generator.writeEndObject();
    }
  }

  /** Post-Aggregator Post aggregator abstract writer */
  protected abstract static class JsonPostAggregation implements DruidJson {
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
