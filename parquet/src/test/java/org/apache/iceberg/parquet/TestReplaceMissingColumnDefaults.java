/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.parquet;

import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.isNull;
import static org.apache.iceberg.expressions.Expressions.notNull;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ParquetFilters#replaceMissingColumnDefaults}: predicates on initial-default
 * columns absent from a file must fold against the default; predicates on present columns and on
 * columns without defaults must be left untouched.
 */
public class TestReplaceMissingColumnDefaults {

  // table read schema: id, plus "c" added later with initial-default "US"
  private static final Schema EXPECTED_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional("c")
              .withId(2)
              .ofType(Types.StringType.get())
              .withInitialDefault(Literal.of("US"))
              .build());

  // a file written before "c" existed: only "id" is physically present
  private static final Schema FILE_SCHEMA_WITHOUT_C =
      new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

  // a file written after "c" was added: both columns present
  private static final Schema FILE_SCHEMA_WITH_C = EXPECTED_SCHEMA;

  private static Expression fold(Expression filter, Schema fileSchema) {
    return ParquetFilters.replaceMissingColumnDefaults(filter, EXPECTED_SCHEMA, fileSchema, true);
  }

  @Test
  public void equalsMatchingDefaultFoldsToTrue() {
    assertThat(fold(equal("c", "US"), FILE_SCHEMA_WITHOUT_C)).isEqualTo(Expressions.alwaysTrue());
  }

  @Test
  public void equalsNonMatchingDefaultFoldsToFalse() {
    assertThat(fold(equal("c", "CA"), FILE_SCHEMA_WITHOUT_C)).isEqualTo(Expressions.alwaysFalse());
  }

  @Test
  public void notNullFoldsToTrueForNonNullDefault() {
    assertThat(fold(notNull("c"), FILE_SCHEMA_WITHOUT_C)).isEqualTo(Expressions.alwaysTrue());
  }

  @Test
  public void isNullFoldsToFalseForNonNullDefault() {
    assertThat(fold(isNull("c"), FILE_SCHEMA_WITHOUT_C)).isEqualTo(Expressions.alwaysFalse());
  }

  @Test
  public void predicateOnPresentColumnIsUnchanged() {
    // file already contains "c": predicate must survive so the file is still filtered on real
    // values
    Expression filter = equal("c", "US");
    assertThat(fold(filter, FILE_SCHEMA_WITH_C)).isSameAs(filter);
  }

  @Test
  public void predicateOnColumnWithoutDefaultIsUnchanged() {
    Expression filter = greaterThan("id", 5L);
    assertThat(fold(filter, FILE_SCHEMA_WITHOUT_C)).isSameAs(filter);
  }

  @Test
  public void conjunctionFoldsOnlyTheAbsentDefaultedColumn() {
    // id > 5 AND c = 'US'  -> on a file missing c, the c clause folds to true, leaving id > 5
    Expression filter = Expressions.and(greaterThan("id", 5L), equal("c", "US"));
    assertThat(fold(filter, FILE_SCHEMA_WITHOUT_C).toString())
        .isEqualTo(greaterThan("id", 5L).toString());
  }

  @Test
  public void nullFilterIsReturnedUnchanged() {
    assertThat(fold(null, FILE_SCHEMA_WITHOUT_C)).isNull();
  }
}
