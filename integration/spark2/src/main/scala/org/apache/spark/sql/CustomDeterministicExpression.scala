/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.{DataType, StringType}

/**
 * Custom expression to override the deterministic property .
 */
case class CustomDeterministicExpression(nonDt: Expression ) extends Expression with Serializable{
  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = null

  override def dataType: DataType = StringType

  override def children: Seq[Expression] = Seq()

  def childexp : Expression = nonDt

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = ev.copy("")
}
