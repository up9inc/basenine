// Copyright 2021 UP9. All rights reserved.
// Use of this source code is governed by Apache License 2.0
// license that can be found in the LICENSE file.

package main

import (
	"regexp"

	"github.com/alecthomas/participle/v2"
	jp "github.com/ohler55/ojg/jp"
)

type Expression struct {
	Logical *Logical `@@`
}

type Logical struct {
	Equality *Equality `@@`
	Op       string    `[ @( "and" | "or" )`
	Next     *Logical  `  @@ ]`
}

type Equality struct {
	Comparison *Comparison `@@`
	Op         string      `[ @( "!" "=" | "=" "=" )`
	Next       *Equality   `  @@ ]`
}

type Comparison struct {
	Unary *Unary      `@@`
	Op    string      `[ @( ">" "=" | ">" | "<" "=" | "<" )`
	Next  *Comparison `  @@ ]`
}

type Unary struct {
	Op      string   `  ( @( "!" | "-" )`
	Unary   *Unary   `    @@ )`
	Primary *Primary `| @@`
}

type Primary struct {
	Number         *float64        `  @Float | @Int`
	String         *string         `| @(String|Char|RawString)`
	Regex          *string         `| "r" @(String|Char|RawString)`
	Bool           *bool           `| ( @"true" | "false" )`
	Nil            bool            `| @"nil"`
	CallExpression *CallExpression `| @@`
	SubExpression  *Expression     `| "(" @@ ")" `
	JsonPath       *jp.Expr
	Regexp         *regexp.Regexp
	Helper         *string
}

type CallExpression struct {
	Identifier       *string           `@Ident ( @"." @Ident )*`
	Parameters       []*Parameter      `[ "(" (@@ ("," @@)*)? ")" ]`
	SelectExpression *SelectExpression `[ @@ ]`
}

type SelectExpression struct {
	Index      *int        `[ "[" @Int "]" ]`
	Key        *string     `[ "[" @(String|Char|RawString) "]" ]`
	Expression *Expression `[ "." @@ ]`
}

type Parameter struct {
	Tag        *string     `[ @Ident ":" ]`
	Expression *Expression `@@`
	JsonPath   *jp.Expr
}

var parser = participle.MustBuild(&Expression{}, participle.UseLookahead(2))

// Parse parses the query (filtering syntax) into tree stucture
// defined as Expression. Tags defines the grammar rules and tokens.
// Expression is the Abstract Syntax Tree (AST) of this query language.
func Parse(text string) (expr *Expression, err error) {
	expr = &Expression{}
	if text == "" {
		return
	}
	err = parser.ParseString("", text, expr)
	return
}
