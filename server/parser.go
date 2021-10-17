package main

import (
	"github.com/alecthomas/participle/v2"
)

type Expression struct {
	Equality *Equality `@@`
}

type Equality struct {
	Comparison *Comparison `@@`
	Op         string      `[ @( "!" "=" | "=" "=" )`
	Next       *Equality   `  @@ ]`
}

type Comparison struct {
	Logical *Logical    `@@`
	Op      string      `[ @( ">" "=" | ">" | "<" "=" | "<" )`
	Next    *Comparison `  @@ ]`
}

type Logical struct {
	Unary *Unary   `@@`
	Op    string   `[ @( "and" | "or" )`
	Next  *Logical `  @@ ]`
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
}

type CallExpression struct {
	Identifier       *string           `@Ident ( @"." @Ident )*`
	Parameters       []*Parameter      `[ "(" (@@ ("," @@)*)? ")" ]`
	SelectExpression *SelectExpression `[ @@ ]`
}

type SelectExpression struct {
	Index      int         `[ "[" @Int "]" ]`
	Key        *string     `[ "[" @(String|Char|RawString) "]" ]`
	Expression *Expression `[ "." @@ ]`
}

type Parameter struct {
	Tag        *string     `[ @Ident ":" ]`
	Expression *Expression `@@`
}

var parser = participle.MustBuild(&Expression{}, participle.UseLookahead(2))

func Parse(text string) *Expression {
	expr := &Expression{}
	err := parser.ParseString("", text, expr)
	if err != nil {
		panic(err)
	}

	return expr
}
