// Copyright 2021 UP9. All rights reserved.
// Use of this source code is governed by Apache License 2.0
// license that can be found in the LICENSE file.

package main

import (
	"regexp"
	"strconv"
	"strings"
	"time"

	oj "github.com/ohler55/ojg/oj"
)

// bool operand evaluator. Boolean literals falls into this method.
func boolOperand(operand interface{}) bool {
	var _operand bool
	switch operand.(type) {
	case string:
		_operand = operand.(string) != ""
	case bool:
		_operand = operand.(bool)
	case int64:
		_operand = operand.(int64) > 0
	case float64:
		_operand = operand.(float64) > 0
	case nil:
		_operand = false
	}
	return _operand
}

// string operand evaluator. String literals falls into this method.
func stringOperand(operand interface{}) string {
	var _operand string
	switch operand.(type) {
	case string:
		_operand = operand.(string)
	case int64:
		_operand = strconv.FormatInt(operand.(int64), 10)
	case float64:
		_operand = strconv.FormatFloat(operand.(float64), 'g', 6, 64)
	case bool:
		_operand = strconv.FormatBool(operand.(bool))
	case nil:
		_operand = "null"
	}
	return _operand
}

// float64 operand evaluator. Any integer or float literal falls into this method.
func float64Operand(operand interface{}) float64 {
	var _operand float64
	switch operand.(type) {
	case string:
		if f, err := strconv.ParseFloat(operand.(string), 64); err == nil {
			_operand = f
		} else {
			_operand = 0
		}
	case int64:
		_operand = float64(operand.(int64))
	case float64:
		_operand = operand.(float64)
	case bool:
		if operand.(bool) {
			_operand = 1.0
		} else {
			_operand = 0
		}
	case nil:
		_operand = 0
	}
	return _operand
}

func and(operand1 interface{}, operand2 interface{}) bool {
	return boolOperand(operand1) && boolOperand(operand2)
}

func or(operand1 interface{}, operand2 interface{}) bool {
	return boolOperand(operand1) || boolOperand(operand2)
}

// Map of logical operations
var logicalOperations = map[string]interface{}{
	"and": and,
	"or":  or,
}

func eql(operand1 interface{}, operand2 interface{}) bool {
	switch operand1.(type) {
	case *regexp.Regexp:
		return operand1.(*regexp.Regexp).MatchString(stringOperand(operand2))
	default:
		switch operand2.(type) {
		case *regexp.Regexp:
			return operand2.(*regexp.Regexp).MatchString(stringOperand(operand1))
		default:
			return stringOperand(operand1) == stringOperand(operand2)
		}
	}
}

func neq(operand1 interface{}, operand2 interface{}) bool {
	switch operand1.(type) {
	case *regexp.Regexp:
		return !operand1.(*regexp.Regexp).MatchString(stringOperand(operand2))
	default:
		switch operand2.(type) {
		case *regexp.Regexp:
			return !operand2.(*regexp.Regexp).MatchString(stringOperand(operand1))
		default:
			return stringOperand(operand1) != stringOperand(operand2)
		}
	}
}

// Map of equality operations
var equalityOperations = map[string]interface{}{
	"==": eql,
	"!=": neq,
}

func gtr(operand1 interface{}, operand2 interface{}) bool {
	return float64Operand(operand1) > float64Operand(operand2)
}

func lss(operand1 interface{}, operand2 interface{}) bool {
	return float64Operand(operand1) < float64Operand(operand2)
}

func geq(operand1 interface{}, operand2 interface{}) bool {
	return float64Operand(operand1) >= float64Operand(operand2)
}

func leq(operand1 interface{}, operand2 interface{}) bool {
	return float64Operand(operand1) <= float64Operand(operand2)
}

// Map of comparison operations
var comparisonOperations = map[string]interface{}{
	">":  gtr,
	"<":  lss,
	">=": geq,
	"<=": leq,
}

func startsWith(args ...interface{}) interface{} {
	return strings.HasPrefix(args[0].(string), args[1].(string))
}

func endsWith(args ...interface{}) interface{} {
	return strings.HasSuffix(args[0].(string), args[1].(string))
}

func contains(args ...interface{}) interface{} {
	return strings.Contains(args[0].(string), args[1].(string))
}

func datetime(args ...interface{}) interface{} {
	layout := "01/02/2006, 3:04:05 PM"
	t, err := time.Parse(layout, args[1].(string))
	if err != nil {
		return false
	} else {
		timestamp := t.Unix()
		return timestamp
	}
}

// Map of helper methods
var helpers = map[string]interface{}{
	"startsWith": startsWith,
	"endsWith":   endsWith,
	"contains":   contains,
	"datetime":   datetime,
}

// Iterates and evaulates each parameter of a given function call
func evalParameters(params []*Parameter, obj interface{}) (vs []interface{}, err error) {
	for _, param := range params {
		var v interface{}
		v, err = evalExpression(param.Expression, obj)
		vs = append(vs, v)
	}
	return
}

// Recurses to evalExpression
func evalSelectExpression(sel *SelectExpression, obj interface{}) (v interface{}, err error) {
	if sel.Expression != nil {
		v, err = evalExpression(sel.Expression, obj)
	} else {
		v = false
	}
	return
}

// Gateway method for evalSelectExpression
func evalCallExpression(call *CallExpression, obj interface{}) (v interface{}, err error) {
	if call.SelectExpression != nil {
		v, err = evalSelectExpression(call.SelectExpression, obj)
	} else {
		v = false
	}
	return
}

// Evaluates boolean, integer, float, string literals and call, sub-, select expressions.
func evalPrimary(pri *Primary, obj interface{}) (v interface{}, collapse bool, err error) {
	if pri.Bool != nil {
		// `true`, `false` goes here
		v = *pri.Bool
	} else if pri.Number != nil {
		// `42`, `3.14` goes here
		v = *pri.Number
	} else if pri.String != nil {
		// `"hello"`` goes here
		v = strings.Trim(*pri.String, "\"")
	} else if pri.JsonPath != nil {
		// `request.path` goes here
		result := pri.JsonPath.Get(obj)
		if len(result) < 1 {
			if pri.Helper == nil {
				// Collapse if JSONPath couldn't be found in the JSON document
				// and it's not a helper method call.
				// collapse = true means the whole expression will be
				// evaluated to false.
				collapse = true
				return
			} else {
				v = false
			}
		} else {
			v = result[0]
		}

		// `brand.name.startsWith("Chev")` goes here
		if pri.Helper != nil && pri.CallExpression != nil {
			var params []interface{}
			params, err = evalParameters(pri.CallExpression.Parameters, obj)
			params = append([]interface{}{v}, params...)
			if helper, ok := helpers[*pri.Helper]; ok {
				v = helper.(func(args ...interface{}) interface{})(params...)
			} else {
				// Collapse if an undefined helper is invoked
				collapse = true
				return
			}
		}
	} else if pri.Regexp != nil {
		// `r"Chev.*"` goes here
		v = pri.Regexp
	} else if pri.SubExpression != nil {
		// `(5 == a)` goes here
		v, err = evalExpression(pri.SubExpression, obj)
	} else if pri.CallExpression != nil {
		// `request.headers["a"]` or `request.path[0]` or `brand["name"].startsWith("Chev")` goes here
		v, err = evalCallExpression(pri.CallExpression, obj)
	} else {
		// Result defaults to `false`
		v = false
	}
	return
}

// Evaluates unary expressions like `!`, `-`
func evalUnary(unar *Unary, obj interface{}) (v interface{}, collapse bool, err error) {
	if unar.Unary != nil {
		v, collapse, err = evalUnary(unar.Unary, obj)
		if err != nil || collapse {
			return
		}
		switch v.(type) {
		case bool:
			if unar.Op == "!" {
				v = !v.(bool)
			}
		case float64:
			if unar.Op == "-" {
				v = -v.(float64)
			}
		}
	} else {
		v, collapse, err = evalPrimary(unar.Primary, obj)
	}

	return
}

// Evaluates comparison expressions like `>=`, `>`, `<=`, `<`
func evalComparison(comp *Comparison, obj interface{}) (v interface{}, collapse bool, err error) {
	var logic interface{}
	logic, collapse, err = evalUnary(comp.Unary, obj)
	if err != nil || collapse {
		return
	}

	var next interface{}
	if comp.Next != nil {
		next, collapse, err = evalComparison(comp.Next, obj)
		if err != nil || collapse {
			return
		}
		v = comparisonOperations[comp.Op].(func(interface{}, interface{}) bool)(logic, next)
		return
	} else {
		v = logic
	}

	return
}

// Evaluates equality expressions like `!=`, `==`
func evalEquality(equ *Equality, obj interface{}) (v interface{}, collapse bool, err error) {
	var comp interface{}
	comp, collapse, err = evalComparison(equ.Comparison, obj)
	if err != nil || collapse {
		return
	}

	var next interface{}
	if equ.Next != nil {
		next, collapse, err = evalEquality(equ.Next, obj)
		if err != nil || collapse {
			return
		}
		v = equalityOperations[equ.Op].(func(interface{}, interface{}) bool)(comp, next)
		return
	} else {
		v = comp
	}

	return
}

// Evaluates logical expressions like `and`, `or`
func evalLogical(logic *Logical, obj interface{}) (v interface{}, collapse bool, err error) {
	var unar interface{}
	unar, collapse, err = evalEquality(logic.Equality, obj)
	if err != nil || collapse {
		return
	}

	// Evaluate `false and`, `true or` fast
	unarBool := boolOperand(unar)
	if logic.Op == "and" && !unarBool {
		v = false
		return
	} else if logic.Op == "or" && unarBool {
		v = true
		return
	}

	var next interface{}
	if logic.Next != nil {
		next, collapse, err = evalLogical(logic.Next, obj)
		if err != nil || collapse {
			return
		}
		v = logicalOperations[logic.Op].(func(interface{}, interface{}) bool)(unar, next)
		return
	} else {
		v = unar
	}

	return
}

// Evaluates the root of AST
func evalExpression(expr *Expression, obj interface{}) (v interface{}, err error) {
	if expr.Logical == nil {
		v = true
		return
	}
	var collapse bool
	v, collapse, err = evalLogical(expr.Logical, obj)
	if collapse {
		v = false
	}
	return
}

// Eval evaluatues boolean truthiness of given JSON against the query that's provided
// in the form of an AST (Expression). It's the method that implements the querying
// functionality in the database.
//
// It's a lazy and a generic implementation to provide a medium for flexible filtering
// on an arbitrary JSON structure.
//
// Calling Precompute() on Expression before calling Eval() improves performance.
func Eval(expr *Expression, json string) (truth bool, err error) {
	obj, err := oj.ParseString(json)
	if err != nil {
		return
	}

	v, err := evalExpression(expr, obj)
	truth = v.(bool)
	return
}
