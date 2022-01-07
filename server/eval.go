// Copyright 2021 UP9. All rights reserved.
// Use of this source code is governed by Apache License 2.0
// license that can be found in the LICENSE file.

package main

import (
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/ohler55/ojg/jp"
	oj "github.com/ohler55/ojg/oj"
)

const redacted = "[REDACTED]"

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

func startsWith(args ...interface{}) (interface{}, interface{}) {
	return args[0], strings.HasPrefix(stringOperand(args[1]), stringOperand(args[2]))
}

func endsWith(args ...interface{}) (interface{}, interface{}) {
	return args[0], strings.HasSuffix(stringOperand(args[1]), stringOperand(args[2]))
}

func contains(args ...interface{}) (interface{}, interface{}) {
	return args[0], strings.Contains(stringOperand(args[1]), stringOperand(args[2]))
}

func datetime(args ...interface{}) (interface{}, interface{}) {
	layout := "1/2/2006, 3:04:05.000 PM"
	t, err := time.Parse(layout, stringOperand(args[2]))
	if err != nil {
		return args[0], false
	} else {
		// Timestamp is an integer like 1635190131000
		timestamp := t.UnixNano() / int64(time.Millisecond)
		return args[0], timestamp
	}
}

func limit(args ...interface{}) (interface{}, interface{}) {
	// Returns true no matter what. Evaluated on compile-time,
	// limits the number of records returned as a result of the query.
	return args[0], true
}

func rlimit(args ...interface{}) (interface{}, interface{}) {
	// Returns true no matter what. Evaluated on compile-time,
	// limits the number of records returned as a result of the query in reverse
	// but in a weird way such that it limits the past.
	return args[0], true
}

func leftOff(args ...interface{}) (interface{}, interface{}) {
	// Returns true no matter what. Evaluated on compile-time,
	// starts the query from given index.
	return args[0], true
}

func _json(args ...interface{}) (interface{}, interface{}) {
	obj, err := oj.ParseString(stringOperand(args[1]))
	if err != nil {
		return args[0], false
	}
	result := args[2].(*jp.Expr).Get(obj)

	if len(result) < 1 {
		return args[0], false
	}
	return args[0], result[0]
}

func redact(args ...interface{}) (interface{}, interface{}) {
	obj := args[0]
	for _, param := range args[2:] {
		jsonPath, err := jp.ParseString(stringOperand(param))
		if err != nil {
			continue
		}
		result := jsonPath.Get(obj)
		if len(result) < 1 {
			continue
		}
		jsonPath.Set(obj, redacted)
	}
	return obj, true
}

// Map of helper methods
var helpers = map[string]interface{}{
	"startsWith": startsWith,
	"endsWith":   endsWith,
	"contains":   contains,
	"datetime":   datetime,
	"limit":      limit,
	"rlimit":     rlimit,
	"leftOff":    leftOff,
	"json":       _json,
	"redact":     redact,
}

// Iterates and evaulates each parameter of a given function call
func evalParameters(params []*Parameter, obj interface{}) (vs []interface{}, err error) {
	for _, param := range params {
		var v interface{}
		if param.JsonPath != nil {
			v = param.JsonPath
		} else {
			v, _, err = evalExpression(param.Expression, obj)
		}
		vs = append(vs, v)
	}
	return
}

// Recurses to evalExpression
func evalSelectExpression(sel *SelectExpression, obj interface{}) (v interface{}, newObj interface{}, err error) {
	if sel.Expression != nil {
		v, newObj, err = evalExpression(sel.Expression, obj)
	} else {
		v = false
	}
	return
}

// Gateway method for evalSelectExpression
func evalCallExpression(call *CallExpression, obj interface{}) (v interface{}, newObj interface{}, err error) {
	if call.SelectExpression != nil {
		v, newObj, err = evalSelectExpression(call.SelectExpression, obj)
	} else {
		v = false
	}
	return
}

// Evaluates boolean, integer, float, string literals and call, sub-, select expressions.
func evalPrimary(pri *Primary, obj interface{}) (v interface{}, newObj interface{}, collapse bool, err error) {
	newObj = obj

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
			params = append([]interface{}{obj, v}, params...)
			if helper, ok := helpers[*pri.Helper]; ok {
				newObj, v = helper.(func(args ...interface{}) (interface{}, interface{}))(params...)
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
		v, newObj, err = evalExpression(pri.SubExpression, obj)
	} else if pri.CallExpression != nil {
		// `request.headers["a"]` or `request.path[0]` or `brand["name"].startsWith("Chev")` goes here
		v, newObj, err = evalCallExpression(pri.CallExpression, obj)
	} else {
		if pri.Nil {
			// `nil` goes here
			v = nil
		} else {
			// Result defaults to `false`
			v = false
		}
	}
	return
}

// Evaluates unary expressions like `!`, `-`
func evalUnary(unar *Unary, obj interface{}) (v interface{}, newObj interface{}, collapse bool, err error) {
	newObj = obj

	if unar.Unary != nil {
		v, newObj, collapse, err = evalUnary(unar.Unary, obj)
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
		v, newObj, collapse, err = evalPrimary(unar.Primary, obj)
	}

	return
}

// Evaluates comparison expressions like `>=`, `>`, `<=`, `<`
func evalComparison(comp *Comparison, obj interface{}) (v interface{}, newObj interface{}, collapse bool, err error) {
	newObj = obj

	var logic interface{}
	logic, newObj, collapse, err = evalUnary(comp.Unary, obj)
	if err != nil || collapse {
		return
	}

	var next interface{}
	if comp.Next != nil {
		next, newObj, collapse, err = evalComparison(comp.Next, obj)
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
func evalEquality(equ *Equality, obj interface{}) (v interface{}, newObj interface{}, collapse bool, err error) {
	newObj = obj

	var comp interface{}
	comp, newObj, collapse, err = evalComparison(equ.Comparison, obj)
	if err != nil || collapse {
		return
	}

	var next interface{}
	if equ.Next != nil {
		next, newObj, collapse, err = evalEquality(equ.Next, obj)
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
func evalLogical(logic *Logical, obj interface{}) (v interface{}, newObj interface{}, collapse bool, err error) {
	newObj = obj

	var unar interface{}
	unar, newObj, collapse, err = evalEquality(logic.Equality, obj)
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
		next, newObj, collapse, err = evalLogical(logic.Next, obj)
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
func evalExpression(expr *Expression, obj interface{}) (v interface{}, newObj interface{}, err error) {
	newObj = obj

	if expr.Logical == nil {
		v = true
		return
	}
	var collapse bool
	v, newObj, collapse, err = evalLogical(expr.Logical, obj)
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
func Eval(expr *Expression, json string) (truth bool, newJson string, err error) {
	obj, err := oj.ParseString(json)
	if err != nil {
		return
	}

	v, newObj, err := evalExpression(expr, obj)
	truth = boolOperand(v)
	newJson = oj.JSON(newObj)
	return
}
