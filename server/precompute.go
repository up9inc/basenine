// Copyright 2021 UP9. All rights reserved.
// Use of this source code is governed by Apache License 2.0
// license that can be found in the LICENSE file.

package main

import (
	"fmt"
	"regexp"
	"strings"

	jp "github.com/ohler55/ojg/jp"
)

var compileTimeEvaluatedHelpers = []string{
	"limit",
	"rlimit",
	"leftOff",
}

type Propagate struct {
	path    string
	limit   uint64
	rlimit  uint64
	leftOff int64
	qj      QueryJump
}

var comparisonGreater = map[string]bool{
	">":  true,
	"<":  false,
	">=": true,
	"<=": false,
}

type QueryValDirection struct {
	value     float64
	operator  string
	direction bool
	enable    bool
}

type QueryJump struct {
	leftOff int
	qvd     QueryValDirection
}

// strContains checks if a string is present in a slice
func strContains(s []string, str string) bool {
	for _, v := range s {
		if v == str {
			return true
		}
	}

	return false
}

// Backpropagates the values returned from the binary expressions
func backpropagate(xProp Propagate, yProp Propagate) (prop Propagate) {
	if xProp.path == "" {
		xProp.path = yProp.path
	}
	if xProp.limit == 0 {
		xProp.limit = yProp.limit
	}
	if xProp.rlimit == 0 {
		xProp.rlimit = yProp.rlimit
	}
	if xProp.leftOff == 0 {
		xProp.leftOff = yProp.leftOff
	}
	if xProp.qj.leftOff == 0 {
		xProp.qj.leftOff = yProp.qj.leftOff
	}

	return xProp
}

// computeCallExpression does compile-time evaluations for the
// CallExpression struct. Populates the non-gramatical fields in Primary struct
// according to the parsing results.
func computeCallExpression(call *CallExpression, prependPath string, qvd QueryValDirection) (jsonPath *jp.Expr, helper *string, prop Propagate, err error) {
	if call.Parameters == nil {
		// Not a function call
		if call.Identifier != nil {
			// Queries like `request.path == "x"`` goes here
			prop.path = *call.Identifier
		}
		if call.SelectExpression != nil {
			if call.SelectExpression.Index != nil {
				// Queries like `request.path[0] == "x"`` goes here
				prop.path = fmt.Sprintf("%s[%d]", prop.path, *call.SelectExpression.Index)
			} else if call.SelectExpression.Key != nil {
				// Queries like `request.headers["x"] == "z"`` goes here
				prop.path = fmt.Sprintf("%s[\"%s\"]", prop.path, strings.Trim(*call.SelectExpression.Key, "\""))
			}

			// Queries like `request.headers["x"].y == "z"`` goes here
			if call.SelectExpression.Expression != nil {
				var _prop Propagate
				_prop, err = computeExpression(call.SelectExpression.Expression, prop.path)
				prop.limit = _prop.limit
				prop.rlimit = _prop.rlimit
				prop.leftOff = _prop.leftOff
				return
			}
		}
	} else {
		// It's a function call
		prop.path = *call.Identifier
	}

	// Build JSONPath
	prop.path = fmt.Sprintf("%s.%s", prependPath, prop.path)
	_jsonPath, err := jp.ParseString(prop.path)

	// Compute query jump based on indexes
	if qvd.enable {
		prop.qj = computeQueryJump(prop.path, qvd)
	}

	// If it's a function call, determine the name of helper method.
	if call.Parameters != nil {
		segments := strings.Split(prop.path, ".")
		helper = &segments[len(segments)-1]
		_jsonPath = _jsonPath[:len(_jsonPath)-1]

		if strContains(compileTimeEvaluatedHelpers, *helper) {
			if len(call.Parameters) > 0 {
				v, err := evalExpression(call.Parameters[0].Expression, nil)
				if err == nil {
					switch *helper {
					case "rlimit":
						prop.rlimit = uint64(float64Operand(v))
					case "limit":
						prop.limit = uint64(float64Operand(v))
					case "leftOff":
						prop.leftOff = int64(float64Operand(v))
					}
				}
			}
		}
	}

	jsonPath = &_jsonPath
	return
}

// computePrimary does compile-time evaluations for the
// Primary struct. Populates the non-gramatical fields in Primary struct
// according to the parsing results.
func computePrimary(pri *Primary, prependPath string, qvd QueryValDirection) (prop Propagate, err error) {
	if pri.SubExpression != nil {
		prop, err = computeExpression(pri.SubExpression, prependPath)
	} else if pri.CallExpression != nil {
		pri.JsonPath, pri.Helper, prop, err = computeCallExpression(pri.CallExpression, prependPath, qvd)
	} else if pri.Regex != nil {
		pri.Regexp, err = regexp.Compile(strings.Trim(*pri.Regex, "\""))
	}
	return
}

// Gateway method for doing compile-time evaluations on Primary struct
func computeUnary(unar *Unary, prependPath string, qvd QueryValDirection) (prop Propagate, err error) {
	var _prop Propagate
	if unar.Unary != nil {
		prop, err = computeUnary(unar.Unary, prependPath, qvd)
	} else {
		_prop, err = computePrimary(unar.Primary, prependPath, qvd)
		prop = backpropagate(prop, _prop)
	}
	return
}

// Gateway method for doing compile-time evaluations on Primary struct
func computeComparison(comp *Comparison, prependPath string, qvd QueryValDirection, disableQueryJump bool) (prop Propagate, err error) {
	var _prop Propagate
	var v float64
	if comp.Next != nil {
		var next interface{}
		next, _, err = evalComparison(comp.Next, nil)
		if err != nil {
			return
		}
		v = float64Operand(next)
	}
	enableQueryJump := true
	if disableQueryJump {
		enableQueryJump = false
	}
	prop, err = computeUnary(comp.Unary, prependPath, QueryValDirection{v, comp.Op, comparisonGreater[comp.Op], enableQueryJump})
	if comp.Next != nil {
		_prop, err = computeComparison(comp.Next, prependPath, qvd, disableQueryJump)
		prop = backpropagate(prop, _prop)
	}
	return
}

// Gateway method for doing compile-time evaluations on Primary struct
func computeEquality(equ *Equality, prependPath string, disableQueryJump bool) (prop Propagate, err error) {
	var _prop Propagate
	var v float64
	if equ.Next != nil {
		var next interface{}
		next, _, err = evalEquality(equ.Next, nil)
		if err != nil {
			return
		}
		v = float64Operand(next)
	}
	prop, err = computeComparison(equ.Comparison, prependPath, QueryValDirection{v, equ.Op, true, equ.Op == "=="}, disableQueryJump)
	if equ.Next != nil {
		_prop, err = computeEquality(equ.Next, prependPath, disableQueryJump)
		prop = backpropagate(prop, _prop)
	}
	return
}

// Gateway method for doing compile-time evaluations on Primary struct
func computeLogical(logic *Logical, prependPath string) (prop Propagate, err error) {
	var _prop Propagate
	var disableQueryJump bool
	if logic.Next != nil && logic.Op == "or" {
		disableQueryJump = true
	}
	prop, err = computeEquality(logic.Equality, prependPath, disableQueryJump)
	if logic.Next != nil {
		_prop, err = computeLogical(logic.Next, prependPath)
		prop = backpropagate(prop, _prop)
	}
	return
}

// Gateway method for doing compile-time evaluations on Primary struct
func computeExpression(expr *Expression, prependPath string) (prop Propagate, err error) {
	if expr.Logical == nil {
		return
	}
	prop, err = computeLogical(expr.Logical, prependPath)
	return
}

// Precompute does compile-time evaluations on parsed query (AST/Expression)
// to prevent unnecessary computations in Eval() method.
// Modifies the fields of only the Primary struct.
func Precompute(expr *Expression) (prop Propagate, err error) {
	prop, err = computeExpression(expr, "")
	return
}
