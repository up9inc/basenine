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

	return xProp
}

// computeCallExpression does compile-time evaluations for the
// CallExpression struct. Populates the non-gramatical fields in Primary struct
// according to the parsing results.
func computeCallExpression(call *CallExpression, prependPath string, jsonHelper bool) (jsonPath *jp.Expr, helper *string, prop Propagate, err error) {
	if call.Parameters == nil {
		// Not a function call
		if call.Identifier != nil {
			// Queries like `request.path == "x"`` goes here
			prop.path = *call.Identifier
		}
		if call.SelectExpression != nil {
			segments := strings.Split(prop.path, ".")
			potentialHelper := &segments[len(segments)-1]
			// Determine whether the .json() helper is used or not
			jsonHelperUsed := false
			if *potentialHelper == "json" {
				helper = potentialHelper
				jsonHelperUsed = true
			}

			if call.SelectExpression.Index != nil {
				// Queries like `request.path[0] == "x"`` goes here
				if jsonHelperUsed {
					jsonPathParam, err := jp.ParseString(fmt.Sprintf("[%d]", *call.SelectExpression.Index))
					if err == nil {
						call.Parameters = []*Parameter{{JsonPath: &jsonPathParam}}
					}
					jsonHelper = false
				} else {
					prop.path = fmt.Sprintf("%s[%d]", prop.path, *call.SelectExpression.Index)
				}
			} else if call.SelectExpression.Key != nil {
				// Queries like `request.headers["x"] == "z"`` goes here
				if jsonHelperUsed {
					jsonPathParam, err := jp.ParseString(fmt.Sprintf("[\"%s\"]", strings.Trim(*call.SelectExpression.Key, "\"")))
					if err == nil {
						call.Parameters = []*Parameter{{JsonPath: &jsonPathParam}}
					}
					jsonHelper = false
				} else {
					prop.path = fmt.Sprintf("%s[\"%s\"]", prop.path, strings.Trim(*call.SelectExpression.Key, "\""))
				}
			}

			// Queries like `request.headers["x"].y == "z"`` or `request.body.json().some.path` goes here
			if call.SelectExpression.Expression != nil {
				var _prop Propagate
				_prop, err = computeExpression(call.SelectExpression.Expression, prop.path, jsonHelperUsed)
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
	if jsonHelper {
		// If .json() helper is used
		jsonPathParam, err := jp.ParseString(prop.path)
		if err == nil {
			call.Parameters = []*Parameter{{JsonPath: &jsonPathParam}}
		}
		jsonHelper = false
		prop.path = prependPath
	} else {
		// else concatenate the paths
		prop.path = fmt.Sprintf("%s.%s", prependPath, prop.path)
	}
	_jsonPath, err := jp.ParseString(prop.path)

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
func computePrimary(pri *Primary, prependPath string, jsonHelper bool) (prop Propagate, err error) {
	if pri.SubExpression != nil {
		prop, err = computeExpression(pri.SubExpression, prependPath, jsonHelper)
	} else if pri.CallExpression != nil {
		pri.JsonPath, pri.Helper, prop, err = computeCallExpression(pri.CallExpression, prependPath, jsonHelper)
	} else if pri.Regex != nil {
		pri.Regexp, err = regexp.Compile(strings.Trim(*pri.Regex, "\""))
	}
	return
}

// Gateway method for doing compile-time evaluations on Primary struct
func computeUnary(unar *Unary, prependPath string, jsonHelper bool) (prop Propagate, err error) {
	var _prop Propagate
	if unar.Unary != nil {
		prop, err = computeUnary(unar.Unary, prependPath, jsonHelper)
	} else {
		_prop, err = computePrimary(unar.Primary, prependPath, jsonHelper)
		prop = backpropagate(prop, _prop)
	}
	return
}

// Gateway method for doing compile-time evaluations on Primary struct
func computeComparison(comp *Comparison, prependPath string, jsonHelper bool) (prop Propagate, err error) {
	var _prop Propagate
	prop, err = computeUnary(comp.Unary, prependPath, jsonHelper)
	if comp.Next != nil {
		_prop, err = computeComparison(comp.Next, prependPath, jsonHelper)
		prop = backpropagate(prop, _prop)
	}
	return
}

// Gateway method for doing compile-time evaluations on Primary struct
func computeEquality(equ *Equality, prependPath string, jsonHelper bool) (prop Propagate, err error) {
	var _prop Propagate
	prop, err = computeComparison(equ.Comparison, prependPath, jsonHelper)
	if equ.Next != nil {
		_prop, err = computeEquality(equ.Next, prependPath, jsonHelper)
		prop = backpropagate(prop, _prop)
	}
	return
}

// Gateway method for doing compile-time evaluations on Primary struct
func computeLogical(logic *Logical, prependPath string, jsonHelper bool) (prop Propagate, err error) {
	var _prop Propagate
	prop, err = computeEquality(logic.Equality, prependPath, jsonHelper)
	if logic.Next != nil {
		_prop, err = computeLogical(logic.Next, prependPath, jsonHelper)
		prop = backpropagate(prop, _prop)
	}
	return
}

// Gateway method for doing compile-time evaluations on Primary struct
func computeExpression(expr *Expression, prependPath string, jsonHelper bool) (prop Propagate, err error) {
	if expr.Logical == nil {
		return
	}
	prop, err = computeLogical(expr.Logical, prependPath, jsonHelper)
	return
}

// Precompute does compile-time evaluations on parsed query (AST/Expression)
// to prevent unnecessary computations in Eval() method.
// Modifies the fields of only the Primary struct.
func Precompute(expr *Expression) (prop Propagate, err error) {
	prop, err = computeExpression(expr, "", false)
	return
}
