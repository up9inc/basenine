// Copyright 2021 UP9. All rights reserved.
// Use of this source code is governed by Apache License 2.0
// license that can be found in the LICENSE file.

package basenine

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
	Path    string
	Limit   uint64
	Rlimit  uint64
	LeftOff int64
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
	if xProp.Path == "" {
		xProp.Path = yProp.Path
	}
	if xProp.Limit == 0 {
		xProp.Limit = yProp.Limit
	}
	if xProp.Rlimit == 0 {
		xProp.Rlimit = yProp.Rlimit
	}
	if xProp.LeftOff == 0 {
		xProp.LeftOff = yProp.LeftOff
	}

	return xProp
}

// computeCallExpression does compile-time evaluations for the
// CallExpression struct. Populates the non-gramatical fields in Primary struct
// according to the parsing results.
func computeCallExpression(call *CallExpression, prependPath string, jsonHelperPath string) (jsonPath *jp.Expr, helper *string, prop Propagate, err error) {
	if call.Parameters == nil {
		// Not a function call
		if call.Identifier != nil {
			// Queries like `request.path == "x"`` goes here
			prop.Path = *call.Identifier
		}
		if call.SelectExpression != nil {
			segments := strings.Split(prop.Path, ".")
			potentialHelper := &segments[len(segments)-1]
			// Determine whether the .json() helper is used or not
			jsonHelperUsed := false
			if *potentialHelper == "json" || *potentialHelper == "xml" {
				helper = potentialHelper
				jsonHelperUsed = true
				jsonHelperPath = prop.Path
			}

			if call.SelectExpression.Index != nil {
				// Queries like `request.path[0] == "x"`` goes here
				if jsonHelperUsed {
					jsonPathParam, err := jp.ParseString(fmt.Sprintf("[%d]", *call.SelectExpression.Index))
					if err == nil {
						call.Parameters = []*Parameter{{JsonPath: &jsonPathParam}}
					}
					prop.Path = fmt.Sprintf("[%d]", *call.SelectExpression.Index)
				} else {
					prop.Path = fmt.Sprintf("%s[%d]", prop.Path, *call.SelectExpression.Index)
				}
			} else if call.SelectExpression.Key != nil {
				// Queries like `request.headers["x"] == "z"`` goes here
				if jsonHelperUsed {
					jsonPathParam, err := jp.ParseString(fmt.Sprintf("[\"%s\"]", strings.Trim(*call.SelectExpression.Key, "\"")))
					if err == nil {
						call.Parameters = []*Parameter{{JsonPath: &jsonPathParam}}
					}
					prop.Path = fmt.Sprintf("[\"%s\"]", strings.Trim(*call.SelectExpression.Key, "\""))
				} else {
					prop.Path = fmt.Sprintf("%s[\"%s\"]", prop.Path, strings.Trim(*call.SelectExpression.Key, "\""))
				}
			}

			// Queries like `request.headers["x"].y == "z"`` or `request.body.json().some.path` goes here
			if call.SelectExpression.Expression != nil {
				var _prop Propagate
				propPath := prop.Path
				if jsonHelperUsed {
					propPath = ""
				}
				_prop, err = computeExpression(call.SelectExpression.Expression, propPath, jsonHelperPath)
				prop.Limit = _prop.Limit
				prop.Rlimit = _prop.Rlimit
				prop.LeftOff = _prop.LeftOff
				return
			}
		}
	} else {
		// It's a function call
		prop.Path = *call.Identifier
	}

	prop.Path = fmt.Sprintf("%s.%s", prependPath, prop.Path)

	var _jsonPath jp.Expr
	if jsonHelperPath != "" {
		jsonPathParam, err := jp.ParseString(prop.Path)
		if err == nil {
			call.Parameters = []*Parameter{{JsonPath: &jsonPathParam}}
		}
		prop.Path = jsonHelperPath
	}

	_jsonPath, err = jp.ParseString(prop.Path)

	// If it's a function call, determine the name of helper method.
	if call.Parameters != nil {
		segments := strings.Split(prop.Path, ".")
		helper = &segments[len(segments)-1]
		_jsonPath = _jsonPath[:len(_jsonPath)-1]

		if strContains(compileTimeEvaluatedHelpers, *helper) {
			if len(call.Parameters) > 0 {
				// We don't alter the record on compile-time. So the second record value is disabled
				v, _, err := evalExpression(call.Parameters[0].Expression, nil)
				if err == nil {
					switch *helper {
					case "rlimit":
						prop.Rlimit = uint64(float64Operand(v))
					case "limit":
						prop.Limit = uint64(float64Operand(v))
					case "leftOff":
						prop.LeftOff = int64(float64Operand(v))
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
func computePrimary(pri *Primary, prependPath string, jsonHelperPath string) (prop Propagate, err error) {
	if pri.SubExpression != nil {
		prop, err = computeExpression(pri.SubExpression, prependPath, jsonHelperPath)
	} else if pri.CallExpression != nil {
		pri.JsonPath, pri.Helper, prop, err = computeCallExpression(pri.CallExpression, prependPath, jsonHelperPath)
	} else if pri.Regex != nil {
		pri.Regexp, err = regexp.Compile(strings.Trim(*pri.Regex, "\""))
	}
	return
}

// Gateway method for doing compile-time evaluations on Primary struct
func computeUnary(unar *Unary, prependPath string, jsonHelperPath string) (prop Propagate, err error) {
	var _prop Propagate
	if unar.Unary != nil {
		prop, err = computeUnary(unar.Unary, prependPath, jsonHelperPath)
	} else {
		_prop, err = computePrimary(unar.Primary, prependPath, jsonHelperPath)
		prop = backpropagate(prop, _prop)
	}
	return
}

// Gateway method for doing compile-time evaluations on Primary struct
func computeComparison(comp *Comparison, prependPath string, jsonHelperPath string) (prop Propagate, err error) {
	var _prop Propagate
	prop, err = computeUnary(comp.Unary, prependPath, jsonHelperPath)
	if comp.Next != nil {
		_prop, err = computeComparison(comp.Next, prependPath, jsonHelperPath)
		prop = backpropagate(prop, _prop)
	}
	return
}

// Gateway method for doing compile-time evaluations on Primary struct
func computeEquality(equ *Equality, prependPath string, jsonHelperPath string) (prop Propagate, err error) {
	var _prop Propagate
	prop, err = computeComparison(equ.Comparison, prependPath, jsonHelperPath)
	if equ.Next != nil {
		_prop, err = computeEquality(equ.Next, prependPath, jsonHelperPath)
		prop = backpropagate(prop, _prop)
	}
	return
}

// Gateway method for doing compile-time evaluations on Primary struct
func computeLogical(logic *Logical, prependPath string, jsonHelperPath string) (prop Propagate, err error) {
	var _prop Propagate
	prop, err = computeEquality(logic.Equality, prependPath, jsonHelperPath)
	if logic.Next != nil {
		_prop, err = computeLogical(logic.Next, prependPath, jsonHelperPath)
		prop = backpropagate(prop, _prop)
	}
	return
}

// Gateway method for doing compile-time evaluations on Primary struct
func computeExpression(expr *Expression, prependPath string, jsonHelperPath string) (prop Propagate, err error) {
	if expr.Logical == nil {
		return
	}
	prop, err = computeLogical(expr.Logical, prependPath, jsonHelperPath)
	return
}

// Precompute does compile-time evaluations on parsed query (AST/Expression)
// to prevent unnecessary computations in Eval() method.
// Modifies the fields of only the Primary struct.
func Precompute(expr *Expression) (prop Propagate, err error) {
	prop, err = computeExpression(expr, "", "")
	return
}
