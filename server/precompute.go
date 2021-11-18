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

// Backpropagates the values returned from the binary expressions
func backpropagate(xPath string, xLimit uint64, xRlimit uint64, yPath string, yLimit uint64, yRlimit uint64) (path string, limit uint64, rlimit uint64) {
	if xPath == "" {
		xPath = yPath
	}
	if xLimit == 0 {
		xLimit = yLimit
	}
	if xRlimit == 0 {
		xRlimit = yRlimit
	}

	return xPath, xLimit, xRlimit
}

// computeCallExpression does compile-time evaluations for the
// CallExpression struct. Populates the non-gramatical fields in Primary struct
// according to the parsing results.
func computeCallExpression(call *CallExpression, prependPath string) (jsonPath *jp.Expr, helper *string, path string, limit uint64, rlimit uint64, err error) {
	if call.Parameters == nil {
		// Not a function call
		if call.Identifier != nil {
			// Queries like `request.path == "x"`` goes here
			path = *call.Identifier
		}
		if call.SelectExpression != nil {
			if call.SelectExpression.Index != nil {
				// Queries like `request.path[0] == "x"`` goes here
				path = fmt.Sprintf("%s[%d]", path, *call.SelectExpression.Index)
			} else if call.SelectExpression.Key != nil {
				// Queries like `request.headers["x"] == "z"`` goes here
				path = fmt.Sprintf("%s[\"%s\"]", path, strings.Trim(*call.SelectExpression.Key, "\""))
			}

			// Queries like `request.headers["x"].y == "z"`` goes here
			if call.SelectExpression.Expression != nil {
				_, limit, rlimit, err = computeExpression(call.SelectExpression.Expression, path)
				return
			}
		}
	} else {
		// It's a function call
		path = *call.Identifier
	}

	// Build JSONPath
	path = fmt.Sprintf("%s.%s", prependPath, path)
	_jsonPath, err := jp.ParseString(path)

	// If it's a function call, determine the name of helper method.
	if call.Parameters != nil {
		segments := strings.Split(path, ".")
		helper = &segments[len(segments)-1]
		_jsonPath = _jsonPath[:len(_jsonPath)-1]

		if *helper == "limit" || *helper == "rlimit" {
			if len(call.Parameters) > 0 {
				v, err := evalExpression(call.Parameters[0].Expression, nil)
				if err == nil {
					if *helper == "rlimit" {
						rlimit = uint64(float64Operand(v))
					} else {
						limit = uint64(float64Operand(v))
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
func computePrimary(pri *Primary, prependPath string) (path string, limit uint64, rlimit uint64, err error) {
	if pri.SubExpression != nil {
		path, limit, rlimit, err = computeExpression(pri.SubExpression, prependPath)
	} else if pri.CallExpression != nil {
		pri.JsonPath, pri.Helper, path, limit, rlimit, err = computeCallExpression(pri.CallExpression, prependPath)
	} else if pri.Regex != nil {
		pri.Regexp, err = regexp.Compile(strings.Trim(*pri.Regex, "\""))
	}
	return
}

// Gateway method for doing compile-time evaluations on Primary struct
func computeUnary(unar *Unary, prependPath string) (path string, limit uint64, rlimit uint64, err error) {
	var _path string
	var _limit, _rlimit uint64
	if unar.Unary != nil {
		path, limit, rlimit, err = computeUnary(unar.Unary, prependPath)
	} else {
		_path, _limit, _rlimit, err = computePrimary(unar.Primary, prependPath)
		path, limit, rlimit = backpropagate(path, limit, rlimit, _path, _limit, _rlimit)
	}
	return
}

// Gateway method for doing compile-time evaluations on Primary struct
func computeComparison(comp *Comparison, prependPath string) (path string, limit uint64, rlimit uint64, err error) {
	var _path string
	var _limit, _rlimit uint64
	path, limit, rlimit, err = computeUnary(comp.Unary, prependPath)
	if comp.Next != nil {
		_path, _limit, _rlimit, err = computeComparison(comp.Next, prependPath)
		path, limit, rlimit = backpropagate(path, limit, rlimit, _path, _limit, _rlimit)
	}
	return
}

// Gateway method for doing compile-time evaluations on Primary struct
func computeEquality(equ *Equality, prependPath string) (path string, limit uint64, rlimit uint64, err error) {
	var _path string
	var _limit, _rlimit uint64
	path, limit, rlimit, err = computeComparison(equ.Comparison, prependPath)
	if equ.Next != nil {
		_path, _limit, _rlimit, err = computeEquality(equ.Next, prependPath)
		path, limit, rlimit = backpropagate(path, limit, rlimit, _path, _limit, _rlimit)
	}
	return
}

// Gateway method for doing compile-time evaluations on Primary struct
func computeLogical(logic *Logical, prependPath string) (path string, limit uint64, rlimit uint64, err error) {
	var _path string
	var _limit, _rlimit uint64
	path, limit, rlimit, err = computeEquality(logic.Equality, prependPath)
	if logic.Next != nil {
		_path, _limit, _rlimit, err = computeLogical(logic.Next, prependPath)
		path, limit, rlimit = backpropagate(path, limit, rlimit, _path, _limit, _rlimit)
	}
	return
}

// Gateway method for doing compile-time evaluations on Primary struct
func computeExpression(expr *Expression, prependPath string) (path string, limit uint64, rlimit uint64, err error) {
	if expr.Logical == nil {
		return
	}
	path, limit, rlimit, err = computeLogical(expr.Logical, prependPath)
	return
}

// Precompute does compile-time evaluations on parsed query (AST/Expression)
// to prevent unnecessary computations in Eval() method.
// Modifies the fields of only the Primary struct.
func Precompute(expr *Expression) (limit uint64, rlimit uint64, err error) {
	_, limit, rlimit, err = computeExpression(expr, "")
	return
}
