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

// computeCallExpression does compile-time evaluations for the
// CallExpression struct. Populates the non-gramatical fields in Primary struct
// according to the parsing results.
func computeCallExpression(call *CallExpression, prependPath string) (jsonPath *jp.Expr, helper *string, path string, err error) {
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
				_, err = computeExpression(call.SelectExpression.Expression, path)
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
	}

	jsonPath = &_jsonPath
	return
}

// computePrimary does compile-time evaluations for the
// Primary struct. Populates the non-gramatical fields in Primary struct
// according to the parsing results.
func computePrimary(pri *Primary, prependPath string) (path string, err error) {
	if pri.SubExpression != nil {
		path, err = computeExpression(pri.SubExpression, prependPath)
	} else if pri.CallExpression != nil {
		pri.JsonPath, pri.Helper, path, err = computeCallExpression(pri.CallExpression, prependPath)
	} else if pri.Regex != nil {
		pri.Regexp, err = regexp.Compile(strings.Trim(*pri.Regex, "\""))
	}
	return
}

// Gateway method for doing compile-time evaluations on Primary struct
func computeUnary(unar *Unary, prependPath string) (path string, err error) {
	var _path string
	if unar.Unary != nil {
		path, err = computeUnary(unar.Unary, prependPath)
	} else {
		_path, err = computePrimary(unar.Primary, prependPath)
		if path == "" {
			path = _path
		}
	}
	return
}

// Gateway method for doing compile-time evaluations on Primary struct
func computeComparison(comp *Comparison, prependPath string) (path string, err error) {
	var _path string
	path, err = computeUnary(comp.Unary, prependPath)
	if comp.Next != nil {
		_path, err = computeComparison(comp.Next, prependPath)
		if path == "" {
			path = _path
		}
	}
	return
}

// Gateway method for doing compile-time evaluations on Primary struct
func computeEquality(equ *Equality, prependPath string) (path string, err error) {
	var _path string
	path, err = computeComparison(equ.Comparison, prependPath)
	if equ.Next != nil {
		_path, err = computeEquality(equ.Next, prependPath)
		if path == "" {
			path = _path
		}
	}
	return
}

// Gateway method for doing compile-time evaluations on Primary struct
func computeLogical(logic *Logical, prependPath string) (path string, err error) {
	var _path string
	path, err = computeEquality(logic.Equality, prependPath)
	if logic.Next != nil {
		_path, err = computeLogical(logic.Next, prependPath)
		if path == "" {
			path = _path
		}
	}
	return
}

// Gateway method for doing compile-time evaluations on Primary struct
func computeExpression(expr *Expression, prependPath string) (path string, err error) {
	if expr.Logical == nil {
		return
	}
	path, err = computeLogical(expr.Logical, prependPath)
	return
}

// Precompute does compile-time evaluations on parsed query (AST/Expression)
// to prevent unnecessary computations in Eval() method.
// Modifies the fields of only the Primary struct.
func Precompute(expr *Expression) (err error) {
	_, err = computeExpression(expr, "")
	return
}
