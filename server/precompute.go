package main

import (
	"fmt"
	"regexp"
	"strings"

	jp "github.com/ohler55/ojg/jp"
)

func computeCallExpression(call *CallExpression, prependPath string) (jsonPath *jp.Expr, helper *string, path string, err error) {
	if call.Parameters == nil {
		if call.Identifier != nil {
			path = *call.Identifier
		}
		if call.SelectExpression != nil {
			if call.SelectExpression.Index != nil {
				path = fmt.Sprintf("%s[%d]", path, *call.SelectExpression.Index)
			} else if call.SelectExpression.Key != nil {
				path = fmt.Sprintf("%s[\"%s\"]", path, strings.Trim(*call.SelectExpression.Key, "\""))
			}

			if call.SelectExpression.Expression != nil {
				_, err = computeExpression(call.SelectExpression.Expression, path)
				return
			}
		}
	} else {
		path = *call.Identifier
	}
	path = fmt.Sprintf("%s.%s", prependPath, path)
	_jsonPath, err := jp.ParseString(path)
	if call.Parameters != nil {
		segments := strings.Split(path, ".")
		helper = &segments[len(segments)-1]
		_jsonPath = _jsonPath[:len(_jsonPath)-1]
	}
	jsonPath = &_jsonPath
	return
}

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

func computeExpression(expr *Expression, prependPath string) (path string, err error) {
	if expr.Logical == nil {
		return
	}
	path, err = computeLogical(expr.Logical, prependPath)
	return
}

func ComputeJsonPaths(expr *Expression) (err error) {
	_, err = computeExpression(expr, "")
	return
}
