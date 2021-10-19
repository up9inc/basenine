package main

import (
	"fmt"
	"regexp"
	"strings"

	jp "github.com/ohler55/ojg/jp"
)

func computeCallExpression(call *CallExpression) (jsonPath *jp.Expr, path string, err error) {
	var _path string
	if call.Parameters == nil {
		if call.Identifier != nil {
			path = *call.Identifier
		}
		if call.SelectExpression != nil {
			if call.SelectExpression.Index != nil {
				path = fmt.Sprintf("%s[%d]", path, *call.SelectExpression.Index)
			} else if call.SelectExpression.Key != nil {
				path = fmt.Sprintf("%s.%s", path, *call.SelectExpression.Key)
			} else if call.SelectExpression.Expression != nil {
				_path, err = computeExpression(call.SelectExpression.Expression, true)
				path = fmt.Sprintf("%s.%s", path, _path)
			}
		}
	}
	_jsonPath, err := jp.ParseString(path)
	jsonPath = &_jsonPath
	if err != nil {
		return
	}
	return
}

func computePrimary(pri *Primary, returnPath bool) (path string, err error) {
	if pri.SubExpression != nil {
		path, err = computeExpression(pri.SubExpression, returnPath)
	} else if pri.CallExpression != nil {
		pri.JsonPath, path, err = computeCallExpression(pri.CallExpression)
	} else if pri.Regex != nil {
		pri.Regexp, err = regexp.Compile(strings.Trim(*pri.Regex, "\""))
	}
	return
}

func computeUnary(unar *Unary, returnPath bool) (path string, err error) {
	if unar.Unary != nil {
		path, err = computeUnary(unar.Unary, returnPath)
	} else {
		path, err = computePrimary(unar.Primary, returnPath)
	}
	return
}

func computeLogical(logic *Logical, returnPath bool) (path string, err error) {
	path, err = computeUnary(logic.Unary, returnPath)
	if logic.Next != nil {
		path, err = computeLogical(logic.Next, returnPath)
	}
	return
}

func computeComparison(comp *Comparison, returnPath bool) (path string, err error) {
	path, err = computeLogical(comp.Logical, returnPath)
	if comp.Next != nil {
		path, err = computeComparison(comp.Next, returnPath)
	}
	return
}

func computeEquality(equ *Equality, returnPath bool) (path string, err error) {
	path, err = computeComparison(equ.Comparison, returnPath)
	if equ.Next != nil {
		path, err = computeEquality(equ.Next, returnPath)
	}
	return
}

func computeExpression(expr *Expression, returnPath bool) (path string, err error) {
	if expr.Equality == nil {
		return
	}
	path, err = computeEquality(expr.Equality, returnPath)
	return
}

func ComputeJsonPaths(expr *Expression) (err error) {
	_, err = computeExpression(expr, false)
	return
}
