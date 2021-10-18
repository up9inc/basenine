package main

import (
	"strconv"

	oj "github.com/ohler55/ojg/oj"
)

func boolOperand(operand interface{}) bool {
	var _operand bool
	switch operand.(type) {
	case bool:
		_operand = operand.(bool)
	case float64:
		_operand = operand.(float64) > 0
	}
	return _operand
}

func stringOperand(operand interface{}) string {
	var _operand string
	switch operand.(type) {
	case string:
		_operand = operand.(string)
	case float64:
		_operand = strconv.FormatFloat(operand.(float64), 'g', 6, 64)
	case bool:
		_operand = strconv.FormatBool(operand.(bool))
	case nil:
		_operand = "null"
	}
	return _operand
}

func and(operand1 interface{}, operand2 interface{}) bool {
	return boolOperand(operand1) && boolOperand(operand2)
}

func or(operand1 interface{}, operand2 interface{}) bool {
	return boolOperand(operand1) || boolOperand(operand2)
}

var logicalOperations = map[string]interface{}{
	"and": and,
	"or":  or,
}

func eql(operand1 interface{}, operand2 interface{}) bool {
	return stringOperand(operand1) == stringOperand(operand2)
}

func neq(operand1 interface{}, operand2 interface{}) bool {
	return stringOperand(operand1) != stringOperand(operand2)
}

var equalityOperations = map[string]interface{}{
	"==": eql,
	"!=": neq,
}

func evalPrimary(pri *Primary, obj interface{}) (v interface{}, err error) {
	if pri.Bool != nil {
		v = *pri.Bool
	} else if pri.Number != nil {
		v = *pri.Number
	} else if pri.String != nil {
		v = *pri.String
	} else {
		v = false
	}
	return
}

func evalUnary(unar *Unary, obj interface{}) (v interface{}, err error) {
	return evalPrimary(unar.Primary, obj)
}

func evalLogical(logic *Logical, obj interface{}) (v interface{}, err error) {
	unar, err := evalUnary(logic.Unary, obj)
	if err != nil {
		return
	}

	var next interface{}
	if logic.Next != nil {
		next, err = evalLogical(logic.Next, obj)
		if err != nil {
			return
		}
		v = logicalOperations[logic.Op].(func(interface{}, interface{}) bool)(unar, next)
		return
	} else {
		v = unar
	}

	return
}

func evalComparison(comp *Comparison, obj interface{}) (v interface{}, err error) {
	return evalLogical(comp.Logical, obj)
}

func evalEquality(equ *Equality, obj interface{}) (v interface{}, err error) {
	logical, err := evalComparison(equ.Comparison, obj)
	if err != nil {
		return
	}

	var next interface{}
	if equ.Next != nil {
		next, err = evalEquality(equ.Next, obj)
		if err != nil {
			return
		}
		v = equalityOperations[equ.Op].(func(interface{}, interface{}) bool)(logical, next)
		return
	} else {
		v = logical
	}

	return
}

func evalExpression(expr *Expression, obj interface{}) (truth bool, err error) {
	v, err := evalEquality(expr.Equality, obj)
	truth = v.(bool)
	return
}

func Eval(expr *Expression, json string) (truth bool, err error) {
	obj, err := oj.ParseString(json)
	if err != nil {
		return
	}

	truth, err = evalExpression(expr, obj)
	return
}
