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

func EvalPrimary(pri *Primary, obj interface{}) (v interface{}, err error) {
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

func EvalUnary(unar *Unary, obj interface{}) (v interface{}, err error) {
	return EvalPrimary(unar.Primary, obj)
}

func EvalLogical(logic *Logical, obj interface{}) (v interface{}, err error) {
	unar, err := EvalUnary(logic.Unary, obj)
	if err != nil {
		return
	}

	var next interface{}
	if logic.Next != nil {
		next, err = EvalLogical(logic.Next, obj)
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

func EvalComparison(comp *Comparison, obj interface{}) (v interface{}, err error) {
	return EvalLogical(comp.Logical, obj)
}

func EvalEquality(equ *Equality, obj interface{}) (v interface{}, err error) {
	logical, err := EvalComparison(equ.Comparison, obj)
	if err != nil {
		return
	}

	var next interface{}
	if equ.Next != nil {
		next, err = EvalEquality(equ.Next, obj)
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

func EvalExpression(expr *Expression, obj interface{}) (truth bool, err error) {
	v, err := EvalEquality(expr.Equality, obj)
	truth = v.(bool)
	return
}

func Eval(expr *Expression, json string) (truth bool, err error) {
	obj, err := oj.ParseString(json)
	if err != nil {
		return
	}

	truth, err = EvalExpression(expr, obj)
	return
}
