package main

import (
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

func EvalPrimary(pri *Primary, obj interface{}) (v interface{}, err error) {
	if pri.Bool != nil {
		v = *pri.Bool
	} else if pri.Number != nil {
		v = *pri.Number
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

func EvalEquality(equ *Equality, obj interface{}) (truth bool, err error) {
	v, err := EvalComparison(equ.Comparison, obj)
	truth = v.(bool)
	return
}

func EvalExpression(expr *Expression, obj interface{}) (truth bool, err error) {
	return EvalEquality(expr.Equality, obj)
}

func Eval(expr *Expression, json string) (truth bool, err error) {
	obj, err := oj.ParseString(json)
	if err != nil {
		return
	}

	truth, err = EvalExpression(expr, obj)
	return
}
