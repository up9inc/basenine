package main

import (
	oj "github.com/ohler55/ojg/oj"
)

func and(operand1 bool, operand2 bool) bool {
	return operand1 && operand2
}

func or(operand1 bool, operand2 bool) bool {
	return operand1 || operand2
}

var logicalOperations = map[string]interface{}{
	"and": and,
	"or":  or,
}

func EvalPrimary(pri *Primary, obj interface{}) (truth bool, err error) {
	if pri.Bool != nil {
		truth = *pri.Bool
	}
	return
}

func EvalUnary(unar *Unary, obj interface{}) (truth bool, err error) {
	return EvalPrimary(unar.Primary, obj)
}

func EvalLogical(logic *Logical, obj interface{}) (truth bool, err error) {
	unar, err := EvalUnary(logic.Unary, obj)
	if err != nil {
		return
	}
	var next bool

	if logic.Next != nil {
		next, err = EvalLogical(logic.Next, obj)
		if err != nil {
			return
		}
		truth = logicalOperations[logic.Op].(func(bool, bool) bool)(unar, next)
		return
	} else {
		truth = unar
	}

	return
}

func EvalComparison(comp *Comparison, obj interface{}) (truth bool, err error) {
	return EvalLogical(comp.Logical, obj)
}

func EvalEquality(equ *Equality, obj interface{}) (truth bool, err error) {
	return EvalComparison(equ.Comparison, obj)
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
