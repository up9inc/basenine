package main

import (
	"strconv"

	oj "github.com/ohler55/ojg/oj"
)

func boolOperand(operand interface{}) bool {
	var _operand bool
	switch operand.(type) {
	case string:
		_operand = operand.(string) != ""
	case bool:
		_operand = operand.(bool)
	case float64:
		_operand = operand.(float64) > 0
	case nil:
		_operand = false
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

func float64Operand(operand interface{}) float64 {
	var _operand float64
	switch operand.(type) {
	case string:
		if f, err := strconv.ParseFloat(operand.(string), 64); err == nil {
			_operand = f
		} else {
			_operand = 0
		}
	case float64:
		_operand = operand.(float64)
	case bool:
		if operand.(bool) {
			_operand = 1.0
		} else {
			_operand = 0
		}
	case nil:
		_operand = 0
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

func gtr(operand1 interface{}, operand2 interface{}) bool {
	return float64Operand(operand1) > float64Operand(operand2)
}

func lss(operand1 interface{}, operand2 interface{}) bool {
	return float64Operand(operand1) < float64Operand(operand2)
}

func geq(operand1 interface{}, operand2 interface{}) bool {
	return float64Operand(operand1) >= float64Operand(operand2)
}

func leq(operand1 interface{}, operand2 interface{}) bool {
	return float64Operand(operand1) <= float64Operand(operand2)
}

var comparisonOperations = map[string]interface{}{
	">":  gtr,
	"<":  lss,
	">=": geq,
	"<=": leq,
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
	if unar.Unary != nil {
		v, err = evalUnary(unar.Unary, obj)
		if err != nil {
			return
		}
		switch v.(type) {
		case bool:
			if unar.Op == "!" {
				v = !v.(bool)
			}
		case float64:
			if unar.Op == "-" {
				v = -v.(float64)
			}
		}
	} else {
		v, err = evalPrimary(unar.Primary, obj)
	}

	return
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
	logic, err := evalLogical(comp.Logical, obj)
	if err != nil {
		return
	}

	var next interface{}
	if comp.Next != nil {
		next, err = evalComparison(comp.Next, obj)
		if err != nil {
			return
		}
		v = comparisonOperations[comp.Op].(func(interface{}, interface{}) bool)(logic, next)
		return
	} else {
		v = logic
	}

	return
}

func evalEquality(equ *Equality, obj interface{}) (v interface{}, err error) {
	comp, err := evalComparison(equ.Comparison, obj)
	if err != nil {
		return
	}

	var next interface{}
	if equ.Next != nil {
		next, err = evalEquality(equ.Next, obj)
		if err != nil {
			return
		}
		v = equalityOperations[equ.Op].(func(interface{}, interface{}) bool)(comp, next)
		return
	} else {
		v = comp
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
