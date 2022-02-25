package basenine

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParserBasicBoolean(t *testing.T) {
	text := `
http or !amqp
	`
	expr, err := Parse(text)
	if err != nil {
		t.Fatal(err.Error())
	}
	// repr.Println(expr)

	val1 := "http"
	val2 := "amqp"

	expect := &Expression{
		Logical: &Logical{
			Equality: &Equality{
				Comparison: &Comparison{
					Unary: &Unary{
						Primary: &Primary{
							CallExpression: &CallExpression{
								Identifier: &val1,
							},
						},
					},
				},
			},
			Op: "or",
			Next: &Logical{
				Equality: &Equality{
					Comparison: &Comparison{
						Unary: &Unary{
							Op: "!",
							Unary: &Unary{
								Primary: &Primary{
									CallExpression: &CallExpression{
										Identifier: &val2,
									},
								},
							},
						},
					},
				},
			},
		},
	}

	assert.Equal(t, expect, expr)
}

func TestParserBooleanLiterals(t *testing.T) {
	text := `
true and false
	`
	expr, err := Parse(text)
	if err != nil {
		t.Fatal(err.Error())
	}
	// repr.Println(expr)

	val1 := true

	expect := &Expression{
		Logical: &Logical{
			Equality: &Equality{
				Comparison: &Comparison{
					Unary: &Unary{
						Primary: &Primary{
							Bool: &val1,
						},
					},
				},
			},
			Op: "and",
			Next: &Logical{
				Equality: &Equality{
					Comparison: &Comparison{
						Unary: &Unary{
							Primary: &Primary{},
						},
					},
				},
			},
		},
	}

	assert.Equal(t, expect, expr)
}

func TestParserCompoundBoolean(t *testing.T) {
	text := `
true and 5 == a
	`
	expr, err := Parse(text)
	if err != nil {
		t.Fatal(err.Error())
	}
	// repr.Println(expr)

	val1 := true
	val2 := float64(5)
	val3 := "a"

	expect := &Expression{
		Logical: &Logical{
			Equality: &Equality{
				Comparison: &Comparison{
					Unary: &Unary{
						Primary: &Primary{
							Bool: &val1,
						},
					},
				},
			},
			Op: "and",
			Next: &Logical{
				Equality: &Equality{
					Comparison: &Comparison{
						Unary: &Unary{
							Primary: &Primary{
								Number: &val2,
							},
						},
					},
					Op: "==",
					Next: &Equality{
						Comparison: &Comparison{
							Unary: &Unary{
								Primary: &Primary{
									CallExpression: &CallExpression{
										Identifier: &val3,
									},
								},
							},
						},
					},
				},
			},
		},
	}

	assert.Equal(t, expect, expr)
}

func TestParserNegatedCompoundBoolean(t *testing.T) {
	text := `
true and !(5 == a)
	`
	expr, err := Parse(text)
	if err != nil {
		t.Fatal(err.Error())
	}
	// repr.Println(expr)

	val1 := true
	val2 := float64(5)
	val3 := "a"

	expect := &Expression{
		Logical: &Logical{
			Equality: &Equality{
				Comparison: &Comparison{
					Unary: &Unary{
						Primary: &Primary{
							Bool: &val1,
						},
					},
				},
			},
			Op: "and",
			Next: &Logical{
				Equality: &Equality{
					Comparison: &Comparison{
						Unary: &Unary{
							Op: "!",
							Unary: &Unary{
								Primary: &Primary{
									SubExpression: &Expression{
										Logical: &Logical{
											Equality: &Equality{
												Comparison: &Comparison{
													Unary: &Unary{
														Primary: &Primary{
															Number: &val2,
														},
													},
												},
												Op: "==",
												Next: &Equality{
													Comparison: &Comparison{
														Unary: &Unary{
															Primary: &Primary{
																CallExpression: &CallExpression{
																	Identifier: &val3,
																},
															},
														},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	assert.Equal(t, expect, expr)
}

func TestParserSubExpression(t *testing.T) {
	text := `
(a.b == "hello") and (x.y > 3.14)
	`
	expr, err := Parse(text)
	if err != nil {
		t.Fatal(err.Error())
	}
	// repr.Println(expr)

	val1 := "a.b"
	val2 := "\"hello\""
	val3 := "x.y"
	val4 := 3.14

	expect := &Expression{
		Logical: &Logical{
			Equality: &Equality{
				Comparison: &Comparison{
					Unary: &Unary{
						Primary: &Primary{
							SubExpression: &Expression{
								Logical: &Logical{
									Equality: &Equality{
										Comparison: &Comparison{
											Unary: &Unary{
												Primary: &Primary{
													CallExpression: &CallExpression{
														Identifier: &val1,
													},
												},
											},
										},
										Op: "==",
										Next: &Equality{
											Comparison: &Comparison{
												Unary: &Unary{
													Primary: &Primary{
														String: &val2,
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			Op: "and",
			Next: &Logical{
				Equality: &Equality{
					Comparison: &Comparison{
						Unary: &Unary{
							Primary: &Primary{
								SubExpression: &Expression{
									Logical: &Logical{
										Equality: &Equality{
											Comparison: &Comparison{
												Unary: &Unary{
													Primary: &Primary{
														CallExpression: &CallExpression{
															Identifier: &val3,
														},
													},
												},
												Op: ">",
												Next: &Comparison{
													Unary: &Unary{
														Primary: &Primary{
															Number: &val4,
														},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	assert.Equal(t, expect, expr)
}

func TestParserRegexLiteral(t *testing.T) {
	text := `
request == r"hello.*"
	`
	expr, err := Parse(text)
	if err != nil {
		t.Fatal(err.Error())
	}
	// repr.Println(expr)

	val1 := "request"
	val2 := "\"hello.*\""

	expect := &Expression{
		Logical: &Logical{
			Equality: &Equality{
				Comparison: &Comparison{
					Unary: &Unary{
						Primary: &Primary{
							CallExpression: &CallExpression{
								Identifier: &val1,
							},
						},
					},
				},
				Op: "==",
				Next: &Equality{
					Comparison: &Comparison{
						Unary: &Unary{
							Primary: &Primary{
								Regex: &val2,
							},
						},
					},
				},
			},
		},
	}

	assert.Equal(t, expect, expr)
}

func TestParserComplexQuery(t *testing.T) {
	text := `
http and request.method == "GET" and request.path == "/example" and (request.query.a == "b" or request.headers.x == "y")
	`
	expr, err := Parse(text)
	if err != nil {
		t.Fatal(err.Error())
	}
	// repr.Println(expr)

	val1 := "http"
	val2 := "request.method"
	val3 := "\"GET\""
	val4 := "request.path"
	val5 := "\"/example\""
	val6 := "request.query.a"
	val7 := "\"b\""
	val8 := "request.headers.x"
	val9 := "\"y\""

	expect := &Expression{
		Logical: &Logical{
			Equality: &Equality{
				Comparison: &Comparison{
					Unary: &Unary{
						Primary: &Primary{
							CallExpression: &CallExpression{
								Identifier: &val1,
							},
						},
					},
				},
			},
			Op: "and",
			Next: &Logical{
				Equality: &Equality{
					Comparison: &Comparison{
						Unary: &Unary{
							Primary: &Primary{
								CallExpression: &CallExpression{
									Identifier: &val2,
								},
							},
						},
					},
					Op: "==",
					Next: &Equality{
						Comparison: &Comparison{
							Unary: &Unary{
								Primary: &Primary{
									String: &val3,
								},
							},
						},
					},
				},
				Op: "and",
				Next: &Logical{
					Equality: &Equality{
						Comparison: &Comparison{
							Unary: &Unary{
								Primary: &Primary{
									CallExpression: &CallExpression{
										Identifier: &val4,
									},
								},
							},
						},
						Op: "==",
						Next: &Equality{
							Comparison: &Comparison{
								Unary: &Unary{
									Primary: &Primary{
										String: &val5,
									},
								},
							},
						},
					},
					Op: "and",
					Next: &Logical{
						Equality: &Equality{
							Comparison: &Comparison{
								Unary: &Unary{
									Primary: &Primary{
										SubExpression: &Expression{
											Logical: &Logical{
												Equality: &Equality{
													Comparison: &Comparison{
														Unary: &Unary{
															Primary: &Primary{
																CallExpression: &CallExpression{
																	Identifier: &val6,
																},
															},
														},
													},
													Op: "==",
													Next: &Equality{
														Comparison: &Comparison{
															Unary: &Unary{
																Primary: &Primary{
																	String: &val7,
																},
															},
														},
													},
												},
												Op: "or",
												Next: &Logical{
													Equality: &Equality{
														Comparison: &Comparison{
															Unary: &Unary{
																Primary: &Primary{
																	CallExpression: &CallExpression{
																		Identifier: &val8,
																	},
																},
															},
														},
														Op: "==",
														Next: &Equality{
															Comparison: &Comparison{
																Unary: &Unary{
																	Primary: &Primary{
																		String: &val9,
																	},
																},
															},
														},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	assert.Equal(t, expect, expr)
}

func TestParserSelectExpressionIndex(t *testing.T) {
	text := `
request.path[1] == "hello"
	`
	expr, err := Parse(text)
	if err != nil {
		t.Fatal(err.Error())
	}
	// repr.Println(expr)

	val1 := "request.path"
	val2 := 1
	val3 := "\"hello\""

	expect := &Expression{
		Logical: &Logical{
			Equality: &Equality{
				Comparison: &Comparison{
					Unary: &Unary{
						Primary: &Primary{
							CallExpression: &CallExpression{
								Identifier: &val1,
								SelectExpression: &SelectExpression{
									Index: &val2,
								},
							},
						},
					},
				},
				Op: "==",
				Next: &Equality{
					Comparison: &Comparison{
						Unary: &Unary{
							Primary: &Primary{
								String: &val3,
							},
						},
					},
				},
			},
		},
	}

	assert.Equal(t, expect, expr)
}

func TestParserSelectExpressionKey(t *testing.T) {
	text := `
!request.headers["user-agent"] == "kube-probe"
	`
	expr, err := Parse(text)
	if err != nil {
		t.Fatal(err.Error())
	}
	// repr.Println(expr)

	val1 := "request.headers"
	val2 := "\"user-agent\""
	val3 := "\"kube-probe\""

	expect := &Expression{
		Logical: &Logical{
			Equality: &Equality{
				Comparison: &Comparison{
					Unary: &Unary{
						Op: "!",
						Unary: &Unary{
							Primary: &Primary{
								CallExpression: &CallExpression{
									Identifier: &val1,
									SelectExpression: &SelectExpression{
										Key: &val2,
									},
								},
							},
						},
					},
				},
				Op: "==",
				Next: &Equality{
					Comparison: &Comparison{
						Unary: &Unary{
							Primary: &Primary{
								String: &val3,
							},
						},
					},
				},
			},
		},
	}

	assert.Equal(t, expect, expr)
}

func TestParserFunctionCall(t *testing.T) {
	text := `
a.b(3, 5)
	`
	expr, err := Parse(text)
	if err != nil {
		t.Fatal(err.Error())
	}
	// repr.Println(expr)

	val1 := "a.b"
	val2 := float64(3)
	val3 := float64(5)

	expect := &Expression{
		Logical: &Logical{
			Equality: &Equality{
				Comparison: &Comparison{
					Unary: &Unary{
						Primary: &Primary{
							CallExpression: &CallExpression{
								Identifier: &val1,
								Parameters: []*Parameter{
									&Parameter{
										Expression: &Expression{
											Logical: &Logical{
												Equality: &Equality{
													Comparison: &Comparison{
														Unary: &Unary{
															Primary: &Primary{
																Number: &val2,
															},
														},
													},
												},
											},
										},
									},
									&Parameter{
										Expression: &Expression{
											Logical: &Logical{
												Equality: &Equality{
													Comparison: &Comparison{
														Unary: &Unary{
															Primary: &Primary{
																Number: &val3,
															},
														},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	assert.Equal(t, expect, expr)
}

func TestParserSelectExpressionChainFunction(t *testing.T) {
	text := `
!http or !request.headers["user-agent"].startsWith("kube-probe")
	`
	expr, err := Parse(text)
	if err != nil {
		t.Fatal(err.Error())
	}
	// repr.Println(expr)

	val1 := "http"
	val2 := "request.headers"
	val3 := "\"user-agent\""
	val4 := "startsWith"
	val5 := "\"kube-probe\""

	expect := &Expression{
		Logical: &Logical{
			Equality: &Equality{
				Comparison: &Comparison{
					Unary: &Unary{
						Op: "!",
						Unary: &Unary{
							Primary: &Primary{
								CallExpression: &CallExpression{
									Identifier: &val1,
								},
							},
						},
					},
				},
			},
			Op: "or",
			Next: &Logical{
				Equality: &Equality{
					Comparison: &Comparison{
						Unary: &Unary{
							Op: "!",
							Unary: &Unary{
								Primary: &Primary{
									CallExpression: &CallExpression{
										Identifier: &val2,
										SelectExpression: &SelectExpression{
											Key: &val3,
											Expression: &Expression{
												Logical: &Logical{
													Equality: &Equality{
														Comparison: &Comparison{
															Unary: &Unary{
																Primary: &Primary{
																	CallExpression: &CallExpression{
																		Identifier: &val4,
																		Parameters: []*Parameter{
																			&Parameter{
																				Expression: &Expression{
																					Logical: &Logical{
																						Equality: &Equality{
																							Comparison: &Comparison{
																								Unary: &Unary{
																									Primary: &Primary{
																										String: &val5,
																									},
																								},
																							},
																						},
																					},
																				},
																			},
																		},
																	},
																},
															},
														},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	assert.Equal(t, expect, expr)
}

func TestParserRulesAssertionSyntax(t *testing.T) {
	text := `
rule(
	description: "Holy in name property",
	query: http and service == r"catalogue.*" and request.path == r"catalogue.*" and response.headers["content-type"].contains("application/json"),
	assert: response.body.name == "Holy"
)
and
rule(
	description: "Content Length header",
	query: http,
	assert: response.headers["content-length"] == r"(\\d+(?:\\.\\d+)?)"
)
and
rule(
	description: "Latency test",
	query: http and service == r"carts.*",
	assert: response.elapsedTime >= 1
)
	`
	expr, err := Parse(text)
	if err != nil {
		t.Fatal(err.Error())
	}
	// repr.Println(expr)

	val1 := "rule"
	val2 := "description"
	val3 := "\"Holy in name property\""
	val4 := "query"
	val5 := "http"
	val6 := "service"
	val7 := "\"catalogue.*\""
	val8 := "request.path"
	val9 := "response.headers"
	val10 := "\"content-type\""
	val11 := "contains"
	val12 := "\"application/json\""
	val13 := "assert"
	val14 := "response.body.name"
	val15 := "\"Holy\""
	val16 := "\"Content Length header\""
	val17 := "\"content-length\""
	val18 := "\"(\\\\d+(?:\\\\.\\\\d+)?)\""
	val19 := "\"Latency test\""
	val20 := "\"carts.*\""
	val21 := "response.elapsedTime"
	val22 := float64(1)

	expect := &Expression{
		Logical: &Logical{
			Equality: &Equality{
				Comparison: &Comparison{
					Unary: &Unary{
						Primary: &Primary{
							CallExpression: &CallExpression{
								Identifier: &val1,
								Parameters: []*Parameter{
									&Parameter{
										Tag: &val2,
										Expression: &Expression{
											Logical: &Logical{
												Equality: &Equality{
													Comparison: &Comparison{
														Unary: &Unary{
															Primary: &Primary{
																String: &val3,
															},
														},
													},
												},
											},
										},
									},
									&Parameter{
										Tag: &val4,
										Expression: &Expression{
											Logical: &Logical{
												Equality: &Equality{
													Comparison: &Comparison{
														Unary: &Unary{
															Primary: &Primary{
																CallExpression: &CallExpression{
																	Identifier: &val5,
																},
															},
														},
													},
												},
												Op: "and",
												Next: &Logical{
													Equality: &Equality{
														Comparison: &Comparison{
															Unary: &Unary{
																Primary: &Primary{
																	CallExpression: &CallExpression{
																		Identifier: &val6,
																	},
																},
															},
														},
														Op: "==",
														Next: &Equality{
															Comparison: &Comparison{
																Unary: &Unary{
																	Primary: &Primary{
																		Regex: &val7,
																	},
																},
															},
														},
													},
													Op: "and",
													Next: &Logical{
														Equality: &Equality{
															Comparison: &Comparison{
																Unary: &Unary{
																	Primary: &Primary{
																		CallExpression: &CallExpression{
																			Identifier: &val8,
																		},
																	},
																},
															},
															Op: "==",
															Next: &Equality{
																Comparison: &Comparison{
																	Unary: &Unary{
																		Primary: &Primary{
																			Regex: &val7,
																		},
																	},
																},
															},
														},
														Op: "and",
														Next: &Logical{
															Equality: &Equality{
																Comparison: &Comparison{
																	Unary: &Unary{
																		Primary: &Primary{
																			CallExpression: &CallExpression{
																				Identifier: &val9,
																				SelectExpression: &SelectExpression{
																					Key: &val10,
																					Expression: &Expression{
																						Logical: &Logical{
																							Equality: &Equality{
																								Comparison: &Comparison{
																									Unary: &Unary{
																										Primary: &Primary{
																											CallExpression: &CallExpression{
																												Identifier: &val11,
																												Parameters: []*Parameter{
																													&Parameter{
																														Expression: &Expression{
																															Logical: &Logical{
																																Equality: &Equality{
																																	Comparison: &Comparison{
																																		Unary: &Unary{
																																			Primary: &Primary{
																																				String: &val12,
																																			},
																																		},
																																	},
																																},
																															},
																														},
																													},
																												},
																											},
																										},
																									},
																								},
																							},
																						},
																					},
																				},
																			},
																		},
																	},
																},
															},
														},
													},
												},
											},
										},
									},
									&Parameter{
										Tag: &val13,
										Expression: &Expression{
											Logical: &Logical{
												Equality: &Equality{
													Comparison: &Comparison{
														Unary: &Unary{
															Primary: &Primary{
																CallExpression: &CallExpression{
																	Identifier: &val14,
																},
															},
														},
													},
													Op: "==",
													Next: &Equality{
														Comparison: &Comparison{
															Unary: &Unary{
																Primary: &Primary{
																	String: &val15,
																},
															},
														},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			Op: "and",
			Next: &Logical{
				Equality: &Equality{
					Comparison: &Comparison{
						Unary: &Unary{
							Primary: &Primary{
								CallExpression: &CallExpression{
									Identifier: &val1,
									Parameters: []*Parameter{
										&Parameter{
											Tag: &val2,
											Expression: &Expression{
												Logical: &Logical{
													Equality: &Equality{
														Comparison: &Comparison{
															Unary: &Unary{
																Primary: &Primary{
																	String: &val16,
																},
															},
														},
													},
												},
											},
										},
										&Parameter{
											Tag: &val4,
											Expression: &Expression{
												Logical: &Logical{
													Equality: &Equality{
														Comparison: &Comparison{
															Unary: &Unary{
																Primary: &Primary{
																	CallExpression: &CallExpression{
																		Identifier: &val5,
																	},
																},
															},
														},
													},
												},
											},
										},
										&Parameter{
											Tag: &val13,
											Expression: &Expression{
												Logical: &Logical{
													Equality: &Equality{
														Comparison: &Comparison{
															Unary: &Unary{
																Primary: &Primary{
																	CallExpression: &CallExpression{
																		Identifier: &val9,
																		SelectExpression: &SelectExpression{
																			Key: &val17,
																		},
																	},
																},
															},
														},
														Op: "==",
														Next: &Equality{
															Comparison: &Comparison{
																Unary: &Unary{
																	Primary: &Primary{
																		Regex: &val18,
																	},
																},
															},
														},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
				Op: "and",
				Next: &Logical{
					Equality: &Equality{
						Comparison: &Comparison{
							Unary: &Unary{
								Primary: &Primary{
									CallExpression: &CallExpression{
										Identifier: &val1,
										Parameters: []*Parameter{
											&Parameter{
												Tag: &val2,
												Expression: &Expression{
													Logical: &Logical{
														Equality: &Equality{
															Comparison: &Comparison{
																Unary: &Unary{
																	Primary: &Primary{
																		String: &val19,
																	},
																},
															},
														},
													},
												},
											},
											&Parameter{
												Tag: &val4,
												Expression: &Expression{
													Logical: &Logical{
														Equality: &Equality{
															Comparison: &Comparison{
																Unary: &Unary{
																	Primary: &Primary{
																		CallExpression: &CallExpression{
																			Identifier: &val5,
																		},
																	},
																},
															},
														},
														Op: "and",
														Next: &Logical{
															Equality: &Equality{
																Comparison: &Comparison{
																	Unary: &Unary{
																		Primary: &Primary{
																			CallExpression: &CallExpression{
																				Identifier: &val6,
																			},
																		},
																	},
																},
																Op: "==",
																Next: &Equality{
																	Comparison: &Comparison{
																		Unary: &Unary{
																			Primary: &Primary{
																				Regex: &val20,
																			},
																		},
																	},
																},
															},
														},
													},
												},
											},
											&Parameter{
												Tag: &val13,
												Expression: &Expression{
													Logical: &Logical{
														Equality: &Equality{
															Comparison: &Comparison{
																Unary: &Unary{
																	Primary: &Primary{
																		CallExpression: &CallExpression{
																			Identifier: &val21,
																		},
																	},
																},
																Op: ">=",
																Next: &Comparison{
																	Unary: &Unary{
																		Primary: &Primary{
																			Number: &val22,
																		},
																	},
																},
															},
														},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	assert.Equal(t, expect, expr)
}

func TestParserSyntaxErrorLiteralNotTerminated(t *testing.T) {
	text := `
=.="
	`
	_, err := Parse(text)
	assert.EqualError(t, err, "2:5: literal not terminated")
}

func TestParserSyntaxErrorUnexpectedToken(t *testing.T) {
	text := `
request.path[3.14] == "hello"
	`
	_, err := Parse(text)
	assert.EqualError(t, err, "2:14: unexpected token \"3.14\" (expected (<string> | <char> | <rawstring>) \"]\")")
}
