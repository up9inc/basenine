package basenine

import (
	"encoding/base64"
	"fmt"
	"testing"
	"time"

	"github.com/ohler55/ojg/jp"
	oj "github.com/ohler55/ojg/oj"
	"github.com/stretchr/testify/assert"
)

var data = []struct {
	query   string
	json    string
	truth   bool
	limit   uint64
	rlimit  uint64
	leftOff int64
	newJson string
}{
	{`true and true`, `{}`, true, 0, 0, 0, `{}`},
	{`true and false`, `{}`, false, 0, 0, 0, `{}`},
	{`false and true`, `{}`, false, 0, 0, 0, `{}`},
	{`false and false`, `{}`, false, 0, 0, 0, `{}`},
	{`true or true`, `{}`, true, 0, 0, 0, `{}`},
	{`false or true`, `{}`, true, 0, 0, 0, `{}`},
	{`true or false`, `{}`, true, 0, 0, 0, `{}`},
	{`false or false`, `{}`, false, 0, 0, 0, `{}`},
	{`true and 5`, `{}`, true, 0, 0, 0, `{}`},
	{`false and 5`, `{}`, false, 0, 0, 0, `{}`},
	{`true and 0`, `{}`, false, 0, 0, 0, `{}`},
	{`5 == 5`, `{}`, true, 0, 0, 0, `{}`},
	{`3 == 5`, `{}`, false, 0, 0, 0, `{}`},
	{`"abc" == "abc"`, `{}`, true, 0, 0, 0, `{}`},
	{`"abc" == "xyz"`, `{}`, false, 0, 0, 0, `{}`},
	{`"abc" != "xyz"`, `{}`, true, 0, 0, 0, `{}`},
	{`"abc" != "abc"`, `{}`, false, 0, 0, 0, `{}`},
	{`true == true`, `{}`, true, 0, 0, 0, `{}`},
	{`true != true`, `{}`, false, 0, 0, 0, `{}`},
	{`true == false`, `{}`, false, 0, 0, 0, `{}`},
	{`3.14 == 3.14`, `{}`, true, 0, 0, 0, `{}`},
	{`3.14 == 42`, `{}`, false, 0, 0, 0, `{}`},
	{`42 > 41`, `{}`, true, 0, 0, 0, `{}`},
	{`42 >= 42`, `{}`, true, 0, 0, 0, `{}`},
	{`41 >= 42`, `{}`, false, 0, 0, 0, `{}`},
	{`13 < 42`, `{}`, true, 0, 0, 0, `{}`},
	{`42 < 13`, `{}`, false, 0, 0, 0, `{}`},
	{`!true`, `{}`, false, 0, 0, 0, `{}`},
	{`-300 < 42`, `{}`, true, 0, 0, 0, `{}`},
	{`true and !(5 == a)`, `{"a": 4}`, true, 0, 0, 0, `{"a": 4}`},
	{`true and !(5 == a)`, `{"a": 5}`, false, 0, 0, 0, `{"a": 5}`},
	{`(a.b == "hello") and (x.y > 3.14)`, `{"a":{"b":"hello"},"x":{"y":3.15}}`, true, 0, 0, 0, `{"a":{"b":"hello"},"x":{"y":3.15}}`},
	{`(a.b == "hello") and (x.y > 3.14)`, `{"a":{"b":"hello"},"x":{"y":3.13}}`, false, 0, 0, 0, `{"a":{"b":"hello"},"x":{"y":3.13}}`},
	{`(a.b == "hello") and (x.y > 3.14)`, `{"a":{"b":"mello"},"x":{"y":3.15}}`, false, 0, 0, 0, `{"a":{"b":"mello"},"x":{"y":3.15}}`},
	{`brand.name == "Chevrolet"`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, true, 0, 0, 0, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`},
	{`brand.name != "Chevrolet"`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, false, 0, 0, 0, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`},
	{`brand.game == "Chevrolet"`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, false, 0, 0, 0, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`},
	{`brand.name == r"Chev.*"`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, true, 0, 0, 0, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`},
	{`brand.name != r"Chev.*"`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, false, 0, 0, 0, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`},
	{`brand.name == r"Bug.*"`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, false, 0, 0, 0, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`},
	{`brand.name != r"Bug.*"`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, true, 0, 0, 0, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`},
	{`request.path[1] == "v1"`, `{"request":{"path":["api","v1","example"]}}`, true, 0, 0, 0, `{"request":{"path":["api","v1","example"]}}`},
	{`request.path[1] != "v1"`, `{"request":{"path":["api","v1","example"]}}`, false, 0, 0, 0, `{"request":{"path":["api","v1","example"]}}`},
	{`request.headers["a"] == "b"`, `{"request":{"path":["api","v1","example"],"headers":{"a":"b","c":"d"}}}`, true, 0, 0, 0, `{"request":{"path":["api","v1","example"],"headers":{"a":"b","c":"d"}}}`},
	{`request.headers["a"] != "b"`, `{"request":{"path":["api","v1","example"],"headers":{"a":"b","c":"d"}}}`, false, 0, 0, 0, `{"request":{"path":["api","v1","example"],"headers":{"a":"b","c":"d"}}}`},
	{`request.headers["a"] == "d"`, `{"request":{"path":["api","v1","example"],"headers":{"a":"b","c":"d"}}}`, false, 0, 0, 0, `{"request":{"path":["api","v1","example"],"headers":{"a":"b","c":"d"}}}`},
	{`request.headers["e"].x == "y"`, `{"request":{"path":["api","v1","example"],"headers":{"a":"b","c":"d","e":{"x":"y"}}}}`, true, 0, 0, 0, `{"request":{"path":["api","v1","example"],"headers":{"a":"b","c":"d","e":{"x":"y"}}}}`},
	{`request.headers["e"].x == "z"`, `{"request":{"path":["api","v1","example"],"headers":{"a":"b","c":"d","e":{"x":"y"}}}}`, false, 0, 0, 0, `{"request":{"path":["api","v1","example"],"headers":{"a":"b","c":"d","e":{"x":"y"}}}}`},
	{`request.headers["e"].x != "y"`, `{"request":{"path":["api","v1","example"],"headers":{"a":"b","c":"d","e":{"x":"y"}}}}`, false, 0, 0, 0, `{"request":{"path":["api","v1","example"],"headers":{"a":"b","c":"d","e":{"x":"y"}}}}`},
	{`request.headers["e"].x != "z"`, `{"request":{"path":["api","v1","example"],"headers":{"a":"b","c":"d","e":{"x":"y"}}}}`, true, 0, 0, 0, `{"request":{"path":["api","v1","example"],"headers":{"a":"b","c":"d","e":{"x":"y"}}}}`},
	{`brand.name.startsWith("Chev")`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, true, 0, 0, 0, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`},
	{`brand.name.startsWith("hev")`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, false, 0, 0, 0, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`},
	{`brand.name.endsWith("let")`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, true, 0, 0, 0, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`},
	{`brand.name.endsWith("le")`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, false, 0, 0, 0, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`},
	{`brand.name.contains("ro")`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, true, 0, 0, 0, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`},
	{`brand.name.contains("hello")`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, false, 0, 0, 0, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`},
	{`brand["name"].startsWith("Chev")`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, true, 0, 0, 0, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`},
	{`brand["name"].startsWithx("Chev")`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, false, 0, 0, 0, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`},
	{`brand["name"].startsWith("hev")`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, false, 0, 0, 0, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`},
	{`timestamp > datetime("10/19/2021, 6:29:02.000 PM")`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"timestamp":1634668524000}`, true, 0, 0, 0, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"timestamp":1634668524000}`},
	{`timestamp > datetime("10/19/2021, 7:29:02.999 PM")`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"timestamp":1634668524000}`, false, 0, 0, 0, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"timestamp":1634668524000}`},
	{`request.headers["a"] == "b" and request.path[1] == "v1"`, `{"request":{"path":["api","v1","example"],"headers":{"a":"b","c":"d"}}}`, true, 0, 0, 0, `{"request":{"path":["api","v1","example"],"headers":{"a":"b","c":"d"}}}`},
	{`year == salesYear`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021,"salesYear":2021}`, true, 0, 0, 0, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021,"salesYear":2021}`},
	{`year == salesYear`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021,"salesYear":2020}`, false, 0, 0, 0, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021,"salesYear":2020}`},
	{`year != salesYear`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021,"salesYear":2020}`, true, 0, 0, 0, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021,"salesYear":2020}`},
	{`year != salesYear`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021,"salesYear":2021}`, false, 0, 0, 0, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021,"salesYear":2021}`},
	{`year > salesYear`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021,"salesYear":2020}`, true, 0, 0, 0, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021,"salesYear":2020}`},
	{`year > salesYear`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021,"salesYear":2022}`, false, 0, 0, 0, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021,"salesYear":2022}`},
	{`brand.name == "Chevrolet" and year == 2021`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, true, 0, 0, 0, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`},
	{`brand.name == "Chevrolet" and year == 2021`, `{"id":114905,"model":"Camaro","trend":{"name":"Chevrolet"},"year":2021}`, false, 0, 0, 0, `{"id":114905,"model":"Camaro","trend":{"name":"Chevrolet"},"year":2021}`},
	{`year == 2021 and brand.name == "Chevrolet"`, `{"id":114905,"model":"Camaro","trend":{"name":"Chevrolet"},"year":2021}`, false, 0, 0, 0, `{"id":114905,"model":"Camaro","trend":{"name":"Chevrolet"},"year":2021}`},
	{`model == nil`, `{"id":114905,"model":null,"brand":{"name":"Chevrolet"},"year":2021}`, true, 0, 0, 0, `{"id":114905,"model":null,"brand":{"name":"Chevrolet"},"year":2021}`},
	{`model != nil`, `{"id":114905,"model":null,"brand":{"name":"Chevrolet"},"year":2021}`, false, 0, 0, 0, `{"id":114905,"model":null,"brand":{"name":"Chevrolet"},"year":2021}`},
	{`model == nil`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, false, 0, 0, 0, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`},
	{`model != nil`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, true, 0, 0, 0, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`},
	{`model == "\"hello world\";v=\"42\", "`, `{"brand":{"name":"Chevrolet"},"id":27502,"model":"\\\"hello world\\\";v=\\\"42\\\", ","year":2021}`, true, 0, 0, 0, `{"brand":{"name":"Chevrolet"},"id":27502,"model":"\\\"hello world\\\";v=\\\"42\\\", ","year":2021}`},
	{`brand.name == "Chevrolet" and limit(100)`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, true, 100, 0, 0, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`},
	{`limit(100) and brand.name == "Chevrolet"`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, true, 100, 0, 0, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`},
	{`brand.name != "Chevrolet" and limit(100)`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, false, 100, 0, 0, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`},
	{`brand.name == "Chevrolet" and rlimit(100)`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, true, 0, 100, 0, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`},
	{`rlimit(100) and brand.name == "Chevrolet"`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, true, 0, 100, 0, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`},
	{`brand.name == "Chevrolet" and leftOff(1000)`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, true, 0, 0, 1000, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`},
	{`leftOff(1000) and brand.name == "Chevrolet"`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, true, 0, 0, 1000, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`},
	{`brand.name.startsWith()`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, false, 0, 0, 0, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`},
	{`brand.name.endsWith()`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, false, 0, 0, 0, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`},
	{`brand.name.contains()`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, false, 0, 0, 0, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`},
	{`datetime()`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"timestamp":1634668524000}`, false, 0, 0, 0, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"timestamp":1634668524000}`},
	{`!brand.name.startsWith("Chev")`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, false, 0, 0, 0, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`},
	{`!brand.name.startsWith("hev")`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, true, 0, 0, 0, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`},
	{`response.body.json().brand.name == "Chevrolet"`, `{"response":{"body":"{\"id\":114905,\"model\":\"Camaro\",\"brand\":{\"name\":\"Chevrolet\"},\"year\":2021}"}}`, true, 0, 0, 0, `{"response":{"body":"{\"id\":114905,\"model\":\"Camaro\",\"brand\":{\"name\":\"Chevrolet\"},\"year\":2021}"}}`},
	{`response.body.json().brand.name == "ChevroletX"`, `{"response":{"body":"{\"id\":114905,\"model\":\"Camaro\",\"brand\":{\"name\":\"Chevrolet\"},\"year\":2021}"}}`, false, 0, 0, 0, `{"response":{"body":"{\"id\":114905,\"model\":\"Camaro\",\"brand\":{\"name\":\"Chevrolet\"},\"year\":2021}"}}`},
	{`response.body.json().trend.name == "Chevrolet"`, `{"response":{"body":"{\"id\":114905,\"model\":\"Camaro\",\"brand\":{\"name\":\"Chevrolet\"},\"year\":2021}"}}`, false, 0, 0, 0, `{"response":{"body":"{\"id\":114905,\"model\":\"Camaro\",\"brand\":{\"name\":\"Chevrolet\"},\"year\":2021}"}}`},
	{`response.body.json().brand.name == "Chevrolet"`, `{"response":{"body":"INVALID JSON"}}`, false, 0, 0, 0, `{"response":{"body":"INVALID JSON"}}`},
	{`response.body.json() == "INVALID JSON"`, `{"response":{"body":"INVALID JSON"}}`, false, 0, 0, 0, `{"response":{"body":"INVALID JSON"}}`},
	{`response.body.json().key[0] == "api"`, `{"response":{"body":"{\"key\":[\"api\",\"v1\",\"example\"]}"}}`, true, 0, 0, 0, `{"response":{"body":"{\"key\":[\"api\",\"v1\",\"example\"]}"}}`},
	{`response.body.json()[0] == "api"`, `{"response":{"body":"[\"api\",\"v1\",\"example\"]"}}`, true, 0, 0, 0, `{"response":{"body":"[\"api\",\"v1\",\"example\"]"}}`},
	{`response.body.json()[0] == "v1"`, `{"response":{"body":"[\"api\",\"v1\",\"example\"]"}}`, false, 0, 0, 0, `{"response":{"body":"[\"api\",\"v1\",\"example\"]"}}`},
	{`response.body.json()["model"] == "Camaro"`, `{"response":{"body":"{\"id\":114905,\"model\":\"Camaro\",\"brand\":{\"name\":\"Chevrolet\"},\"year\":2021}"}}`, true, 0, 0, 0, `{"response":{"body":"{\"id\":114905,\"model\":\"Camaro\",\"brand\":{\"name\":\"Chevrolet\"},\"year\":2021}"}}`},
	{`response.body.json()["model"] == "CamaroX"`, `{"response":{"body":"{\"id\":114905,\"model\":\"Camaro\",\"brand\":{\"name\":\"Chevrolet\"},\"year\":2021}"}}`, false, 0, 0, 0, `{"response":{"body":"{\"id\":114905,\"model\":\"Camaro\",\"brand\":{\"name\":\"Chevrolet\"},\"year\":2021}"}}`},
	{`response.body.json().brand.name == "Chevrolet"`, `{"response":{"body":"eyJpZCI6MTE0OTA1LCJtb2RlbCI6IkNhbWFybyIsImJyYW5kIjp7Im5hbWUiOiJDaGV2cm9sZXQifSwieWVhciI6MjAyMX0="}}`, true, 0, 0, 0, `{"response":{"body":"eyJpZCI6MTE0OTA1LCJtb2RlbCI6IkNhbWFybyIsImJyYW5kIjp7Im5hbWUiOiJDaGV2cm9sZXQifSwieWVhciI6MjAyMX0="}}`},
	{`id == 114905 and redact("model", "brand.name")`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, true, 0, 0, 0, fmt.Sprintf(`{"id":114905,"model":"%s","brand":{"name":"%s"},"year":2021}`, REDACTED, REDACTED)},
	{`id == 114905 and redact("modelx", "brand.name")`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, true, 0, 0, 0, fmt.Sprintf(`{"id":114905,"model":"Camaro","brand":{"name":"%s"},"year":2021}`, REDACTED)},
	{`id == 114906 and redact("model", "brand.name")`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, false, 0, 0, 0, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`},
	{`redact("model", "brand.name") and id == 114906`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, false, 0, 0, 0, fmt.Sprintf(`{"id":114905,"model":"%s","brand":{"name":"%s"},"year":2021}`, REDACTED, REDACTED)},
	{`redact("id", "brand.name") and id == 114905`, `{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"year":2021}`, false, 0, 0, 0, fmt.Sprintf(`{"id":"%s","model":"Camaro","brand":{"name":"%s"},"year":2021}`, REDACTED, REDACTED)},
	{`request.path.* == "v1"`, `{"request":{"path":["api","v1","example"]}}`, true, 0, 0, 0, `{"request":{"path":["api","v1","example"]}}`},
	{`request.path.* == "v2"`, `{"request":{"path":["api","v1","example"]}}`, false, 0, 0, 0, `{"request":{"path":["api","v1","example"]}}`},
	{`request.path.* != "v2"`, `{"request":{"path":["api","v1","example"]}}`, true, 0, 0, 0, `{"request":{"path":["api","v1","example"]}}`},
	{`request.path.* == "v2"`, `{"request":{"path":["api","v1","example"]}}`, false, 0, 0, 0, `{"request":{"path":["api","v1","example"]}}`},
	{`request.path.* > 2`, `{"request":{"path":[1, 2, 3]}}`, true, 0, 0, 0, `{"request":{"path":[1, 2, 3]}}`},
	{`request.path.* > 4`, `{"request":{"path":[1, 2, 3]}}`, false, 0, 0, 0, `{"request":{"path":[1, 2, 3]}}`},
	{`request.path.* < 2`, `{"request":{"path":[1, 2, 3]}}`, true, 0, 0, 0, `{"request":{"path":[1, 2, 3]}}`},
	{`request.path.* < 0`, `{"request":{"path":[1, 2, 3]}}`, false, 0, 0, 0, `{"request":{"path":[1, 2, 3]}}`},
	{`request.path.* >= 2`, `{"request":{"path":[1, 2, 3]}}`, true, 0, 0, 0, `{"request":{"path":[1, 2, 3]}}`},
	{`request.path.* >= 4`, `{"request":{"path":[1, 2, 3]}}`, false, 0, 0, 0, `{"request":{"path":[1, 2, 3]}}`},
	{`request.path.* <= 2`, `{"request":{"path":[1, 2, 3]}}`, true, 0, 0, 0, `{"request":{"path":[1, 2, 3]}}`},
	{`request.path.* <= 0`, `{"request":{"path":[1, 2, 3]}}`, false, 0, 0, 0, `{"request":{"path":[1, 2, 3]}}`},
	{`request.path.*.x > 2`, `{"request":{"path":[{"x":1}, {"x":2}, {"x":3}]}}`, true, 0, 0, 0, `{"request":{"path":[{"x":1}, {"x":2}, {"x":3}]}}`},
	{`request.path.*.x > 4`, `{"request":{"path":[{"x":1}, {"x":2}, {"x":3}]}}`, false, 0, 0, 0, `{"request":{"path":[{"x":1}, {"x":2}, {"x":3}]}}`},
	{`request.path.*.x and true`, `{"request":{"path":[{"x":1}, {"x":2}, {"x":3}]}}`, true, 0, 0, 0, `{"request":{"path":[{"x":1}, {"x":2}, {"x":3}]}}`},
	{`request.path.*.x and true`, `{"request":{"path":[]}}`, false, 0, 0, 0, `{"request":{"path":[]}}`},
	{`request.path.* == request.path.*`, `{"request":{"path":[1, 2, 3]}}`, true, 0, 0, 0, `{"request":{"path":[1, 2, 3]}}`},
	{`request.path.* != request.path.*`, `{"request":{"path":[1, 2, 3]}}`, false, 0, 0, 0, `{"request":{"path":[1, 2, 3]}}`},
	{`request.path.* > request.path.*`, `{"request":{"path":[1, 2, 3]}}`, false, 0, 0, 0, `{"request":{"path":[1, 2, 3]}}`},
	{`request.path.* > response.header.*`, `{"request":{"path":[1, 2, 3]},"response":{"header":[-1, -2, -3]}}`, true, 0, 0, 0, `{"request":{"path":[1, 2, 3]},"response":{"header":[-1, -2, -3]}}`},
	{`request.path.* < request.path.*`, `{"request":{"path":[1, 2, 3]}}`, false, 0, 0, 0, `{"request":{"path":[1, 2, 3]}}`},
	{`response.header.* < request.path.*`, `{"request":{"path":[1, 2, 3]},"response":{"header":[-1, -2, -3]}}`, true, 0, 0, 0, `{"request":{"path":[1, 2, 3]},"response":{"header":[-1, -2, -3]}}`},
	{`request.path.* >= request.path.*`, `{"request":{"path":[1, 2, 3]}}`, false, 0, 0, 0, `{"request":{"path":[1, 2, 3]}}`},
	{`request.path.* >= response.header.*`, `{"request":{"path":[1, 2, 3]},"response":{"header":[-1, -2, -3]}}`, true, 0, 0, 0, `{"request":{"path":[1, 2, 3]},"response":{"header":[-1, -2, -3]}}`},
	{`request.path.* <= request.path.*`, `{"request":{"path":[1, 2, 3]}}`, false, 0, 0, 0, `{"request":{"path":[1, 2, 3]}}`},
	{`response.header.* <= request.path.*`, `{"request":{"path":[1, 2, 3]},"response":{"header":[-1, -2, -3]}}`, true, 0, 0, 0, `{"request":{"path":[1, 2, 3]},"response":{"header":[-1, -2, -3]}}`},
}

func TestEval(t *testing.T) {
	for _, row := range data {
		expr, err := Parse(row.query)
		if err != nil {
			t.Fatal(err.Error())
		}
		// repr.Println(expr)
		prop, err := Precompute(expr)
		if err != nil {
			t.Fatal(err.Error())
		}
		assert.Equal(t, row.limit, prop.Limit)
		assert.Equal(t, row.rlimit, prop.Rlimit)
		assert.Equal(t, row.leftOff, prop.LeftOff)
		// repr.Println(expr)
		truth, newJson, err := Eval(expr, row.json)
		if err != nil {
			t.Fatal(err.Error())
		}
		if row.truth {
			assert.True(t, truth, fmt.Sprintf("Query: `%s` JSON: %s", row.query, row.json))
		} else {
			assert.False(t, truth, fmt.Sprintf("Query: `%s` JSON: %s", row.query, row.json))
		}
		assert.JSONEq(t, row.newJson, newJson)
	}
}

var dataXml = []struct {
	query string
	truth bool
}{
	{`response.body.xml().bookstore.book[1].title == "Harry Potter"`, true},
	{`response.body.xml().bookstore.book[1].title == "Lord of the Rings"`, false},
}

func TestEvalXml(t *testing.T) {
	for _, row := range dataXml {
		json := `{"response":{"body":"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n<bookstore><book category=\"cooking\"><title lang=\"en\">Everyday Italian</title><author>Giada De Laurentiis</author><year>2005</year><price>30.00</price></book><book category=\"children\"><title lang=\"en\">Harry Potter</title><author>J K. Rowling</author><year>2005</year><price>29.99</price></book><book category=\"web\"><title lang=\"en\">XQuery Kick Start</title><author>James McGovern</author><author>Per Bothner</author><author>Kurt Cagle</author><author>James Linn</author><author>Vaidyanathan Nagarajan</author><year>2003</year><price>49.99</price></book><book category=\"web\"><title lang=\"en\">Learning XML</title><author>Erik T. Ray</author><year>2003</year><price>39.95</price></book></bookstore>\r\n"}}`

		expr, err := Parse(row.query)
		if err != nil {
			t.Fatal(err.Error())
		}

		_, err = Precompute(expr)
		if err != nil {
			t.Fatal(err.Error())
		}

		truth, _, err := Eval(expr, json)
		if err != nil {
			t.Fatal(err.Error())
		}

		if row.truth {
			assert.True(t, truth, fmt.Sprintf("Query: `%s` JSON: %s", row.query, json))
		} else {
			assert.False(t, truth, fmt.Sprintf("Query: `%s` JSON: %s", row.query, json))
		}
	}
}

var dataRedact = []struct {
	query      string
	truth      bool
	json       string
	expected   string
	strCompare bool
}{
	{`redact("response.body.json().model")`, true, `{"response":{"body":"{\"id\":114905,\"model\":\"Camaro\",\"brand\":{\"name\":\"Chevrolet\"},\"year\":2021}"}}`, fmt.Sprintf(`{"id":114905,"model":"%s","brand":{"name":"Chevrolet"},"year":2021}`, REDACTED), false},
	{`redact("response.body.json().model")`, true, `{"response":{"body":"eyJpZCI6MTE0OTA1LCJtb2RlbCI6IkNhbWFybyIsImJyYW5kIjp7Im5hbWUiOiJDaGV2cm9sZXQifSwieWVhciI6MjAyMX0="}}`, `eyJpZCI6MTE0OTA1LCJtb2RlbCI6IltSRURBQ1RFRF0iLCJicmFuZCI6eyJuYW1lIjoiQ2hldnJvbGV0In0sInllYXIiOjIwMjF9`, false},
	{`redact("response.body.xml().bookstore.book[1].title")`, true, `{"response":{"body":"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n<bookstore><book category=\"cooking\"><title lang=\"en\">Everyday Italian</title><author>Giada De Laurentiis</author><year>2005</year><price>30.00</price></book><book category=\"children\"><title lang=\"en\">Harry Potter</title><author>J K. Rowling</author><year>2005</year><price>29.99</price></book><book category=\"web\"><title lang=\"en\">XQuery Kick Start</title><author>James McGovern</author><author>Per Bothner</author><author>Kurt Cagle</author><author>James Linn</author><author>Vaidyanathan Nagarajan</author><year>2003</year><price>49.99</price></book><book category=\"web\"><title lang=\"en\">Learning XML</title><author>Erik T. Ray</author><year>2003</year><price>39.95</price></book></bookstore>\r\n"}}`, `<?xml version="1.0" encoding="UTF-8"?>
<bookstore><book category="cooking"><author>Giada De Laurentiis</author><price>30.00</price><title lang="en">Everyday Italian</title><year>2005</year></book><book category="children"><author>J K. Rowling</author><price>29.99</price><title>[REDACTED]</title><year>2005</year></book><book category="web"><author>James McGovern</author><author>Per Bothner</author><author>Kurt Cagle</author><author>James Linn</author><author>Vaidyanathan Nagarajan</author><price>49.99</price><title lang="en">XQuery Kick Start</title><year>2003</year></book><book category="web"><author>Erik T. Ray</author><price>39.95</price><title lang="en">Learning XML</title><year>2003</year></book></bookstore>`, true},
	{`redact("response.body.xml().bookstore.book[1].title")`, true, `{"response":{"body":"PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0iVVRGLTgiPz4KPGJvb2tzdG9yZT48Ym9vayBjYXRlZ29yeT0iY29va2luZyI+PHRpdGxlIGxhbmc9ImVuIj5FdmVyeWRheSBJdGFsaWFuPC90aXRsZT48YXV0aG9yPkdpYWRhIERlIExhdXJlbnRpaXM8L2F1dGhvcj48eWVhcj4yMDA1PC95ZWFyPjxwcmljZT4zMC4wMDwvcHJpY2U+PC9ib29rPjxib29rIGNhdGVnb3J5PSJjaGlsZHJlbiI+PHRpdGxlIGxhbmc9ImVuIj5IYXJyeSBQb3R0ZXI8L3RpdGxlPjxhdXRob3I+SiBLLiBSb3dsaW5nPC9hdXRob3I+PHllYXI+MjAwNTwveWVhcj48cHJpY2U+MjkuOTk8L3ByaWNlPjwvYm9vaz48Ym9vayBjYXRlZ29yeT0id2ViIj48dGl0bGUgbGFuZz0iZW4iPlhRdWVyeSBLaWNrIFN0YXJ0PC90aXRsZT48YXV0aG9yPkphbWVzIE1jR292ZXJuPC9hdXRob3I+PGF1dGhvcj5QZXIgQm90aG5lcjwvYXV0aG9yPjxhdXRob3I+S3VydCBDYWdsZTwvYXV0aG9yPjxhdXRob3I+SmFtZXMgTGlubjwvYXV0aG9yPjxhdXRob3I+VmFpZHlhbmF0aGFuIE5hZ2FyYWphbjwvYXV0aG9yPjx5ZWFyPjIwMDM8L3llYXI+PHByaWNlPjQ5Ljk5PC9wcmljZT48L2Jvb2s+PGJvb2sgY2F0ZWdvcnk9IndlYiI+PHRpdGxlIGxhbmc9ImVuIj5MZWFybmluZyBYTUw8L3RpdGxlPjxhdXRob3I+RXJpayBULiBSYXk8L2F1dGhvcj48eWVhcj4yMDAzPC95ZWFyPjxwcmljZT4zOS45NTwvcHJpY2U+PC9ib29rPjwvYm9va3N0b3JlPgo="}}`, `PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0iVVRGLTgiPz4KPGJvb2tzdG9yZT48Ym9vayBjYXRlZ29yeT0iY29va2luZyI+PGF1dGhvcj5HaWFkYSBEZSBMYXVyZW50aWlzPC9hdXRob3I+PHByaWNlPjMwLjAwPC9wcmljZT48dGl0bGUgbGFuZz0iZW4iPkV2ZXJ5ZGF5IEl0YWxpYW48L3RpdGxlPjx5ZWFyPjIwMDU8L3llYXI+PC9ib29rPjxib29rIGNhdGVnb3J5PSJjaGlsZHJlbiI+PGF1dGhvcj5KIEsuIFJvd2xpbmc8L2F1dGhvcj48cHJpY2U+MjkuOTk8L3ByaWNlPjx0aXRsZT5bUkVEQUNURURdPC90aXRsZT48eWVhcj4yMDA1PC95ZWFyPjwvYm9vaz48Ym9vayBjYXRlZ29yeT0id2ViIj48YXV0aG9yPkphbWVzIE1jR292ZXJuPC9hdXRob3I+PGF1dGhvcj5QZXIgQm90aG5lcjwvYXV0aG9yPjxhdXRob3I+S3VydCBDYWdsZTwvYXV0aG9yPjxhdXRob3I+SmFtZXMgTGlubjwvYXV0aG9yPjxhdXRob3I+VmFpZHlhbmF0aGFuIE5hZ2FyYWphbjwvYXV0aG9yPjxwcmljZT40OS45OTwvcHJpY2U+PHRpdGxlIGxhbmc9ImVuIj5YUXVlcnkgS2ljayBTdGFydDwvdGl0bGU+PHllYXI+MjAwMzwveWVhcj48L2Jvb2s+PGJvb2sgY2F0ZWdvcnk9IndlYiI+PGF1dGhvcj5FcmlrIFQuIFJheTwvYXV0aG9yPjxwcmljZT4zOS45NTwvcHJpY2U+PHRpdGxlIGxhbmc9ImVuIj5MZWFybmluZyBYTUw8L3RpdGxlPjx5ZWFyPjIwMDM8L3llYXI+PC9ib29rPjwvYm9va3N0b3JlPg==`, true},
}

func TestEvalRedactJson(t *testing.T) {
	for _, row := range dataRedact {
		expr, err := Parse(row.query)
		if err != nil {
			t.Fatal(err.Error())
		}

		_, err = Precompute(expr)
		if err != nil {
			t.Fatal(err.Error())
		}

		truth, newJson, err := Eval(expr, row.json)
		if err != nil {
			t.Fatal(err.Error())
		}

		if row.truth {
			assert.True(t, truth, fmt.Sprintf("Query: `%s` JSON: %s", row.query, row.json))
		} else {
			assert.False(t, truth, fmt.Sprintf("Query: `%s` JSON: %s", row.query, row.json))
		}

		newObj, err := oj.ParseString(newJson)
		assert.Nil(t, err)

		jsonPath, err := jp.ParseString("response.body")
		assert.Nil(t, err)

		nested := jsonPath.Get(newObj)[0].(string)

		base64DecodedNestedJson, err := base64.StdEncoding.DecodeString(nested)
		if err == nil {
			nested = string(base64DecodedNestedJson)
		}

		base64DecodedExpected, err := base64.StdEncoding.DecodeString(row.expected)
		if err == nil {
			row.expected = string(base64DecodedExpected)
		}

		if row.strCompare {
			assert.Equal(t, row.expected, nested)
		} else {
			assert.JSONEq(t, row.expected, nested)
		}
	}
}

var dataTimeHelpers = []struct {
	query string
	truth bool
}{
	{`timestamp <= now()`, true},
	{`timestamp >= now()`, false},
	{`timestamp <= seconds(-5)`, false},
	{`timestamp >= seconds(-5)`, true},
	{`timestamp <= minutes(-5)`, false},
	{`timestamp >= minutes(-5)`, true},
	{`timestamp <= hours(-5)`, false},
	{`timestamp >= hours(-5)`, true},
	{`timestamp <= days(-5)`, false},
	{`timestamp >= days(-5)`, true},
	{`timestamp <= weeks(-5)`, false},
	{`timestamp >= weeks(-5)`, true},
	{`timestamp <= months(-5)`, false},
	{`timestamp >= months(-5)`, true},
	{`timestamp <= years(-5)`, false},
	{`timestamp >= years(-5)`, true},
	{`timestamp <= seconds(5)`, true},
	{`timestamp >= seconds(5)`, false},
	{`timestamp <= minutes(5)`, true},
	{`timestamp >= minutes(5)`, false},
	{`timestamp <= hours(5)`, true},
	{`timestamp >= hours(5)`, false},
	{`timestamp <= days(5)`, true},
	{`timestamp >= days(5)`, false},
	{`timestamp <= weeks(5)`, true},
	{`timestamp >= weeks(5)`, false},
	{`timestamp <= months(5)`, true},
	{`timestamp >= months(5)`, false},
	{`timestamp <= years(5)`, true},
	{`timestamp >= years(5)`, false},
}

func TestEvalTimeHelpers(t *testing.T) {
	json := fmt.Sprintf(`{"id":114905,"model":"Camaro","brand":{"name":"Chevrolet"},"timestamp":%d}`, time.Now().Add(time.Duration(-2)*time.Second).UnixNano()/int64(time.Millisecond))
	for _, row := range dataTimeHelpers {
		expr, err := Parse(row.query)
		if err != nil {
			t.Fatal(err.Error())
		}

		_, err = Precompute(expr)
		if err != nil {
			t.Fatal(err.Error())
		}

		truth, _, err := Eval(expr, json)
		if err != nil {
			t.Fatal(err.Error())
		}

		if row.truth {
			assert.True(t, truth, fmt.Sprintf("Query: `%s` JSON: %s", row.query, json))
		} else {
			assert.False(t, truth, fmt.Sprintf("Query: `%s` JSON: %s", row.query, json))
		}
	}
}
