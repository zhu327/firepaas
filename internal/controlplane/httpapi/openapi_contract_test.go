// openapi_contract_test.go：docs/openapi.yaml 与 httpapi 路由表的路径级契约。
//
// 文档头部声明“路径与方法的唯一权威是 api.go 的 Register 路由表”，本测试把
// 该声明变成可执行门禁：新增/删除端点却忘记同步文档时失败。字段级形状仍以
// handler 为准（文件头已注明），不做自动校验。
package httpapi

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// TestOpenAPIPathCoverage 校验 openapi 的 paths 与 api.go 路由表一一对应。
func TestOpenAPIPathCoverage(t *testing.T) {
	code := routeTableFromSource(t, "api.go")
	doc := openAPIRoutesFromDoc(t, "../../../docs/openapi.yaml")

	var missingInDoc, missingInCode []string
	for r := range code {
		if !doc[r] {
			missingInDoc = append(missingInDoc, r)
		}
	}
	for r := range doc {
		if !code[r] {
			missingInCode = append(missingInCode, r)
		}
	}
	sort.Strings(missingInDoc)
	sort.Strings(missingInCode)
	if len(missingInDoc) > 0 || len(missingInCode) > 0 {
		t.Fatalf("openapi 路由漂移：\n  代码有而文档缺: %v\n  文档有而代码缺: %v",
			missingInDoc, missingInCode)
	}
}

// routeTableFromSource 解析 api.go AST，收集 mux.HandleFunc("METHOD /path", …)。
func routeTableFromSource(t *testing.T, path string) map[string]bool {
	t.Helper()
	src, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, src, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	routes := map[string]bool{}
	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || len(call.Args) != 2 {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "HandleFunc" {
			return true
		}
		lit, ok := call.Args[0].(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			return true
		}
		pattern, err := strconv.Unquote(lit.Value)
		if err != nil {
			return true
		}
		method, path, ok := strings.Cut(pattern, " ")
		if !ok || !strings.HasPrefix(path, "/") {
			return true
		}
		routes[strings.ToUpper(method)+" "+path] = true
		return true
	})
	if len(routes) == 0 {
		t.Fatalf("no HandleFunc routes found in %s", path)
	}
	return routes
}

var (
	openAPIPathLine   = regexp.MustCompile(`^  (/[^:]*):\s*$`)
	openAPIMethodLine = regexp.MustCompile(`^    (get|post|put|delete|patch):\s*$`)
)

// openAPIRoutesFromDoc 扫描 YAML 的 paths: 段（路径键为两空格缩进的 / 开头行，
// 方法键为四空格缩进），避免为测试引入 YAML 依赖。
func openAPIRoutesFromDoc(t *testing.T, path string) map[string]bool {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	routes := map[string]bool{}
	var current string
	inPaths := false
	for _, line := range strings.Split(string(raw), "\n") {
		if !inPaths {
			if line == "paths:" {
				inPaths = true
			}
			continue
		}
		if line != "" && !strings.HasPrefix(line, " ") {
			break // 离开 paths: 段
		}
		if m := openAPIPathLine.FindStringSubmatch(line); m != nil {
			current = m[1]
			continue
		}
		if m := openAPIMethodLine.FindStringSubmatch(line); m != nil {
			if current == "" {
				t.Fatalf("method line %q without a preceding path in %s", line, path)
			}
			routes[strings.ToUpper(m[1])+" "+current] = true
		}
	}
	if len(routes) == 0 {
		t.Fatalf("no routes parsed from %s", path)
	}
	return routes
}
