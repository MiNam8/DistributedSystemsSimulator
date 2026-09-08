// This file is the mechanical enforcement of the purity contract described in
// doc.go. It is deliberately the first test in the project, written before there
// was any logic to violate it.
//
// Purity is not a style preference here. Every guarantee the project makes rests
// on one property: a given seed replays a simulated run bit for bit. A single
// time.Now, a single unseeded rand, or a single goroutine inside the core
// destroys that property *silently* — the tests keep passing, the simulator keeps
// running, and the reproducibility contract quietly becomes a lie. So it is
// checked by the build rather than remembered by the author.
//
// The test lives in package raft_test so that its own imports (os/exec, go/ast)
// are not themselves dependencies of the core.
package raft_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// allowedDirectImports is the complete set of packages the core may import
// directly. An allowlist is used rather than a denylist because a denylist only
// blocks the impure packages someone thought of in advance, whereas this blocks
// everything not argued for.
//
// Every entry here is free of I/O, time, concurrency and global state. Note the
// absence of fmt: it would pull in os, which pulls in time, which would make the
// transitive guarantee below unprovable. That is why the core uses errors.New
// sentinels and hand-written String methods.
var allowedDirectImports = map[string]bool{
	"errors":    true,
	"sort":      true,
	"slices":    true,
	"cmp":       true,
	"strconv":   true,
	"math/bits": true,
}

// forbiddenTransitiveDeps must not appear anywhere in the core's transitive
// import graph. Exact paths and prefixes are both checked.
//
// This list is stronger than it looks. It is not "the core avoids calling
// time.Now" — it is "the clock is not linked into the core at all", which is a
// claim the linker can be asked to confirm.
var (
	forbiddenExact = []string{
		"time", "net", "os", "sync", "syscall", "context",
		"io", "fmt", "reflect", "log", "bufio", "bytes",
	}
	forbiddenPrefix = []string{
		"net/", "os/", "sync/", "math/rand", "encoding/",
		"database/", "log/", "io/", "runtime/pprof", "testing",
	}
)

// TestCoreHasNoImpureTransitiveDeps asserts that the core's entire linked
// dependency graph is free of I/O, time and concurrency primitives.
func TestCoreHasNoImpureTransitiveDeps(t *testing.T) {
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go toolchain not on PATH; cannot inspect the import graph")
	}

	out, err := exec.Command("go", "list", "-deps", ".").Output()
	if err != nil {
		t.Fatalf("go list -deps: %v", err)
	}

	for _, dep := range strings.Fields(string(out)) {
		// stdlib internals (internal/abi, internal/goarch, ...) are unavoidable
		// implementation detail of even a zero-import package.
		if strings.HasPrefix(dep, "internal/") {
			continue
		}
		for _, bad := range forbiddenExact {
			if dep == bad {
				t.Errorf("core transitively depends on %q — the purity contract is broken.\n"+
					"Run: go list -deps . | grep %s   to find the path that pulls it in.", dep, dep)
			}
		}
		for _, bad := range forbiddenPrefix {
			if strings.HasPrefix(dep, bad) {
				t.Errorf("core transitively depends on %q (forbidden prefix %q) — the purity contract is broken.", dep, bad)
			}
		}
	}
}

// TestCoreHasNoImpureDirectImports asserts the core's direct imports against the
// allowlist. This catches an impure import at the exact file that introduced it,
// which the transitive test cannot do.
func TestCoreHasNoImpureDirectImports(t *testing.T) {
	for file, f := range parseCore(t) {
		for _, imp := range f.Imports {
			path := strings.Trim(imp.Path.Value, `"`)
			if !allowedDirectImports[path] {
				t.Errorf("%s imports %q, which is not on the core allowlist.\n"+
					"If this package is genuinely pure — no I/O, no clock, no goroutines, no global state —\n"+
					"add it to allowedDirectImports with a note on why. Otherwise it belongs in the runtime,\n"+
					"not the core: return an Effect and let the caller do it.", file, path)
			}
		}
	}
}

// TestCoreHasNoConcurrency asserts the core contains no goroutines, channels or
// select statements.
//
// Banning sync at the import level is not enough: a goroutine plus a channel
// needs no imports at all, and would introduce nondeterministic interleaving
// that the simulator cannot reproduce. All concurrency in this system lives in
// the node runtime, which owns the core from a single goroutine.
func TestCoreHasNoConcurrency(t *testing.T) {
	for file, f := range parseCore(t) {
		ast.Inspect(f, func(n ast.Node) bool {
			switch n.(type) {
			case *ast.GoStmt:
				t.Errorf("%s contains a go statement. The core must be single-threaded; "+
					"concurrency belongs in the node runtime.", file)
			case *ast.ChanType:
				t.Errorf("%s declares a channel. The core communicates by returning Effects, "+
					"never by blocking on a channel.", file)
			case *ast.SelectStmt:
				t.Errorf("%s contains a select statement. Nondeterministic choice cannot be "+
					"replayed from a seed.", file)
			}
			return true
		})
	}
}

// TestCoreHasNoMutableGlobals asserts that package-level variables are limited
// to sentinel errors.
//
// Mutable global state survives across simulated runs within one process, so a
// seed that reproduces a failure in isolation may not reproduce it in a soak
// run — the single most confusing way for determinism to fail. All state
// belongs on the Raft struct.
func TestCoreHasNoMutableGlobals(t *testing.T) {
	for file, f := range parseCore(t) {
		for _, decl := range f.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.VAR {
				continue
			}
			for _, spec := range gen.Specs {
				vs := spec.(*ast.ValueSpec)
				for i, name := range vs.Names {
					if i < len(vs.Values) && isErrorsNewCall(vs.Values[i]) {
						continue // sentinel error: immutable in practice
					}
					t.Errorf("%s declares package-level var %q. The core must hold all state on "+
						"the Raft struct — a mutable global breaks seed reproducibility across runs "+
						"in the same process. Only errors.New sentinels are permitted.", file, name.Name)
				}
			}
		}
	}
}

// isErrorsNewCall reports whether e is a call to errors.New.
func isErrorsNewCall(e ast.Expr) bool {
	call, ok := e.(*ast.CallExpr)
	if !ok {
		return false
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	pkg, ok := sel.X.(*ast.Ident)
	return ok && pkg.Name == "errors" && sel.Sel.Name == "New"
}

// parseCore parses every non-test .go file in the core package.
func parseCore(t *testing.T) map[string]*ast.File {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("reading core package directory: %v", err)
	}
	fset := token.NewFileSet()
	files := make(map[string]*ast.File)
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(".", name), nil, 0)
		if err != nil {
			t.Fatalf("parsing %s: %v", name, err)
		}
		files[name] = f
	}
	if len(files) == 0 {
		t.Fatal("no core source files found — the purity test is not actually checking anything")
	}
	return files
}
