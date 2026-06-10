package dag

import "context"

// FuncNode is a convenience Node built from a function. It lets callers (and
// tests) construct a DAG without declaring a type per node.
type FuncNode struct {
	NodeID  string
	Deps    []string
	Fn      func(ctx context.Context, in Inputs) (Output, error)
	OnExpand func(ctx context.Context, out Output) ([]Node, error)
}

// ID implements Node.
func (f *FuncNode) ID() string { return f.NodeID }

// DependsOn implements Node.
func (f *FuncNode) DependsOn() []string { return f.Deps }

// Execute implements Node.
func (f *FuncNode) Execute(ctx context.Context, in Inputs) (Output, error) {
	return f.Fn(ctx, in)
}

// Expand implements Expandable when OnExpand is set.
func (f *FuncNode) Expand(ctx context.Context, out Output) ([]Node, error) {
	if f.OnExpand == nil {
		return nil, nil
	}
	return f.OnExpand(ctx, out)
}
