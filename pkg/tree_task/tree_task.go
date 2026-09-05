package tree_task

import "context"

type TreeTask[In, Out any] interface {
	Divide(context.Context) ([]TreeTask[In, Out], error)
	Process(context.Context) (Out, error)
	IsParallel() bool
	MergeOutputs(context.Context, []Out) (Out, error)
}
