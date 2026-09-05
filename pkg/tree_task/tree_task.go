package tree_task

import "context"

type TreeTask[Out any] interface {
	Divide(context.Context) ([]TreeTask[Out], error)
	Process(context.Context) (Out, bool, error)
	IsParallel() bool
	MergeOutputs(context.Context, []Out) (Out, error)
}
