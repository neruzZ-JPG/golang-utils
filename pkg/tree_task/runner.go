package tree_task

import (
	"context"
	"sync"
)

func RunTreeTask[Out any](ctx context.Context, task TreeTask[Out]) (Out, error) {
	var zero Out
	if cause := context.Cause(ctx); cause != nil {
		return zero, cause
	}

	out, shouldContinue, err := task.Process(ctx)
	if err == nil {
		return out, nil
	}
	if !shouldContinue {
		return zero, err
	}
	if cause := context.Cause(ctx); cause != nil {
		var zero Out
		return zero, cause
	}

	subTasks, err := task.Divide(ctx)
	if err != nil {
		if cause := context.Cause(ctx); cause != nil {
			var zero Out
			return zero, cause
		}

		var zero Out
		return zero, err
	}

	var subOutputs []Out
	if task.IsParallel() {
		subOutputs, err = runParallel(ctx, subTasks)
	} else {
		subOutputs, err = runSequential(ctx, subTasks)
	}
	if err != nil {
		var zero Out
		return zero, err
	}

	return task.MergeOutputs(ctx, subOutputs)
}

func runSequential[Out any](
	ctx context.Context,
	tasks []TreeTask[Out],
) ([]Out, error) {
	outputs := make([]Out, 0, len(tasks))

	for _, child := range tasks {
		out, err := RunTreeTask(ctx, child)
		if err != nil {
			return nil, err
		}
		outputs = append(outputs, out)
	}

	return outputs, nil
}

func runParallel[Out any](
	ctx context.Context,
	tasks []TreeTask[Out],
) ([]Out, error) {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	outputsByIndex := make([]Out, len(tasks))
	var wg sync.WaitGroup
	var failOnce sync.Once

	for i, child := range tasks {
		wg.Add(1)

		go func(index int, task TreeTask[Out]) {
			defer wg.Done()

			out, err := RunTreeTask(ctx, task)
			outputsByIndex[index] = out

			if err != nil {
				failOnce.Do(func() {
					cancel(err)
				})
			}
		}(i, child)
	}

	wg.Wait()

	if cause := context.Cause(ctx); cause != nil {
		var zero []Out
		return zero, cause
	}

	return outputsByIndex, nil
}
