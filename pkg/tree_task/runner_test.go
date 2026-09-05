package tree_task

import (
	"context"
	"errors"
	"log"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type letterTask struct {
	input    []string
	parallel bool
}

func (t *letterTask) Process(ctx context.Context) (string, error) {
	if cause := context.Cause(ctx); cause != nil {
		return "", cause
	}
	if len(t.input) == 0 {
		return "", errors.New("empty input")
	}
	if len(t.input) == 1 {
		return strings.ToUpper(t.input[0]), nil
	}

	return "", errors.New("needs division")
}

func (t *letterTask) Divide(ctx context.Context) ([]TreeTask[string], error) {
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	if len(t.input) < 2 {
		return nil, errors.New("cannot divide")
	}

	middle := len(t.input) / 2
	return []TreeTask[string]{
		&letterTask{
			input:    t.input[:middle],
			parallel: t.parallel,
		},
		&letterTask{
			input:    t.input[middle:],
			parallel: t.parallel,
		},
	}, nil
}

func (t *letterTask) IsParallel() bool {
	return t.parallel
}

func (t *letterTask) MergeOutputs(_ context.Context, outputs []string) (string, error) {
	return strings.Join(outputs, ""), nil
}

func TestRunTreeTask(t *testing.T) {
	tests := []struct {
		name     string
		parallel bool
	}{
		{name: "sequential"},
		{name: "parallel", parallel: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task := &letterTask{
				input:    []string{"a", "b", "c", "d", "e"},
				parallel: tt.parallel,
			}

			got, err := RunTreeTask(context.Background(), task)
			if err != nil {
				t.Fatalf("RunTreeTask() error = %v", err)
			}

			want := "ABCDE"
			if got != want {
				t.Fatalf("RunTreeTask() = %v, want %v", got, want)
			}
		})
	}
}

var errBranchFailed = errors.New("branch failed")

type failingTask struct {
	started <-chan struct{}
}

func (t *failingTask) Process(context.Context) (string, error) {
	<-t.started
	return "", errBranchFailed
}

func (t *failingTask) Divide(context.Context) ([]TreeTask[string], error) {
	return nil, errBranchFailed
}

func (*failingTask) IsParallel() bool {
	return false
}

func (*failingTask) MergeOutputs(context.Context, []string) (string, error) {
	return "", nil
}

type cancelObserverTask struct {
	started  chan struct{}
	observed atomic.Bool
}

func (t *cancelObserverTask) Process(ctx context.Context) (string, error) {
	close(t.started)
	<-ctx.Done()
	if errors.Is(context.Cause(ctx), errBranchFailed) {
		t.observed.Store(true)
	}

	return "", context.Cause(ctx)
}

func (t *cancelObserverTask) Divide(ctx context.Context) ([]TreeTask[string], error) {
	return nil, context.Cause(ctx)
}

func (t *cancelObserverTask) IsParallel() bool {
	return false
}

func (t *cancelObserverTask) MergeOutputs(context.Context, []string) (string, error) {
	return "", nil
}

type parallelFailureTask struct {
	observer *cancelObserverTask
	started  chan struct{}
}

func (t *parallelFailureTask) Process(context.Context) (string, error) {
	return "", errors.New("needs division")
}

func (t *parallelFailureTask) Divide(ctx context.Context) ([]TreeTask[string], error) {
	return []TreeTask[string]{
		&failingTask{started: t.started},
		t.observer,
	}, nil
}

func (t *parallelFailureTask) IsParallel() bool {
	return true
}

func (t *parallelFailureTask) MergeOutputs(context.Context, []string) (string, error) {
	return "", nil
}

func TestRunTreeTaskParallelCancelsSiblings(t *testing.T) {
	started := make(chan struct{})
	observer := &cancelObserverTask{started: started}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := RunTreeTask(ctx, &parallelFailureTask{
		observer: observer,
		started:  started,
	})
	if !errors.Is(err, errBranchFailed) {
		t.Fatalf("RunTreeTask() error = %v, want %v", err, errBranchFailed)
	}
	if !observer.observed.Load() {
		t.Fatal("parallel sibling was not canceled after the first error")
	}
}

type AddTask struct {
	numList []int
}

func (task *AddTask) Process(ctx context.Context) (int, error) {
	if cause := context.Cause(ctx); cause != nil {
		return 0, cause
	}
	if len(task.numList) > 5 {
		return 0, errors.New("too long to add")
	}

	res := 0
	for _, num := range task.numList {
		res += num
	}

	log.Printf("[AddTask] process input=%v result=%d", task.numList, res)
	return res, nil
}

func (task *AddTask) Divide(ctx context.Context) ([]TreeTask[int], error) {
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	if len(task.numList) <= 1 {
		return nil, errors.New("too short to divide")
	}

	middle := len(task.numList) / 2
	left := task.numList[:middle]
	right := task.numList[middle:]
	log.Printf("[AddTask] divide input=%v left=%v right=%v", task.numList, left, right)

	return []TreeTask[int]{
		&AddTask{
			numList: left,
		},
		&AddTask{
			numList: right,
		},
	}, nil
}

func (task *AddTask) IsParallel() bool {
	return true
}

func (task *AddTask) MergeOutputs(ctx context.Context, nums []int) (int, error) {
	if cause := context.Cause(ctx); cause != nil {
		return 0, cause
	}

	res := 0
	for _, num := range nums {
		res += num
	}

	log.Printf("[AddTask] merge inputs=%v result=%d", nums, res)

	return res, nil
}

func TestAddTask(t *testing.T) {
	nums := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}

	got, err := RunTreeTask(context.Background(), &AddTask{
		numList: nums,
	})
	if err != nil {
		t.Fatalf("RunTreeTask() error = %v", err)
	}

	want := 66
	if got != want {
		t.Fatalf("RunTreeTask() = %d, want %d", got, want)
	}
}
