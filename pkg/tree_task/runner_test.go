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

func (t *letterTask) Process(ctx context.Context) (string, bool, error) {
	if cause := context.Cause(ctx); cause != nil {
		log.Printf("[letterTask] process canceled cause=%v", cause)
		return "", false, cause
	}
	if len(t.input) == 0 {
		log.Printf("[letterTask] process input=%v error=%v", t.input, "empty input")
		return "", false, errors.New("empty input")
	}
	if len(t.input) == 1 {
		result := strings.ToUpper(t.input[0])
		log.Printf("[letterTask] process input=%v result=%q", t.input, result)
		return result, false, nil
	}

	log.Printf("[letterTask] process input=%v shouldContinue=true", t.input)
	return "", true, errors.New("needs division")
}

func (t *letterTask) Divide(ctx context.Context) ([]TreeTask[string], error) {
	if cause := context.Cause(ctx); cause != nil {
		log.Printf("[letterTask] divide canceled cause=%v", cause)
		return nil, cause
	}
	if len(t.input) < 2 {
		log.Printf("[letterTask] divide input=%v error=%v", t.input, "cannot divide")
		return nil, errors.New("cannot divide")
	}

	middle := len(t.input) / 2
	left := t.input[:middle]
	right := t.input[middle:]
	log.Printf("[letterTask] divide input=%v left=%v right=%v", t.input, left, right)

	return []TreeTask[string]{
		&letterTask{
			input:    left,
			parallel: t.parallel,
		},
		&letterTask{
			input:    right,
			parallel: t.parallel,
		},
	}, nil
}

func (t *letterTask) IsParallel() bool {
	return t.parallel
}

func (t *letterTask) MergeOutputs(_ context.Context, outputs []string) (string, error) {
	result := strings.Join(outputs, "")
	log.Printf("[letterTask] merge inputs=%v result=%q", outputs, result)
	return result, nil
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
			log.Printf("[TestRunTreeTask] %s start input=%v", tt.name, task.input)

			got, err := RunTreeTask(context.Background(), task)
			log.Printf("[TestRunTreeTask] %s result=%q err=%v", tt.name, got, err)
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

func (t *failingTask) Process(context.Context) (string, bool, error) {
	<-t.started
	log.Printf("[failingTask] process error=%v", errBranchFailed)
	return "", false, errBranchFailed
}

func (t *failingTask) Divide(context.Context) ([]TreeTask[string], error) {
	log.Printf("[failingTask] divide error=%v", errBranchFailed)
	return nil, errBranchFailed
}

func (*failingTask) IsParallel() bool {
	return false
}

func (*failingTask) MergeOutputs(context.Context, []string) (string, error) {
	log.Printf("[failingTask] merge not reached")
	return "", nil
}

type cancelObserverTask struct {
	started  chan struct{}
	observed atomic.Bool
}

func (t *cancelObserverTask) Process(ctx context.Context) (string, bool, error) {
	close(t.started)
	log.Printf("[cancelObserverTask] process waiting for cancel")
	<-ctx.Done()
	cause := context.Cause(ctx)
	if errors.Is(cause, errBranchFailed) {
		t.observed.Store(true)
	}

	log.Printf("[cancelObserverTask] process canceled observed=%v cause=%v", t.observed.Load(), cause)
	return "", false, cause
}

func (t *cancelObserverTask) Divide(ctx context.Context) ([]TreeTask[string], error) {
	cause := context.Cause(ctx)
	log.Printf("[cancelObserverTask] divide cause=%v", cause)
	return nil, cause
}

func (t *cancelObserverTask) IsParallel() bool {
	return false
}

func (t *cancelObserverTask) MergeOutputs(context.Context, []string) (string, error) {
	log.Printf("[cancelObserverTask] merge not reached")
	return "", nil
}

type parallelFailureTask struct {
	observer *cancelObserverTask
	started  chan struct{}
}

func (t *parallelFailureTask) Process(context.Context) (string, bool, error) {
	log.Printf("[parallelFailureTask] process shouldContinue=true")
	return "", true, errors.New("needs division")
}

func (t *parallelFailureTask) Divide(ctx context.Context) ([]TreeTask[string], error) {
	log.Printf("[parallelFailureTask] divide children=%d", 2)
	return []TreeTask[string]{
		&failingTask{started: t.started},
		t.observer,
	}, nil
}

func (t *parallelFailureTask) IsParallel() bool {
	return true
}

func (t *parallelFailureTask) MergeOutputs(context.Context, []string) (string, error) {
	log.Printf("[parallelFailureTask] merge not reached")
	return "", nil
}

func TestRunTreeTaskParallelCancelsSiblings(t *testing.T) {
	started := make(chan struct{})
	observer := &cancelObserverTask{started: started}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	log.Printf("[TestRunTreeTaskParallelCancelsSiblings] start")

	_, err := RunTreeTask(ctx, &parallelFailureTask{
		observer: observer,
		started:  started,
	})
	log.Printf("[TestRunTreeTaskParallelCancelsSiblings] result err=%v observed=%v", err, observer.observed.Load())
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

func (task *AddTask) Process(ctx context.Context) (int, bool, error) {
	if cause := context.Cause(ctx); cause != nil {
		log.Printf("[AddTask] process canceled cause=%v", cause)
		return 0, false, cause
	}
	if len(task.numList) > 5 {
		log.Printf("[AddTask] process input=%v shouldContinue=true", task.numList)
		return 0, true, errors.New("too long to add")
	}

	res := 0
	for _, num := range task.numList {
		res += num
	}

	log.Printf("[AddTask] process input=%v result=%d shouldContinue=false", task.numList, res)
	return res, false, nil
}

func (task *AddTask) Divide(ctx context.Context) ([]TreeTask[int], error) {
	if cause := context.Cause(ctx); cause != nil {
		log.Printf("[AddTask] divide canceled cause=%v", cause)
		return nil, cause
	}
	if len(task.numList) <= 1 {
		log.Printf("[AddTask] divide input=%v error=%v", task.numList, "too short to divide")
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
		log.Printf("[AddTask] merge canceled cause=%v", cause)
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
	log.Printf("[TestAddTask] start input=%v", nums)

	got, err := RunTreeTask(context.Background(), &AddTask{
		numList: nums,
	})
	log.Printf("[TestAddTask] result=%d err=%v", got, err)
	if err != nil {
		t.Fatalf("RunTreeTask() error = %v", err)
	}

	want := 66
	if got != want {
		t.Fatalf("RunTreeTask() = %d, want %d", got, want)
	}
}
