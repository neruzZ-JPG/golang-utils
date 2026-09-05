package tree_task

import (
	"context"
	"errors"
	"math"
	"runtime"
	"testing"
)

var errBenchmarkNeedDivide = errors.New("need divide")

type benchmarkTask struct {
	values    []int
	parallel  bool
	threshold int
	work      int
}

func (t *benchmarkTask) Process(ctx context.Context) (int, bool, error) {
	if cause := context.Cause(ctx); cause != nil {
		return 0, false, cause
	}
	if len(t.values) > t.threshold {
		return 0, true, errBenchmarkNeedDivide
	}

	sum := 0
	for _, value := range t.values {
		sum += value
	}

	benchmarkWork(t.work)
	return sum, false, nil
}

func (t *benchmarkTask) Divide(ctx context.Context) ([]TreeTask[int], error) {
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	if len(t.values) <= 1 {
		return nil, errors.New("cannot divide")
	}

	middle := len(t.values) / 2
	return []TreeTask[int]{
		&benchmarkTask{
			values:    t.values[:middle],
			parallel:  t.parallel,
			threshold: t.threshold,
			work:      t.work,
		},
		&benchmarkTask{
			values:    t.values[middle:],
			parallel:  t.parallel,
			threshold: t.threshold,
			work:      t.work,
		},
	}, nil
}

func (t *benchmarkTask) IsParallel() bool {
	return t.parallel
}

func (t *benchmarkTask) MergeOutputs(_ context.Context, outputs []int) (int, error) {
	sum := 0
	for _, output := range outputs {
		sum += output
	}
	return sum, nil
}

func benchmarkWork(iterations int) {
	value := 1.0
	for i := 0; i < iterations; i++ {
		value = math.Sqrt(value + float64(i))
	}
	runtime.KeepAlive(value)
}

func benchmarkValues() []int {
	values := make([]int, 32)
	for i := range values {
		values[i] = 1
	}
	return values
}

func BenchmarkTreeTaskSequential(b *testing.B) {
	runTreeTaskBenchmark(b, false, 10_000)
}

func BenchmarkTreeTaskParallel(b *testing.B) {
	runTreeTaskBenchmark(b, true, 10_000)
}

func BenchmarkTreeTaskSequentialOverhead(b *testing.B) {
	runTreeTaskBenchmark(b, false, 0)
}

func BenchmarkTreeTaskParallelOverhead(b *testing.B) {
	runTreeTaskBenchmark(b, true, 0)
}

func runTreeTaskBenchmark(b *testing.B, parallel bool, work int) {
	task := &benchmarkTask{
		values:    benchmarkValues(),
		parallel:  parallel,
		threshold: 1,
		work:      work,
	}

	for i := 0; i < b.N; i++ {
		if _, err := RunTreeTask(context.Background(), task); err != nil {
			b.Fatal(err)
		}
	}
}
