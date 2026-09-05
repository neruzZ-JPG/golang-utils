# golang-utils

> 一组轻量、可独立引用的 Go 工具包。

`golang-utils` 提供两个可以直接复用的包：

- `stringutil`：字符串处理工具。
- `tree_task`：通用树节点任务执行器，支持递归拆分、串并行调度、结果合并和错误取消。

如果你的程序里需要递归拆分任务、并发执行子任务，或者实现类似 Map-Reduce 的计算流程，可以直接使用 `tree_task`。

## 安装

引入 `tree_task`：

```powershell
go get github.com/neruzZ-JPG/golang-utils/pkg/tree_task@v0.1.1
```

引入 `stringutil`：

```powershell
go get github.com/neruzZ-JPG/golang-utils/pkg/stringutil@v0.1.1
```

如需始终跟随最新稳定版本，可以把版本号替换为 `@latest`。

代码中导入：

```go
import (
    "github.com/neruzZ-JPG/golang-utils/pkg/stringutil"
    "github.com/neruzZ-JPG/golang-utils/pkg/tree_task"
)
```

## 快速开始

`stringutil` 的使用很简单：

```go
reversed := stringutil.Reverse("golang")
// 输出：gnalog
```

`tree_task` 的核心入口是：

```go
result, err := tree_task.RunTreeTask(context.Background(), task)
```

你只需要实现一个 `TreeTask`，执行器会负责递归、调度、并行和合并。

## tree_task 是什么

一个任务可能太大，需要拆成几个小任务；拆开之后，某些分支可以同时执行；最后再把零散结果合并起来。

`tree_task` 把这些通用逻辑集中到执行器中，任务实现只需要回答四个问题：

- 什么时候能直接处理；
- 不能处理时如何拆分；
- 子任务是否应该并行；
- 子任务结果如何合并。

### 接口

```go
type TreeTask[Out any] interface {
    Divide(context.Context) ([]TreeTask[Out], error)
    Process(context.Context) (Out, bool, error)
    IsParallel() bool
    MergeOutputs(context.Context, []Out) (Out, error)
}
```

### 执行过程

1. 先调用 `Process`。
2. `Process` 成功时，直接返回结果。
3. `Process` 返回错误且 `shouldContinue == true` 时，调用 `Divide`。
4. `IsParallel` 决定子任务顺序执行还是并发执行。
5. 所有子任务完成后，由 `MergeOutputs` 汇总。
6. 并行分支中出现第一个错误时，其他兄弟分支会收到取消信号。

`Process` 的第二个返回值只在出错时起作用：

| 情况 | 返回值 |
| --- | --- |
| 成功 | `out, false, nil` |
| 需要继续拆分 | `zero, true, err` |
| 真正失败 | `zero, false, err` |

### 使用示例

下面是一个整数求和任务。数组较长时拆成两半，最后把所有小结果加回来。

```go
package main

import (
    "context"
    "errors"
    "fmt"

    "github.com/neruzZ-JPG/golang-utils/pkg/tree_task"
)

var errNeedDivide = errors.New("need divide")

type AddTask struct {
    nums     []int
    parallel bool
}

func (t *AddTask) Process(ctx context.Context) (int, bool, error) {
    if cause := context.Cause(ctx); cause != nil {
        return 0, false, cause
    }
    if len(t.nums) > 2 {
        return 0, true, errNeedDivide
    }

    sum := 0
    for _, num := range t.nums {
        sum += num
    }
    return sum, false, nil
}

func (t *AddTask) Divide(ctx context.Context) ([]tree_task.TreeTask[int], error) {
    if len(t.nums) < 2 {
        return nil, errors.New("cannot divide")
    }

    middle := len(t.nums) / 2
    return []tree_task.TreeTask[int]{
        &AddTask{
            nums:     t.nums[:middle],
            parallel: t.parallel,
        },
        &AddTask{
            nums:     t.nums[middle:],
            parallel: t.parallel,
        },
    }, nil
}

func (t *AddTask) IsParallel() bool {
    return t.parallel
}

func (t *AddTask) MergeOutputs(_ context.Context, outputs []int) (int, error) {
    sum := 0
    for _, output := range outputs {
        sum += output
    }
    return sum, nil
}

func main() {
    task := &AddTask{
        nums:     []int{1, 2, 3, 4, 5, 6, 7},
        parallel: true,
    }

    result, err := tree_task.RunTreeTask(context.Background(), task)
    fmt.Println(result, err)
}
```

## 特性

- 输出类型由泛型约束，不需要手动处理 `any` 和类型断言。
- 并行结果按下标收集，即使子任务完成顺序不同，汇总顺序仍然稳定。
- 使用 `context.WithCancelCause` 传播首个错误，并取消其他并行分支。
- 任务方法接收 `context.Context`，便于实现超时和主动取消。
- 提供完整测试、竞态检测和基准测试。

## 验证与测试

```powershell
# 运行全部测试
go test ./...

# 运行竞态检测
go test -race ./...

# 查看任务拆分与合并日志
go test ./pkg/tree_task -v -count=1

# 只运行 AddTask 示例测试
go test ./pkg/tree_task -run '^TestAddTask$' -v -count=1
```

## 版本引用

当前稳定版本为 `v0.1.1`。推荐明确指定版本，避免上游更新导致构建变化：

```powershell
go get github.com/neruzZ-JPG/golang-utils/pkg/tree_task@v0.1.1
```

此时使用者的 `go.mod` 会包含类似依赖：

```text
require github.com/neruzZ-JPG/golang-utils v0.1.1
```

需要体验尚未发布的功能时，可以引用默认分支：

```powershell
go get github.com/neruzZ-JPG/golang-utils/pkg/tree_task@main
```
