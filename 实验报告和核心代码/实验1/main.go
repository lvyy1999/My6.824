package main

import (
	"container/heap"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
)

var number = 1

// printNumber : 打印number当前值并使其加1，超过9则重新置为1
func printNumber() {
	fmt.Printf("%d", number)
	if number++; number > 9 {
		number = 1
	}
}

var letter = 'A'

// printLetter : 打印letter当前值并使其加1，超过Z则重新置为A
func printLetter() {
	fmt.Printf("%c", letter)
	if letter++; letter > 'Z' {
		letter = 'A'
	}
}

// MinHeap ：最小堆，实现container/heap中要求的接口
type MinHeap struct {
	arr []int
}

func (h MinHeap) Len() int {
	return len(h.arr)
}

func (h MinHeap) Less(i, j int) bool {
	return h.arr[i] < h.arr[j]
}

func (h MinHeap) Swap(i, j int) {
	h.arr[i], h.arr[j] = h.arr[j], h.arr[i]
}

func (h *MinHeap) Push(x interface{}) {
	h.arr = append(h.arr, x.(int))
}

func (h *MinHeap) Pop() interface{} {
	n := len(h.arr)
	x := h.arr[n-1]
	h.arr = h.arr[0 : n-1]
	return x
}

// MinHeapInit : 对数组模拟最小堆初始化
func MinHeapInit(arr []int) {
	n := len(arr)
	for i := n / 2; i >= 0; i-- {
		MinHeapAdjust(arr, i, n)
	}
}

// MinHeapAdjust : 对数组模拟最小堆调整过程
func MinHeapAdjust(arr []int, i, n int) {
	for j := 2*i + 1; j < n; i, j = j, 2*j+1 {
		if j+1 < n && arr[j] > arr[j+1] {
			j = j + 1
		}
		if arr[i] > arr[j] {
			arr[i], arr[j] = arr[j], arr[i]
		} else {
			break
		}
	}
}

// MinHeapSort : 对数组模拟最小堆排序过程
func MinHeapSort(arr []int) {
	// 建堆
	MinHeapInit(arr)
	// 排序
	for n := len(arr); n > 1; n-- {
		// 将最小值放到数组尾部，再对剩余元素进行堆调整
		arr[0], arr[n-1] = arr[n-1], arr[0]
		MinHeapAdjust(arr, 0, n-1)
	}
	// 翻转即为从小到大排序结果
	for i, j := 0, len(arr)-1; i < j; i, j = i+1, j-1 {
		arr[i], arr[j] = arr[j], arr[i]
	}
}

// 分布式lab1
func main() {
	fmt.Println("---------- lab1 start ----------")
	fmt.Println("a)配置Golang环境；")
	fmt.Println("已完成，go版本：1.20.4，开发环境：Goland2025，运行环境：windows/centos虚拟机")

	fmt.Println("------------------------------")
	fmt.Println("b)尝试多种方式实现两个goroutine交替打印数字与字母，例如：12AB34CD…")
	turns := 20 //打印轮数
	var wg sync.WaitGroup

	// 方法1，使用锁和条件变量
	wg.Add(2)
	mutex := &sync.Mutex{}
	cond := sync.NewCond(mutex)
	printNum := true //当前是打印数字还是字母
	fmt.Println("方法1：使用互斥锁与条件变量")
	// 打印数字的线程
	go func() {
		defer wg.Done()
		for i := 0; i < turns; i++ {
			cond.L.Lock()
			for !printNum {
				cond.Wait()
			}
			printNumber()
			printNumber()
			printNum = false
			cond.Signal()
			cond.L.Unlock()
		}
	}()
	//打印字母的线程
	go func() {
		defer wg.Done()
		for i := 0; i < turns; i++ {
			cond.L.Lock()
			for printNum {
				cond.Wait()
			}
			printLetter()
			printLetter()
			printNum = true
			cond.Signal()
			cond.L.Unlock()
		}
	}()
	// 阻塞等待线程执行完毕
	wg.Wait()
	fmt.Println("")

	// 方法2，使用管道机制
	number = 1
	letter = 'A'
	wg.Add(2)
	chNumber := make(chan bool)
	chLetter := make(chan bool)
	fmt.Println("方法2：使用管道机制")
	// 打印数字的线程
	go func() {
		defer wg.Done()
		for i := 0; i < turns; i++ {
			<-chNumber
			printNumber()
			printNumber()
			chLetter <- true
		}
	}()
	//打印字母的线程
	go func() {
		defer wg.Done()
		for i := 0; i < turns; i++ {
			<-chLetter
			printLetter()
			printLetter()
			if i != turns-1 {
				chNumber <- true
			}
		}
	}()
	// 开始打印
	chNumber <- true
	// 阻塞等待线程执行完毕
	wg.Wait()
	fmt.Println("")

	// 方法3，使用原子操作
	number = 1
	letter = 'A'
	wg.Add(2)
	var order int32 = 1 //1:打印数字，2:打印字母
	fmt.Println("方法3：使用原子操作")
	// 打印数字的线程
	go func() {
		defer wg.Done()
		for i := 0; i < turns; i++ {
			for atomic.LoadInt32(&order) != 1 {
				// order不为1时自旋等待
			}
			printNumber()
			printNumber()
			atomic.StoreInt32(&order, 2)
		}
	}()
	//打印字母的线程
	go func() {
		defer wg.Done()
		for i := 0; i < turns; i++ {
			for atomic.LoadInt32(&order) != 2 {
				// order不为2时自旋等待
			}
			printLetter()
			printLetter()
			atomic.StoreInt32(&order, 1)
		}
	}()
	// 阻塞等待线程执行完毕
	wg.Wait()
	fmt.Println("")

	fmt.Println("------------------------------")
	fmt.Println("c)实现整型堆排序（方法一：借助container/heap包；方法二：数组模拟）；")
	fmt.Println("待排序数组（随机生成20个100以内数字）:")
	arr := make([]int, 20)
	for i := 0; i < 20; i++ {
		arr[i] = rand.Intn(100)
	}
	fmt.Println(arr)
	// 方法1：借助container/heap包
	fmt.Println("方法一：借助container/heap包")
	h := &MinHeap{}
	heap.Init(h)
	for i := 0; i < 20; i++ {
		heap.Push(h, arr[i])
	}
	for i := 0; i < 20; i++ {
		x := heap.Pop(h)
		fmt.Printf("%d ", x)
	}
	fmt.Println("")
	// 方法2：数组模拟
	fmt.Println("方法二：数组模拟")
	MinHeapSort(arr)
	for i := 0; i < 20; i++ {
		fmt.Printf("%d ", arr[i])
	}
	fmt.Println("")

	fmt.Println("---------- lab1 end ----------")
}
