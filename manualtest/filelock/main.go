package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/SwanSpouse/goleveldb/leveldb/storage"
)

var (
	filename string
	child    bool
)

func init() {
	// os.TempDir 获取一个临时文件夹
	flag.StringVar(&filename, "filename", filepath.Join(os.TempDir(), "goleveldb_filelock_test"), "Filename used for testing")
	flag.BoolVar(&child, "child", false, "This is the child")
}

/*
命令行flag的语法有如下三种形式：
	-flag // 只支持bool类型
	-flag=x
	-flag x // 只支持非bool类型

	以上语法对于一个或两个‘－’号，效果是一样的，但是要注意对于第三种情况，只能用于非 bool 类型的 flag。
	原因是：如果支持，那么对于这样的命令 cmd -x *，如果有一个文件名字是：0或false等，则命令的原意会改变（bool 类型可以和其他类型一样处理，其次bool类型支持 -flag 这种形式，因为Parse()中，对bool类型进行了特殊处理）。
	默认的，提供了 -flag，则对应的值为 true，否则为 flag.Bool/BoolVar 中指定的默认值；如果希望显示设置为 false 则使用 -flag=false。
*/
// 启动一个另一个进程
func runChild() error {
	var args []string
	args = append(args, os.Args[1:]...)
	// BoolVar这个东西的默认值是true
	args = append(args, "-child") // TODO limingji 在这里就会把child的这个值赋值成true？？？ 这个厉害了。

	// 在启动一个当前程序，参数都相同；只不过多一个-child
	cmd := exec.Command(os.Args[0], args...)
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	r := bufio.NewReader(&out)
	for {
		line, _, e1 := r.ReadLine()
		if e1 != nil {
			break
		}
		fmt.Println("[Child]", string(line))
	}
	return err
}

func main() {
	flag.Parse()
	fmt.Printf("Using path: %s child:%+v\n", filename, child)
	if child {
		fmt.Println("Child flag set.")
	}
	// 第一次启动这个进程的时候看，应该可以执行成功；因为没有人使用这个文件锁
	stor, err := storage.OpenFile(filename, false)
	if err != nil {
		fmt.Printf("Could not open storage: %s", err)
		os.Exit(10)
	}

	if !child {
		fmt.Println("Executing child -- first test (expecting error)")
		err := runChild()
		if err == nil {
			fmt.Println("Expecting error from child")
		} else if err.Error() != "exit status 10" {
			fmt.Println("Got unexpected error from child:", err)
		} else {
			fmt.Printf("Got error from child: %s (expected)\n", err)
		}
	}
	// 然后把这个storage关闭
	err = stor.Close()
	if err != nil {
		fmt.Printf("Error when closing storage: %s", err)
		os.Exit(11)
	}

	// 再次启动一个进程的时候，就可以启动成功了；因为文件锁在上面刚刚已经被释放了。
	if !child {
		fmt.Println("Executing child -- second test")
		err := runChild()
		if err != nil {
			fmt.Println("Got unexpected error from child:", err)
		}
	}
	// 删除临时文件
	os.RemoveAll(filename)
}
