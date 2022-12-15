package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ADDRESS 保存所有storage server的地址
var ADDRESS = [6]string{
	"localhost:10000",
	"localhost:10001",
	"localhost:10002",
	"localhost:10003",
	"localhost:10004",
	"localhost:10005",
}

// Statistics 统计信息
type Statistics struct {
	m     map[string]bool // 统计每个chunk是否已经算过
	count int             // 统计数量
	mu    sync.Mutex      // 保护共享变量的锁
}

// 全局变量
// 共享变量
var statistics = Statistics{
	m:     make(map[string]bool),
	count: 0,
}

var ConnectPool []net.Conn // 连接句柄池
var AddressPool []string   // 连接地址池

// 建立tcp连接
func connect(address string) net.Conn {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil
	}
	return conn
}

// 统计dblp
// 参数：连接tcp句柄，统计方式，统计key，统计地址
func countDBLP(conn net.Conn, msg, address string, wg *sync.WaitGroup) {
	defer wg.Done()
	data := make([]byte, 2048) // 1024太小了
	// 发送信息
	_, err := conn.Write([]byte(msg))
	if err != nil {
		fmt.Println("无法向", address, "发送请求")
		return
	}
	n, err := conn.Read(data)
	if err != nil {
		fmt.Println("无法从", address, "收到结果")
		return
	}
	m := make(map[string]int)
	json.Unmarshal(data[:n], &m)

	// debug
	// xx := strings.Split(address, ":")
	// f, _ := os.OpenFile(xx[1]+".txt", os.O_RDWR|os.O_CREATE, 0755)
	// for k, _ := range m {
	// 	f.Write([]byte(k + "\n"))
	// }

	// 保护共享变量
	statistics.mu.Lock()
	defer statistics.mu.Unlock()
	for k, v := range m {
		if _, ok := statistics.m[k]; !ok { // 该chunk尚未统计
			statistics.m[k] = true
			statistics.count += v
		}
	}
}

func askAuthor() string {
try:
	fmt.Print("请输入作者（必须全称，并且大小写正确）：")
	// 读取一行
	// fmt.Scanln()
	author, _ := bufio.NewReader(os.Stdin).ReadString('\n')
	author = author[:len(author)-1] // 过滤回车、换行
	if len(author) == 0 {
		fmt.Println("输入内容为空，请重新输入")
		goto try
	}
	fmt.Println("输入的内容为", author)
	fmt.Println([]byte(author))
	fmt.Println(len(author))
	// fmt.Print("请输入时间，格式为[start,end]，开闭区间皆可：")
	return author
}

func askTime() (start, end string) {
try:
	fmt.Print("请输入时间（以闭区间的形式输入，例如[2000,2005]，若不需要时间，则输入[-1,-1]）：")
	t, _ := bufio.NewReader(os.Stdin).ReadString('\n')
	t = t[:len(t)-1]
	ts := strings.Split(t, ",")
	if len(ts) != 2 {
		fmt.Println("输入非法，请重新输入")
		goto try
	}
	if len(ts[0]) == 1 || len(ts[1]) == 1 {
		fmt.Println("输入非法，请重新输入")
		goto try
	}
	start = ts[0][1:]
	end = ts[1][:len(ts[1])-1]
	_, err1 := strconv.Atoi(start)
	_, err2 := strconv.Atoi(end)
	if err1 != nil || err2 != nil {
		fmt.Println("输入非法，请重新输入")
		goto try
	}
	return
}

func PrintResult(cost float64) {
	statistics.mu.Lock()
	defer statistics.mu.Unlock()
	fmt.Printf("共找到%d篇符合条件的文章，耗时%f秒\n", statistics.count, cost)
	if len(statistics.m) < 117 {
		fmt.Println("注意，真实情况可能大于该结果！")
	}
}

// 重置statistics
func cleanStatistics() {
	statistics.mu.Lock()
	defer statistics.mu.Unlock()
	statistics.count = 0
	statistics.m = make(map[string]bool) // 重新申请一个map
}

func killServer(i int) {
	ConnectPool[i].Close()
	ConnectPool = append(ConnectPool[:i], ConnectPool[i+1:]...)
	AddressPool = append(AddressPool[:i], AddressPool[i+1:]...)
	fmt.Printf("剩余server数量为%d，分别为：\n", len(AddressPool))
	for _, address := range AddressPool {
		fmt.Println(address)
	}
}

// 初始化
func init() {
	fmt.Println("正在初始化...")

	for _, address := range ADDRESS {
		conn := connect(address)
		if conn != nil {
			ConnectPool = append(ConnectPool, conn)
			AddressPool = append(AddressPool, address)
		}
	}

	if len(AddressPool) == 0 {
		fmt.Println("没有找到dblp存储服务器，程序结束")
		os.Exit(-1)
	}
	fmt.Println("连接成功的storage server有：")
	for _, address := range AddressPool {
		fmt.Println(address)
	}

	rand.Seed(time.Now().Unix())
}

func main() {
choices:
	fmt.Println("[1]查询dblp")
	fmt.Println("[2]查询log")
	fmt.Println("[3]还没做")
	fmt.Println("[4]与随机server断开连接")
	fmt.Println("[5]退出")
	fmt.Print("输入选择：")

	var choice int
	fmt.Scanln(&choice)
	switch choice {
	case 1:
		author := askAuthor()
		start, end := askTime()
		if start == "-1" && end == "-1" { // 不需要输入时间
			start = ""
			end = ""
		}
		fmt.Println(author, start, end)
		// 形成需要发送的内容
		// 消息格式 flag;name;start;end
		msg := "0;" + author + ";" + start + ";" + end
		// 进行查找
		fmt.Println("开始查询")
		startTime := time.Now()
		var wg sync.WaitGroup
		for i, conn := range ConnectPool {
			wg.Add(1)
			go countDBLP(conn, msg, AddressPool[i], &wg)
		}
		wg.Wait()

		// 查询结束
		PrintResult(time.Since(startTime).Seconds())

		cleanStatistics()
		goto choices
	case 4:
		if len(AddressPool) <= 3 {
			fmt.Println("server数量过少，不能杀死")
			goto choices
		}
		killServer(rand.Intn(len(AddressPool)))
		goto choices
	case 5:
		// 关闭tcp连接
		for _, conn := range ConnectPool {
			conn.Close()
		}
		return
	default:
		fmt.Println("请输入正确的选择")
		goto choices
	}
	//
	// if *a != "" && *t == "" { // 根据作者查找
	// 	author := *a
	// 	var wg sync.WaitGroup
	// 	for i, conn := range connectPool {
	// 		wg.Add(1)
	// 		go countDBLP(conn, "0", author, addressPool[i], &wg)
	// 	}
	// 	wg.Wait()
	//
	// 	// 关闭tcp连接
	// 	for _, conn := range connectPool {
	// 		conn.Close()
	// 	}
	//
	// 	statistics.mu.Lock()
	//
	// 	// debug
	// 	// f, _ := os.OpenFile("tmp.txt", os.O_RDWR|os.O_CREATE, 0755)
	// 	// for k, _ := range statistics.m {
	// 	// 	f.Write([]byte(k + "\n"))
	// 	// }
	// 	// f.Close()
	//
	// 	// 输出结果
	// 	fmt.Printf("共找到%d篇与%v有关的文章\n", statistics.count, author)
	// 	// 共117个chunk
	// 	fmt.Println(len(statistics.m))
	// 	if len(statistics.m) < 117 {
	// 		fmt.Println("注意，真实情况可能大于该结果！")
	// 	}
	// 	statistics.mu.Unlock()
	// } else if *t != "" { // 根据时间查找
	// 	// 格式[start,end]，例如[2001,2010]
	//
	// }
	// if *a != "" && *t != "" {
	//
	// }
}
