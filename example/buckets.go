package main

import (
	"bytes"
	"fmt"
	"time"

	"github.com/GiterLab/mux"
)

var timeStart time.Time

// MessageHandler 消息处理
func MessageHandler(bucketName string, msg []byte) {
	time.Sleep(1 * time.Second) // 模拟数据处理所需要的时间
	fmt.Println(bucketName, string(msg))
}

// GetdistributeKey 获取分发键值
func GetdistributeKey(msg []byte) (key string, err error) {
	return string(msg),nil
}

// TimeoutHandler 时间超时处理函数
func TimeoutHandler() {
	fmt.Println(time.Now().Sub(timeStart))
}

func main() {
	var msg chan []byte
	msg = make(chan []byte, 10)

	timeStart = time.Now()
	// 1. 创建mux实例
	m := mux.New(1000, nil)
	procArr := make([]string,3)
	procArr[0]="a"
	procArr[1]="b"
	procArr[2]="c"
	// 3. 定义好消息的处理函数、分发键值处理函数和消息超时处理函数
	mux.Process(procArr, 1000, 5, m, MessageHandler, msg, GetdistributeKey, TimeoutHandler)

	// 模拟一些数据
	for index := 0; index < 3; index++ {
		b := bytes.Buffer{}
		b.WriteString(fmt.Sprintf("%d", index))
		msg <- b.Bytes()
		fmt.Println("Publish data", string(b.Bytes()))
	}
	fmt.Println("wait here...")
	select {}
}
