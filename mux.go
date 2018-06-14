package mux

import (
	"errors"
	"fmt"
	"time"

	"github.com/GiterLab/groupcache/consistenthash"
	"github.com/tobyzxj/uuid"
)

// HandlerCallBack 处理回调函数
type HandlerCallBack func(bucketName string, msg []byte)

// GetdistributeKeyCallBack 获取分发键值
type GetdistributeKeyCallBack func(msg []byte) string

// HandlerTimeoutCallBack 数据处理超时回调函数,一般用于数据流监控, 监控数据是否产生出现了异常
type HandlerTimeoutCallBack func()

// New 创建一个实例
func New(replicas int, fn consistenthash.Hash) *consistenthash.Map {
	return consistenthash.New(replicas, fn)
}

// Consumer 消费者信息
type Consumer struct {
	bucketName string
	msgChannel chan []byte
}

// Process 数据处理，并发处理
func Process(procNum int, channelLen int, timeoutSec int, m *consistenthash.Map, messaageHandler HandlerCallBack, message <-chan []byte, distributeKeyHandler GetdistributeKeyCallBack, timeoutHandler HandlerTimeoutCallBack) error {
	var consumerList []Consumer
	var consumerListMap map[string]int // bucketName 对应的index序号，优化查询速度

	if timeoutHandler == nil {
		timeoutHandler = func() {
			fmt.Println("[mux] handle message timeout")
		}
	}

	if procNum == 0 || channelLen == 0 || timeoutSec == 0 || m == nil || messaageHandler == nil || distributeKeyHandler == nil {
		return errors.New(" m *consistenthash.Map is nil")
	}
	consumerList = make([]Consumer, procNum)
	consumerListMap = make(map[string]int, procNum)
	for index := 0; index < procNum; index++ {
		consumerList[index].bucketName = uuid.New()
		consumerList[index].msgChannel = make(chan []byte, channelLen)
		consumerListMap[consumerList[index].bucketName] = index
		m.Add(consumerList[index].bucketName)

		// 启动对应的消费者
		go func(bucketName string, msg <-chan []byte) {
			var (
				running bool
				reading = msg
				body    []byte
			)

			// 循环数据处理，永不退出
			for {
				body, running = <-reading
				if !running {
					fmt.Println("[mux] channel1 is close:", bucketName)
				}
				messaageHandler(bucketName, body)
			}
		}(consumerList[index].bucketName, consumerList[index].msgChannel)
	}

	// 启动主生产者，发送给不同的消费者(桶)
	go func() {
		var (
			running bool
			reading = message
			body    []byte
		)

		// 循环数据处理，永不退出
		for {
			select {
			case <-time.After(time.Duration(timeoutSec) * time.Second):
				timeoutHandler()
			case body, running = <-reading:
				// all messages consumed
				if !running {
					fmt.Println("[mux] channel2 is close")
				}
				// 按分发键值分发给不同的处理函数
				key := distributeKeyHandler(body)
				bucketName := m.Get(key)
				index := consumerListMap[bucketName]
				consumerList[index].msgChannel <- body
			}
		}
	}()

	return nil
}
