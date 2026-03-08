package util

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func InTimeWindow(targetTime time.Time, timeWindow string) (bool, error) {
	parts := strings.Split(timeWindow, "-")
	if len(parts) != 2 {
		return false, fmt.Errorf("invalid time window format: %s. Expected HH:MM-HH:MM", timeWindow)
	}

	startStr := strings.TrimSpace(parts[0])
	endStr := strings.TrimSpace(parts[1])

	const layout = "15:04"
	startTime, err := time.Parse(layout, startStr)
	if err != nil {
		return false, fmt.Errorf("invalid start time format: %s", startStr)
	}

	endTime, err := time.Parse(layout, endStr)
	if err != nil {
		return false, fmt.Errorf("invalid end time format: %s", endStr)
	}

	if startTime.Equal(endTime) {
		return false, fmt.Errorf("invalid time window: %s", timeWindow)
	}

	hour := targetTime.Hour()
	minute := targetTime.Minute()

	//去掉日期
	t := time.Date(startTime.Year(), startTime.Month(), startTime.Day(), hour, minute, 0, 0, startTime.Location())

	// 普通窗口 (startTime <= endTime)
	if startTime.Before(endTime) {
		return (t.After(startTime) || t.Equal(startTime)) && t.Before(endTime), nil
	} else {
		// 跨天：窗口为 start->24:00 和 00:00->end
		return (t.After(startTime) || t.Equal(startTime)) || t.Before(endTime), nil
	}

}

func EnterWorkDir() {
	fullpath, err := os.Executable()
	if err != nil {
		panic(err)
	}
	dir, _ := filepath.Split(fullpath)
	err = os.Chdir(dir)
	if err != nil {
		panic(err)
	}
	currentDir, _ := os.Getwd()
	fmt.Printf("当前目录为: %s\n", currentDir)
}

func RunShellCommand(command string) (string, error) {
	var cmd *exec.Cmd
	cmd = exec.Command("sh", "-c", command)
	output, err := cmd.CombinedOutput()
	outputStr := string(output)
	if err != nil {
		return outputStr, err
	}
	return outputStr, nil
}

func GetManyOfChan[T any](ctx context.Context, ch <-chan T, size int) (rows []T, ok bool) {

	if size == 0 {
		panic("size==0")
	}

	rows = make([]T, 0, size)

	for i := 0; i < size; i++ {
		select {
		case <-ctx.Done():
			return rows, false
		case row, open := <-ch:
			if !open {
				return rows, false
			}
			rows = append(rows, row)
		}
	}

	return rows, true
}

func InSlice[T comparable](target T, list []T) bool {
	for i, _ := range list {
		if target == list[i] {
			return true
		}
	}
	return false
}
func PathExists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	return false
}

func TimeCost() func(str string) {
	//计算耗时
	bts := time.Now().Unix()
	return func(str string) {
		ts := time.Now().Unix() - bts
		slog.Info("%s", str, "耗时", ts)
	}
}

func ParseDSN(dsn string) (host string, port int, db string, err error) {
	// 先分割出地址和数据库
	parts := strings.SplitN(dsn, "/", 2)

	var addr string
	if len(parts) == 1 {
		addr = parts[0]
		db = ""
	} else if len(parts) == 2 {
		addr, db = parts[0], parts[1]
	} else {
		return "", 0, "", fmt.Errorf("dsn 格式错误: %s", dsn)
	}

	// 继续分割地址和端口
	hostPort := strings.SplitN(addr, ":", 2)
	if len(hostPort) != 2 {
		return "", 0, "", fmt.Errorf("地址端口格式错误: %s", addr)
	}

	port, err = strconv.Atoi(hostPort[1])
	if err != nil {
		return "", 0, "", fmt.Errorf("端口格式错误: %s", hostPort[1])
	}

	return hostPort[0], port, db, nil
}

func WriteFileTail(filename string, text string) {
	//写入文件尾部（追加）
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	f.WriteString(text)
}
