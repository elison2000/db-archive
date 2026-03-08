package util

import (
	"regexp"
	"strconv"
	"time"
)

func ParseDateMacros(condition string) string {
	now := time.Now()
	// 正则说明：
	// \s* 匹配可选空格
	// ([+-])? 捕获可选的正号或负号
	// (\d+)? 捕获可选的数字偏移量
	re := regexp.MustCompile(`\{\{\s*today\s*(?:([+-])\s*(\d+))?\s*\}\}`)

	return re.ReplaceAllStringFunc(condition, func(match string) string {
		subMatches := re.FindStringSubmatch(match)

		days := 0
		// 如果存在运算符和数字部分
		if len(subMatches) > 2 && subMatches[2] != "" {
			operator := subMatches[1]
			val, _ := strconv.Atoi(subMatches[2])

			if operator == "+" {
				days = val
			} else {
				days = -val
			}
		}

		// 返回计算后的日期字符串 (YYYY-MM-DD)
		return now.AddDate(0, 0, days).Format("2006-01-02")
	})
}
