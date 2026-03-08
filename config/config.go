package config

import (
	"fmt"
	"gopkg.in/ini.v1"
)

var Global *Config

type DBConfig struct {
	Host     string `ini:"host"`
	Port     int    `ini:"port"`
	User     string `ini:"user"`
	Password string `ini:"password"`
	Database string `ini:"database"`
}

type Config struct {
	Concurrency int       `ini:"concurrency"`
	HttpPort    int       `ini:"http_port"`
	DBConfig    *DBConfig `ini:"db"`
	SecretKey   string    `ini:"-"`
}

func Init() {
	fileName := `config.ini`

	//加载配置文件
	c, err := ini.Load(fileName)
	if err != nil {
		panic(fmt.Sprintf("加载配置文件 '%s' 失败: %v", fileName, err))
		return
	}

	Global = new(Config)
	err = c.MapTo(Global)
	if err != nil {
		panic(fmt.Sprintf("映射配置信息失败 %v", err))
		return
	}

	if Global.Concurrency == 0 {
		Global.Concurrency = 2
	}

	if Global.HttpPort == 0 {
		Global.HttpPort = 8080
	}

}
