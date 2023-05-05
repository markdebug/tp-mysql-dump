package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
	"io"
	"os"
	"os/exec"
	"sync"
	"time"
)

type Config struct {
	DbConfig   []DbConfig   `yaml:"dbConfig"`
	DumpConfig []DumpConfig `yaml:"dumpConfig"`
}

type DbConfig struct {
	ConnectionName string `yaml:"connectionName"`
	Host           string `yaml:"host"`
	Port           string `json:"port"`
	UserName       string `yaml:"userName"`
	Password       string `yaml:"password"`
}

type DumpConfig struct {
	DbName         string `yaml:"dbName"`
	TableName      string `yaml:"tableName"`
	WhereOperation string `yaml:"whereOperation"`
	BackFilePath   string `yaml:"backFilePath"`
	S3BackDir      string `yaml:"s3BackDir"`
	S3Bucket       string `yaml:"s3Bucket"`
	AutoDelete     bool   `yaml:"autoDelete"`
	DeleteLimit    string `yaml:"deleteLimit"`
	DumpUseDb      string `yaml:"dumpUseDb"`
	DeleteUseDb    string `yaml:"deleteUseDb"`
}

func GetConfig() Config {
	f, err := os.ReadFile("config.yml")
	if err != nil {
		log.Fatal("read fail", err)
	}
	t := Config{}
	err = yaml.Unmarshal(f, &t)
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	return t
}
func init() {
	logFilePath := fmt.Sprintf("mysql_back_%s.log", time.Now().Format("20060102"))
	writer3, err := os.OpenFile(logFilePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0755)
	if err != nil {
		log.Fatalf("create file log.txt failed: %v", err)
	}
	log.SetOutput(io.MultiWriter(writer3))
}
func main() {
	var wg sync.WaitGroup
	config := GetConfig()
	BackupMySqlDb(config, &wg)

}

func getDbConfig(connectionName string, dbConfig []DbConfig) DbConfig {
	for _, db := range dbConfig {
		if db.ConnectionName == connectionName {
			return db
		}
	}
	return DbConfig{}
}

// BackupMySqlDb 备份脚本
func BackupMySqlDb(config Config, waitGroup *sync.WaitGroup) {
	log.Info("准备执行脚本", config)
	//在这里如果没有传输表名，那么将会备份整个数据库，否则将只备份自己传入的表
	for _, dump := range config.DumpConfig {
		waitGroup.Add(1)
		go dumpOperation(dump, config, waitGroup)
	}
	waitGroup.Wait()

}

func dumpOperation(dump DumpConfig, config Config, waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()

	if dump.WhereOperation == "" {
		log.Error("dump where 条件为空!!")
		return
	}
	dbConfig := getDbConfig(dump.DumpUseDb, config.DbConfig)
	if dbConfig.Host == "" {
		log.Error("数据库链接错误，请检查,")
		return
	}
	var s string
	fileName := fmt.Sprintf("%s_%s.sql.gz", dump.TableName, time.Now().Format("200601020105"))
	backFilePath := dump.BackFilePath + fileName
	if dump.TableName == "" {
		s = fmt.Sprintf("mysqldump  --lock-tables=false --skip-extended-insert -t -h%s -P%s  -u%s -p%s --databases %s --compress --verbose  | gzip > %s", dbConfig.Host, dbConfig.Port, dbConfig.UserName, dbConfig.Password, dump.DbName, backFilePath)
	} else if dump.WhereOperation == "" {
		s = fmt.Sprintf("mysqldump  --lock-tables=false --skip-extended-insert -t  -h%s -P%s  -u%s -p%s --databases %s --tables %s --compress --verbose | gzip > %s", dbConfig.Host, dbConfig.Port, dbConfig.UserName, dbConfig.Password, dump.DbName, dump.TableName, backFilePath)
	} else {
		s = fmt.Sprintf("mysqldump  --lock-tables=false --skip-extended-insert -t  -h%s -P%s  -u%s -p%s --databases %s --tables %s --compress --verbose --where='%s' | gzip > %s", dbConfig.Host, dbConfig.Port, dbConfig.UserName, dbConfig.Password, dump.DbName, dump.TableName, dump.WhereOperation, backFilePath)
	}
	log.Info("mysql dump脚本生成成功", s)
	out, err := exec.Command("bash", "-c", s).Output()
	if err != nil {
		log.Error("执行mysql dump失败: ", s)
		return
	}
	log.Info("dump脚本执行完毕", string(out))
	//out, err = exec.Command("bash", "-c", "aws s3 ls s3://oyetalk-test/oyetalk-status/").Output()
	//log.Println("查询s3结果", string(out))
	tmpDistPath := dump.S3BackDir + time.Now().Format("200601") + "/" + time.Now().Format("20060102") + "/"
	distPath := "s3://" + dump.S3Bucket + "/" + tmpDistPath
	//开始push 到s3
	c := exec.Command("aws", "s3", "cp", backFilePath, distPath)
	log.Info("执行上传s3的命令是", c.String())
	output, err := c.CombinedOutput()
	if err != nil {
		log.Error("上传s3失败: ", err)
		return
	}
	log.Info("上传s3结束:", string(output))
	//开始查询是否上传成功
	queryResult := checkS3File(dump.S3Bucket, tmpDistPath+fileName)
	log.Info("查询s3结果", queryResult)
	if dump.AutoDelete && queryResult {
		log.Info("准备开始执行清理备份语句")
		deleteBackData(config, dump.DbName, dump.TableName, dump.WhereOperation, dump.DeleteLimit, dump.DeleteUseDb)
	}

}

func deleteBackData(config Config, dbName, tableName, whereSql, deleteLimit, connectionName string) {
	log.Infof("delete BackData  dbName: %s  tableName: %s  whereSql: %s", dbName, tableName, whereSql)
	if whereSql == "" {
		log.Warning("清理脚本sql where 为空，请检查")
		return
	}
	dbConfig := getDbConfig(connectionName, config.DbConfig)
	if dbConfig.Host == "" {
		log.Error("数据库链接错误，请检查,")
		return
	}
	dbUrl := "%s:%s@tcp(%s:%s)/%s?charset=utf8"
	dbConnection := fmt.Sprintf(dbUrl, dbConfig.UserName, dbConfig.Password, dbConfig.Host, dbConfig.Port, dbName)
	database, err := sqlx.Open("mysql", dbConnection)
	if err != nil {
		log.Error("打开MySQL链接错误,", err)
		return
	}
	sql := "delete from %s where %s limit %s"
	executeSql := fmt.Sprintf(sql, tableName, whereSql, deleteLimit)
	log.Info("执行的sql语句为:", executeSql)
	var deleteCount int64 = 0
	for {
		res, err := database.Exec(executeSql)
		if err != nil {
			log.Error("执行sql异常, ", err)
			return
		}
		rowsAffected, _ := res.RowsAffected()
		log.Info("执行清理sql语句返回:", rowsAffected)
		deleteCount += rowsAffected
		if rowsAffected < 1 {
			log.Infof("备份清理完毕，退出 %s %s %d", dbName, tableName, deleteCount)
			break
		}
	}
}

func checkS3File(bucketName, fileName string) bool {
	log.Infof("查询s3 文件大小 %s %s", bucketName, fileName)
	// Load the Shared AWS Configuration (~/.aws/config)
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Error(err)
		return false
	}
	// Create an Amazon S3 service client
	client := s3.NewFromConfig(cfg)
	// Get the first page of results for ListObjectsV2 for a bucket
	output, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(fileName),
	})
	if err != nil {
		log.Error(err)
		return false
	}
	fileSize := output.ContentLength
	log.Infof("文件大小为 fileName:%s fileSize: %d", fileName, fileSize)
	return fileSize > 0
}
