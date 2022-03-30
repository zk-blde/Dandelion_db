package internal

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"sort"
	"sync"

	"fmt"
	"unsafe"
 
	"io"
	"orchid_db/mapEngine"
	"os"
	"strings"
)

var rwlock sync.RWMutex
var wg sync.WaitGroup

var msgChan chan string = make(chan string)

// 定义一个结构体
type OrchidDB struct {
	DirPath DirPath
	kandv []*KValue
	InternalMap mapEngine.BaseEngine
}

type DirPath struct{
	Dirpath string //`json:"dir_path" toml:"dir_path"` // orchiddb dir path of db file
}

type KValue struct {
	key int
	value string
}

/// PathExists 判断文件夹是否存在
//func PathExists(path string) (bool, error) {
//	_, err := os.Stat(path)
//	if err == nil {
//		return true, nil
//	}
//	if os.IsNotExist(err) {
//		return false, nil
//	}
//	return false, err
//}

// init 初始化方法
//todo
// go不把init写成方法
func (db *OrchidDB)initKV(source *OrchidDB)[]*KValue{
	// create the dir path if not exists.
	// 判断文件路径存不存在， 如果不存在则创建 (调用上面的方法)
	//exist, _ := PathExists(source.DirPath.Dirpath)
	_, err := os.Stat(source.DirPath.Dirpath)
	if os.IsNotExist(err){
		//fmt.Printf("no dir![%v]\n", source.DirPath.Dirpath)
		//// 创建文件夹
		//err := os.Mkdir(source.DirPath.Dirpath, os.ModePerm)
		//if err != nil {
		//	fmt.Printf("mkdir failed![%v]\n", err)
		//} else {
		//	fmt.Printf("mkdir success!\n")
		//}
		fmt.Printf("no file![%v]\n", source.DirPath.Dirpath)
		f, err := os.Create("text")
		if err != nil{
			fmt.Println(err)
		}
		fmt.Println(f.Name())
		defer f.Close()
	}
	return db.load_source_file(source)

}


// load_source_file 把文件内容读取出来 ???????????? 这里应该写的有点问题
func (db *OrchidDB) load_source_file(source *OrchidDB) []*KValue {
	file, err := os.Open(source.DirPath.Dirpath)
	if err != nil {
		fmt.Println("open file failed, err:", err)
		//return  // 这里有问题 ！！！！！！
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	for {
		line, err := reader.ReadString('\n') //注意是字符
		if err == io.EOF {
			if len(line) != 0 {
				fmt.Println(line)
			}
			fmt.Println("文件读完了")
			//return // 这里有问题 ！！！！！！
		}
		if err != nil {
			fmt.Println("read file failed, err:", err)
			break
		}
		fmt.Print(line)
		li := strings.Replace(line,"\n", "", -1)
		// 使用空格分割出字符串返回列表, 0索引为键, 1 索引为值
		i := strings.Split(li, " ")
		//method := i[0]
		k := i[1]
		k1 := db.str2bytes(k)
		k2 := db.BytesToInt(k1)
		v := i[2]
		//v1 := db.str2bytes(v)
		//v2 := db.BytesToInt(v1)
		//初始化两个数组
		//db.k = append(db.k, key)
		//db.v = append(db.v, value)
		kv:= &KValue{
			key: k2,
			value: v,
		}
		db.kandv = append(db.kandv,kv)
		// 对切片按照key值排序
		sort.Slice(db.kandv, func(i, j int) bool {
			return db.kandv[j].key > db.kandv[i].key // 升序
			// return testStructs[i].Num > testStructs[j].Num  // 降序
		})
	}
	return db.kandv
}

// update_source 更新本地文件
func (db *OrchidDB)update_source(ctx context.Context){
	file, err := os.OpenFile(db.DirPath.Dirpath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		fmt.Println("open file failed, err:", err)
		return
	}
	// todo
	//db.IntToBytes(key)
	defer file.Close()
LOOP:
	for {
		msg := <- msgChan
		file.WriteString(msg)
		select {
		case <- ctx.Done():
			break LOOP
		default:
		}
	}
}

func (db *OrchidDB)Ctx_object(){
	// 实例化ctx对象
	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	go db.update_source(ctx)
	cancel() // 通知子进程goroutine结束
	wg.Wait()
}

//BinarySearch 二分法查找
//切片s是升序的
//k为待查找的整数
//如果查到有就返回对应角标,
//没有就返回-1
func (db *OrchidDB)BinarySearch(s []*KValue, k int) int {
	lo, hi := 0, len(s)-1
	for lo <= hi {
		m := (lo + hi) >> 1
		if s[m].key < k {
			lo = m + 1
		} else if s[m].key > k {
			hi = m - 1
		} else {
			return m
		}
	}
	return -1
}

//字符串转换字节
func (db *OrchidDB)str2bytes(s string) []byte {
	 x := (*[2]uintptr)(unsafe.Pointer(&s))
	 h := [3]uintptr{x[0], x[1], x[1]}
	 return *(*[]byte)(unsafe.Pointer(&h))
}

// 字节转换字符串
func (db *OrchidDB)bytes2str(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

//整形转换成字节
func (db *OrchidDB)IntToBytes(n int) []byte {
	x := int32(n)
	bytesBuffer := bytes.NewBuffer([]byte{})
	binary.Write(bytesBuffer, binary.BigEndian, x)
	return bytesBuffer.Bytes()
}
//字节转换成整形
func (db *OrchidDB)BytesToInt(b []byte) int {
	bytesBuffer := bytes.NewBuffer(b)

	var x int32
	binary.Read(bytesBuffer, binary.BigEndian, &x)
	return int(x)
}

//sort 排序字符串
//func IsHave(target string, str_array []string) bool {
//	sort.Strings(str_array)
//	index := sort.SearchStrings(str_array, target)
//	//index的取值：0 ~ (len(str_array)-1)
//	return index < len(str_array) && str_array[index] == target
//}


// set 方法
func (db *OrchidDB)Set(key string, v string)string{
	rwlock.Lock()
	defer rwlock.Unlock()
	db.kandv = db.initKV(db)

	k1 := db.str2bytes(key)
	k2 :=db.BytesToInt(k1)
	n := db.BinarySearch(db.kandv, k2)
	// n == -1 说明没有这个值
	if n == -1 {
		kv:= &KValue{
			key: k2,
			value: v,
		}
		db.kandv = append(db.kandv,kv)
		//把字符串拼接 加进 通道
		msg := "set" + " " + key + " " + v + "\n"
		msgChan <- msg
		// 这里更新文件
		//db.update_source("set", key, v)
		return "key is not find, Automatic Set"
	}else{
	// 在的情况
		db.kandv[n] = &KValue{
			key: k2,
			value: v,
		}
		//把字符串拼接 加进 通道
		msg := "set" + " " + key + " " + v + "\n"
		msgChan <- msg
		// 这里更新文件
		//db.update_source("set", key, v)
		return "key is find, Automatic update"
	}
}

// get 方法
func (db *OrchidDB)Get(key string)(value string){
	rwlock.RLock()  // 加锁
	defer rwlock.RUnlock() // 解读锁
	db.kandv = db.initKV(db)
	k1 := db.str2bytes(key)
	k2 :=db.BytesToInt(k1)
	// 二分法返回对应的下标
	n := db.BinarySearch(db.kandv, k2)
	if n != -1{
		return db.kandv[n].value
	}else{
		return "value is not find"
	}
}

// update 方法
func (db *OrchidDB)Put(key string, v string)bool{
	/*
	更新key的value, 并更新source文件
	return bool 成功返回true, 失败返回false
	*/
	rwlock.Lock()
	defer rwlock.Unlock()
	db.kandv = db.initKV(db)
	k1 := db.str2bytes(key)
	k2 :=db.BytesToInt(k1)
	n := db.BinarySearch(db.kandv, k2)

	if n != -1 {
		// 在的情况
		db.kandv[n] = &KValue{
			key: k2,
			value: v,
		}
		//把字符串拼接 加进 通道
		msg := "put" + " " + key + " " + v + "\n"
		msgChan <- msg
		// todo 更新文件
		return true
	}else{
		//rwlock.RUnlock()
		return false
	}
}

// delete 删除的操作
func (db *OrchidDB)Delete(key string)bool{
	rwlock.Lock()
	defer rwlock.Unlock()
	db.kandv = db.initKV(db)
	k1 := db.str2bytes(key)
	k2 :=db.BytesToInt(k1)
	n := db.BinarySearch(db.kandv, k2)
	if n != -1{
		//把字符串拼接 加进 通道
		msg := "set" + " " + key + " " + db.kandv[n].value + "\n"
		msgChan <- msg
		// 先更新文件
		//db.update_source("delete", key, db.kandv[n].value)
		db.kandv = append(db.kandv[:n], db.kandv[n+1:]...)
		//rwlock.RUnlock()
		return true
	}else{
		//rwlock.RUnlock()
		return false
	}
}


func (db *OrchidDB)valid_cammand(cammand string)(string,bool){
	cmd := strings.Split(cammand, " ")
	if cmd[0] == "del"{
		if len(cmd) != 2 {
			return "cosole: del key", false
		}else{
			return "ok", true
		}
	}
	if cmd[0] == "get" || cmd[0] == "set" || cmd[0] == "update"{
		if len(cmd) != 3{
			// 这里的格式化有问题
			return "console: method key value", false
		}else{
			return "ok", true
		}
	}else{
		return "console: method [key] [value]", false
	}
}

func (db *OrchidDB)console(method string, key string, value string ){
	var cammand string
	fmt.Printf("请输入")
	fmt.Scanf("%d\n", &cammand)
	//fmt.Println("用户输入的是: ", method)
	str, ok := db.valid_cammand(cammand)
	if ok {
		method = strings.Split(cammand, " ")[0]
		key = strings.Split(cammand, " ")[1]
		value = strings.Split(cammand, " ")[2]
		if method == "set"{
			db.Set(key,value)
		}else if method =="get" {
			db.Get(key)
		}else if method == "update" {
			db.Put(key, value)
		}else{
			db.Delete(key)
		}
	}else{
		print(str)
	}
}

//func main(){
//	//初始化的init 方法
//
//
//}