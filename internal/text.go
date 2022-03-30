package internal

import (
	"encoding/hex"
	"fmt"
)

func main() {
	strData := "3156EF"

	// 将HEX编码的字符串转换为HEX数据
	data1, _ := hex.DecodeString(strData)
	for n,v:=range data1{
		fmt.Printf("strData[%d]值十进制为：%v , 16进制为：%#X \n",n,v,v)
	}

	// 将HEX数据转换为HEX编码的字符串
	fmt.Printf("strsdata = %v  \n",hex.EncodeToString(data1))

}
