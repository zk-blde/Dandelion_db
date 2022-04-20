package cache

import (
	"encoding/binary"
)

const EntryHeaderSize = 10

const (
	PUT uint16 = iota
	DEL
)
// Entry 写入文件的记录
type Entry struct {
	Key       []byte
	Value     []byte
	KeySize   uint32
	ValueSize uint32
	Mark      uint16
}

func (e *Entry) GetSize() int64 {
	return int64(EntryHeaderSize + e.KeySize + e.ValueSize)
}


// 解码 buf 字节数组，返回 Entry
func Decode(buf []byte) (*Entry, error) {
	ks := binary.BigEndian.Uint32(buf[0:4])
	vs := binary.BigEndian.Uint32(buf[4:8])
	mark := binary.BigEndian.Uint16(buf[8:10])
	return &Entry{KeySize: ks, ValueSize: vs, Mark: mark}, nil
}

