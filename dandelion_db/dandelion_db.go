package dandelion_db

import (
	"Dandelion_db/cache"
	"Dandelion_db/file"
	"io"
	"os"
	"sync"
)

type DandelionDB struct {
	mapEngine map[string]int64  // 索引信息
	dbFile *file.DBFile      // 数据文件
	dirPath string           // 数据目录
	mu      sync.RWMutex
}


func OpenDB(dirPath string) (*DandelionDB, error) {
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	dbFile, err := file.NewDBFile(dirPath)
	if err != nil{
		return nil, err
	}

	db := &DandelionDB{
		dbFile:  dbFile,
		mapEngine: make(map[string]int64),
		dirPath: dirPath,
	}

	// 加载索引
	db.loadMapEngineFormFile()
	return db, nil
}

// todo merge




func (db *DandelionDB) loadMapEngineFormFile(){
	if db.dbFile == nil {
		return
	}
	var offset int64
	for {
		e, err := db.dbFile.Read(offset)
		if err != nil {
			// 读取完毕
			if err == io.EOF {
				break
			}
			return
		}

		// 设置索引状态
		db.mapEngine[string(e.Key)] = offset

		if e.Mark == cache.DEL {
			// 删除内存中的 key
			delete(db.mapEngine, string(e.Key))
		}

		offset += e.GetSize()
	}
	return
}