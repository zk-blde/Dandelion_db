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

	// 加载索引 mapEngine
	db.loadMapEngineFormFile()
	return db, nil
}

// todo merge
func (db *DandelionDB) Merge() error {
	if db.dbFile.Offset == 0 {
		return nil
	}

	var (
		validEntries []*cache.Entry
		offset       int64
	)

	// 读取元数据文件中的Entry
	for {
		e, err := db.dbFile.Read(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		// 内存中的索引状态是最新的，直接对比过滤出有效的 Entry
		if off, ok := db.mapEngine[string(e.Key)]; ok && off == offset {
			validEntries = append(validEntries, e)
		}
		offset += e.GetSize()
	}

	if len(validEntries) > 0 {
		// 新建临时文件
		mergeDBFile, err := file.NewMergeDBFile(db.dirPath)
		if err != nil {
			return err
		}
		defer os.Remove(mergeDBFile.File.Name())

		// 重新写入有效的 entry
		for _, entry := range validEntries {
			writeOff := mergeDBFile.Offset
			err := mergeDBFile.Write(entry)
			if err != nil {
				return err
			}

			// 更新索引
			db.mapEngine[string(entry.Key)] = writeOff
		}

		// 获取文件名
		dbFileName := db.dbFile.File.Name()
		// 关闭文件
		db.dbFile.File.Close()
		// 删除旧的数据文件
		os.Remove(dbFileName)

		// 获取文件名
		mergeDBFileName := mergeDBFile.File.Name()
		// 关闭文件
		mergeDBFile.File.Close()
		// 临时文件变更为新的数据文件
		os.Rename(mergeDBFileName, db.dirPath+string(os.PathSeparator)+file.FileName)

		db.dbFile = mergeDBFile
	}
	return nil
}



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

// Put 写入数据
func (db *DandelionDB) Put(key []byte, value []byte) (err error) {
	if len(key) == 0 {
		return
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	offset := db.dbFile.Offset
	// 封装成 Entry
	entry := cache.NewEntry(key, value, cache.PUT)
	// 追加到数据文件当中
	err = db.dbFile.Write(entry)

	// 写到内存
	db.mapEngine[string(key)] = offset
	return
}

// Get 取出数据
func (db *DandelionDB) Get(key []byte) (val []byte, err error) {
	if len(key) == 0 {
		return
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	// 从内存当中取出索引信息
	offset, ok := db.mapEngine[string(key)]
	// key 不存在
	if !ok {
		return
	}

	// 从磁盘中读取数据
	var e *cache.Entry
	e, err = db.dbFile.Read(offset)
	if err != nil && err != io.EOF {
		return
	}
	if e != nil {
		val = e.Value
	}
	return
}

// Del 删除数据
func (db *DandelionDB) Del(key []byte) (err error) {
	if len(key) == 0 {
		return
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	// 从内存当中取出索引信息
	_, ok := db.mapEngine[string(key)]
	// key 不存在，忽略
	if !ok {
		return
	}

	// 封装成 Entry 并写入
	e := cache.NewEntry(key, nil, cache.DEL)
	err = db.dbFile.Write(e)
	if err != nil {
		return
	}

	// 删除内存中的 key
	delete(db.mapEngine, string(key))
	return
}