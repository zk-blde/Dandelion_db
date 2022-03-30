package mapEngine

import "errors"

type InnerDBOperation struct {
	Db map[string]string
}



func (i *InnerDBOperation)Set(key string,value string) error {
	_,ok:=i.Db[key]
	if ok{
		return errors.New("key existed")
	}else{
		i.Db[key]=value
	}
	return nil
}

func (i *InnerDBOperation)Get(key string) (string, error)  {
	value,ok:=i.Db[key]
	if ok{
		return value,nil
	}else {
		return "",errors.New("key not existed")
	}
}

func (i *InnerDBOperation)Put(key string, value string) error  {
	_,ok:=i.Db[key]
	if ok{
		i.Db[key]=value

	}else {
		return errors.New("key not existed")
	}
	return nil
}

func (i *InnerDBOperation)Delete(key string) error  {
	delete(i.Db,key)
	_,ok:=i.Db[key]
	if !ok{
		return nil
	}else {
		return errors.New("delete failed")
	}
}