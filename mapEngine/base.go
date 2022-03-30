package mapEngine

type BaseEngine interface {
	Get(key string) (string, error)
	Set(key string, value string) error
	Put(key string, value string) error
	Delete(key string) error
}
