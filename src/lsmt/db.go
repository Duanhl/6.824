package lsmt

type Kv struct {
	Key     string
	Value   string
	Version uint64
}

type LsmtDB struct {
}

func (db *LsmtDB) Get(key string) (string, error) {
	return "", nil
}

func (db *LsmtDB) Put(key string, value string) error {
	return nil
}

func (db *LsmtDB) Range(start string, end string) ([]Kv, error) {
	return make([]Kv, 0), nil
}
