package craq

import "errors"

type CraqServer struct {
	endpoints []*CraqServer
	store     map[string]KVBlock
	endserver *CraqServer
}

type KVVersion struct {
	value   []byte
	version int64
	dirty   bool
}

type KVBlock struct {
	Key      string
	versions []KVVersion
}

type KV struct {
	Key   string
	Value []byte
}

func (craq *CraqServer) Get(key string) (KV, error) {
	block, ok := craq.store[key]
	if ok {
		lastVersion := block.versions[len(block.versions)-1]
		if lastVersion.dirty {
			versionNum := craq.endserver.GetNewstVersion(key)
			for i := 0; i < len(block.versions); i++ {
				if block.versions[i].version == versionNum {
					block.versions[i].dirty = false
					return KV{
						Key:   key,
						Value: block.versions[i].value,
					}, nil
				}
			}

		} else {
			return KV{
				Key:   key,
				Value: lastVersion.value,
			}, nil
		}
	}
	return KV{}, errors.New("can't find a kv named [" + key + "]")
}

func (craq *CraqServer) GetNewstVersion(key string) int64 {
	if craq.endserver == craq {
		block, ok := craq.store[key]
		if ok {
			return block.versions[len(block.versions)-1].version
		} else {
			return -1
		}
	} else {
		return -1
	}
}
