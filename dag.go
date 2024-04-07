package merkledag

import (
	"crypto/sha256"
	"encoding/json"
	"hash"
)

const (
	LIST_LIMIT  = 2048
	BLOCK_LIMIT = 256 * 1024
)

const (
	BLOB = "blob"
	LIST = "list"
	TREE = "tree"
)

type Link struct {
	Name string
	Hash []byte
	Size int
}

type Object struct {
	Links []Link
	Data  []byte
}

func Add(store KVStore, node Node, h hash.Hash) []byte {
	// 将节点数据写入到 KVStore 中
	if node.Type() == FILE {
		file := node.(File)
		tmp := StoreFile(store, file, h)
		jsonMarshal, _ := json.Marshal(tmp)
		hash := calculateHash(jsonMarshal)
		return hash
	} else {
		dir := node.(Dir)
		tmp := StoreDir(store, dir, h)
		jsonMarshal, _ := json.Marshal(tmp)
		hash := calculateHash(jsonMarshal)
		return hash
	}

}

// 实现哈希计算的逻辑
func calculateHash(data []byte) []byte {
	h := sha256.New()
	h.Write(data)
	return h.Sum(nil)
}

func StoreFile(store KVStore, file File, h hash.Hash) *Object {
	if len(file.Bytes()) <= 256*1024 {
		data := file.Bytes()
		blob := Object{Data: data, Links: nil}
		jsonMarshal, _ := json.Marshal(blob)
		hash := calculateHash(jsonMarshal)
		store.Put(hash, data)
		return &blob
	}
	linkLen := (len(file.Bytes()) + (256*1024 - 1)) / (256 * 1024)
	hight := 0 //merkle dag 的高度
	tmp := linkLen
	for {
		hight++
		tmp /= 4096
		if tmp == 0 {
			break
		}
	}
	res, _ := dfsForStoreFile(hight, file, store, 0, h)
	return res
}

func dfsForStoreFile(hight int, file File, store KVStore, seedId int, h hash.Hash) (*Object, int) {
	if hight == 1 {
		if (len(file.Bytes()) - seedId) <= 256*1024 {
			data := file.Bytes()[seedId:]
			blob := Object{Data: data, Links: nil}
			jsonMarshal, _ := json.Marshal(blob)
			hash := calculateHash(jsonMarshal)
			store.Put(hash, data)
			return &blob, len(data)
		}
		links := &Object{}
		lenData := 0
		for i := 1; i <= 4096; i++ {
			end := seedId + 256*1024
			if len(file.Bytes()) < end {
				end = len(file.Bytes())
			}
			data := file.Bytes()[seedId:end]
			blob := Object{Data: data, Links: nil}
			lenData += len(data)
			jsonMarshal, _ := json.Marshal(blob)
			hash := calculateHash(jsonMarshal)
			store.Put(hash, data)
			links.Links = append(links.Links, Link{
				Hash: hash,
				Size: len(data),
			})
			links.Data = append(links.Data, []byte("blob")...)
			seedId += 256 * 1024
			if seedId >= len(file.Bytes()) {
				break
			}
		}
		jsonMarshal, _ := json.Marshal(links)
		hash := calculateHash(jsonMarshal)
		store.Put(hash, jsonMarshal)
		return links, lenData
	} else {
		links := &Object{}
		lenData := 0
		for i := 1; i <= 4096; i++ {
			if seedId >= len(file.Bytes()) {
				break
			}
			tmp, lens := dfsForStoreFile(hight-1, file, store, seedId, h)
			lenData += lens
			jsonMarshal, _ := json.Marshal(tmp)
			hash := calculateHash(jsonMarshal)
			links.Links = append(links.Links, Link{
				Hash: hash,
				Size: lens,
			})
			typeName := "link"
			if tmp.Links == nil {
				typeName = "blob"
			}
			links.Data = append(links.Data, []byte(typeName)...)
		}
		jsonMarshal, _ := json.Marshal(links)
		hash := calculateHash(jsonMarshal)
		store.Put(hash, jsonMarshal)
		return links, lenData
	}
}

func StoreDir(store KVStore, dir Dir, h hash.Hash) *Object {
	it := dir.It() //遍历目录节点下的所有子节点
	treeObject := &Object{}
	for it.Next() {
		n := it.Node() //当前目录下的node
		switch n.Type() {
		case FILE:
			file := n.(File)
			tmp := StoreFile(store, file, h)
			jsonMarshal, _ := json.Marshal(tmp)
			hash := calculateHash(jsonMarshal)
			treeObject.Links = append(treeObject.Links, Link{
				Hash: hash,
				Size: int(file.Size()),
			})
			typeName := "link"
			if tmp.Links == nil {
				typeName = "blob"
			}
			treeObject.Data = append(treeObject.Data, []byte(typeName)...)
		case DIR:
			dir := n.(Dir)
			tmp := StoreDir(store, dir, h)
			jsonMarshal, _ := json.Marshal(tmp)
			hash := calculateHash(jsonMarshal)
			treeObject.Links = append(treeObject.Links, Link{
				Hash: hash,
				Size: int(dir.Size()),
			})
			typeName := "tree"
			treeObject.Data = append(treeObject.Data, []byte(typeName)...)
		}
	}
	jsonMarshal, _ := json.Marshal(treeObject)
	hash := calculateHash(jsonMarshal)
	store.Put(hash, jsonMarshal)
	return treeObject
}

